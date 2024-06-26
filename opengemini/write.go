package opengemini

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type WriteCallback func(error)

func (c *client) WriteBatchPoints(database string, bp []*Point) error {
	var buffer bytes.Buffer
	writer := c.newWriter(&buffer)

	enc := NewLineProtocolEncoder(writer)
	if err := enc.BatchEncode(bp); err != nil {
		return errors.New("batchEncode failed, error: " + err.Error())
	}

	if closer, ok := writer.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return errors.New("writer close failed, error: " + err.Error())
		}
	}

	resp, err := c.innerWrite(database, &buffer)
	if err != nil {
		return errors.New("innerWrite request failed, error: " + err.Error())
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		errorBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.New("writeBatchPoint read resp body failed, error: " + err.Error())
		}
		return errors.New("writeBatchPoint error resp, code: " + resp.Status + "body: " + string(errorBody))
	}
	return nil
}

type sendBatchWithCB struct {
	point    *Point
	callback WriteCallback
}

func (c *client) WritePoint(ctx context.Context, database string, point *Point, callback WriteCallback) error {
	if c.config.BatchConfig != nil {
		value, ok := c.dataChan.Load(database)
		if !ok {
			newCollection := make(chan *sendBatchWithCB, c.config.BatchConfig.BatchSize*2)
			actual, loaded := c.dataChan.LoadOrStore(database, newCollection)
			if loaded {
				close(newCollection)
			} else {
				go c.internalBatchSend(ctx, database, actual.(chan *sendBatchWithCB))
			}
			value = actual
		}
		collection := value.(chan *sendBatchWithCB)
		collection <- &sendBatchWithCB{
			point:    point,
			callback: callback,
		}
		return nil
	}

	var buffer bytes.Buffer
	writer := c.newWriter(&buffer)

	enc := NewLineProtocolEncoder(writer)
	if err := enc.Encode(point); err != nil {
		return errors.New("encode failed, error: " + err.Error())
	}

	if closer, ok := writer.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return errors.New("writer close failed, error: " + err.Error())
		}
	}

	resp, err := c.innerWrite(database, &buffer)
	if err != nil {
		callback(errors.New("innerWrite request failed, error: " + err.Error()))
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		errorBody, err := io.ReadAll(resp.Body)
		if err != nil {
			callback(errors.New("writePoint read resp body failed, error: " + err.Error()))
		} else {
			callback(fmt.Errorf("writePoint error resp, code: " + resp.Status + "body: " + string(errorBody)))
		}
	} else {
		callback(nil)
	}

	return nil
}

func (c *client) internalBatchSend(ctx context.Context, database string, resource <-chan *sendBatchWithCB) {
	var tickInterval = c.config.BatchConfig.BatchInterval
	var ticker = time.NewTicker(tickInterval)
	var points = make([]*Point, 0, c.config.BatchConfig.BatchSize)
	var cbs []WriteCallback
	needFlush := false
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			needFlush = true
		case record := <-resource:
			points = append(points, record.point)
			cbs = append(cbs, record.callback)
		}
		if len(points) >= c.config.BatchConfig.BatchSize || needFlush {
			err := c.WriteBatchPoints(database, points)
			for _, callback := range cbs {
				callback(err)
			}
			needFlush = false
			ticker.Reset(tickInterval)
			points = []*Point{}
			cbs = []WriteCallback{}
		}
	}
}

func (c *client) newWriter(buffer *bytes.Buffer) io.Writer {
	if c.config.GzipEnabled {
		return gzip.NewWriter(buffer)
	} else {
		return buffer
	}
}

func (c *client) innerWrite(database string, buffer *bytes.Buffer) (*http.Response, error) {
	req := requestDetails{
		queryValues: make(url.Values),
		body:        buffer,
	}
	if c.config.GzipEnabled {
		req.header = make(http.Header)
		req.header.Set("Content-Encoding", "gzip")
		req.header.Set("Accept-Encoding", "gzip")
	}
	req.queryValues.Add("db", database)

	c.metrics.writeCounter.Add(1)
	c.metrics.writeDatabaseCounter.WithLabelValues(database).Add(1)
	startAt := time.Now()

	response, err := c.executeHttpPost(UrlWrite, req)

	cost := float64(time.Since(startAt).Milliseconds())
	c.metrics.writeLatency.Observe(cost)
	c.metrics.writeDatabaseLatency.WithLabelValues(database).Observe(cost)

	return response, err
}
