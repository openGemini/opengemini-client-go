package opengemini

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type WriteCallback func(error)

func (c *client) WriteBatchPoints(database string, bp *BatchPoints) error {
	var buffer bytes.Buffer
	writer := c.newWriter(&buffer)

	enc := NewLineProtocolEncoder(writer)
	if err := enc.BatchEncode(bp); err != nil {
		return err
	}

	if closer, ok := writer.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}

	resp, err := c.innerWrite(database, &buffer)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		errorBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body after write failure: %w", err)
		}
		return fmt.Errorf("write failed, error: %s", string(errorBody))
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
		return err
	}

	if closer, ok := writer.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}

	resp, err := c.innerWrite(database, &buffer)
	if err != nil {
		callback(err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		errorBody, err := io.ReadAll(resp.Body)
		if err != nil {
			callback(fmt.Errorf("failed to read response body after write failure: %w", err))
		} else {
			callback(fmt.Errorf("write failed, status: %s, error: %s", resp.Status, string(errorBody)))
		}
	} else {
		callback(nil)
	}

	return nil
}

func (c *client) internalBatchSend(ctx context.Context, database string, resource <-chan *sendBatchWithCB) {
	var tickInterval = c.config.BatchConfig.BatchInterval
	var ticker = time.NewTicker(tickInterval)
	var points = new(BatchPoints)
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
			points.AddPoint(record.point)
			cbs = append(cbs, record.callback)
		}
		if len(points.Points) >= c.config.BatchConfig.BatchSize || needFlush {
			err := c.WriteBatchPoints(database, points)
			for _, callback := range cbs {
				callback(err)
			}
			needFlush = false
			ticker.Reset(tickInterval)
			points.Points = []*Point{}
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
	return c.executeHttpPost(UrlWrite, req)
}
