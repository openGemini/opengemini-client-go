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

// CallbackDummy if user don't want to handle WritePoint error, could use this function as empty callback
func CallbackDummy(_ error) {
	// Do nothing
}

func (c *client) WriteBatchPoints(ctx context.Context, database string, bp []*Point) error {
	return c.WriteBatchPointsWithRp(ctx, database, "", bp)
}

func (c *client) WritePoint(database string, point *Point, callback WriteCallback) error {
	return c.WritePointWithRp(database, "", point, callback)
}

type sendBatchWithCB struct {
	point    *Point
	callback WriteCallback
}

type dbRp struct {
	db string
	rp string
}

func (c *client) WritePointWithRp(database string, rp string, point *Point, callback WriteCallback) error {
	if c.config.BatchConfig != nil {
		select {
		case <-c.batchContext.Done():
			return c.batchContext.Err()
		default:
			value, ok := c.dataChanMap.Load(dbRp{db: database, rp: rp})
			if !ok {
				newCollection := make(chan *sendBatchWithCB, c.config.BatchConfig.BatchSize*2)
				actual, loaded := c.dataChanMap.LoadOrStore(database, newCollection)
				if loaded {
					close(newCollection)
				} else {
					go c.internalBatchSend(c.batchContext, database, rp, actual.(chan *sendBatchWithCB))
				}
				value = actual
			}
			collection := value.(chan *sendBatchWithCB)
			collection <- &sendBatchWithCB{
				point:    point,
				callback: callback,
			}
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

	resp, err := c.innerWrite(context.TODO(), database, rp, &buffer)
	if err != nil {
		callback(errors.New("innerWrite request failed, error: " + err.Error()))
		return nil
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

func (c *client) WriteBatchPointsWithRp(ctx context.Context, database string, rp string, bp []*Point) error {
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

	resp, err := c.innerWrite(ctx, database, rp, &buffer)
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

func (c *client) internalBatchSend(ctx context.Context, database string, rp string, resource <-chan *sendBatchWithCB) {
	var tickInterval = c.config.BatchConfig.BatchInterval
	var ticker = time.NewTicker(tickInterval)
	var points = make([]*Point, 0, c.config.BatchConfig.BatchSize)
	var cbs []WriteCallback
	needFlush := false
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			for record := range resource {
				record.callback(fmt.Errorf("send batch context cancelled"))
			}
			return
		case <-ticker.C:
			needFlush = true
		case record := <-resource:
			points = append(points, record.point)
			cbs = append(cbs, record.callback)
		}
		if len(points) >= c.config.BatchConfig.BatchSize || needFlush {
			err := c.WriteBatchPointsWithRp(ctx, database, rp, points)
			for _, callback := range cbs {
				callback(err)
			}
			needFlush = false
			ticker.Reset(tickInterval)
			points = points[:0]
			cbs = cbs[:0]
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

func (c *client) innerWrite(ctx context.Context, database string, rp string, buffer *bytes.Buffer) (*http.Response, error) {
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
	req.queryValues.Add("rp", rp)

	c.metrics.writeCounter.Add(1)
	c.metrics.writeDatabaseCounter.WithLabelValues(database).Add(1)
	startAt := time.Now()

	response, err := c.executeHttpRequestWithContext(ctx, http.MethodPost, UrlWrite, req)

	cost := float64(time.Since(startAt).Milliseconds())
	c.metrics.writeLatency.Observe(cost)
	c.metrics.writeDatabaseLatency.WithLabelValues(database).Observe(cost)

	return response, err
}
