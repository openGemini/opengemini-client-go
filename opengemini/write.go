// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

type sendBatchWithCB struct {
	point    *Point
	callback WriteCallback
}

type dbRp struct {
	db string
	rp string
}

// CallbackDummy if user don't want to handle WritePoint error, could use this function as empty callback
//
//goland:noinspection GoUnusedExportedFunction
func CallbackDummy(_ error) {
	// Do nothing
}

func (c *client) WritePoint(database string, point *Point, callback WriteCallback) error {
	return c.WritePointWithRp(database, "", point, callback)
}

func (c *client) WriteBatchPoints(ctx context.Context, database string, bp []*Point) error {
	return c.WriteBatchPointsWithRp(ctx, database, "", bp)
}

func (c *client) WritePointWithRp(database string, rp string, point *Point, callback WriteCallback) error {
	if c.config.BatchConfig != nil {
		select {
		case <-c.batchContext.Done():
			return c.batchContext.Err()
		default:
			key := dbRp{db: database, rp: rp}
			value, ok := c.dataChanMap.Load(key)
			if !ok {
				newCollection := make(chan *sendBatchWithCB, c.config.BatchConfig.BatchSize*2)
				actual, loaded := c.dataChanMap.LoadOrStore(key, newCollection)
				if loaded {
					close(newCollection)
				} else {
					go c.internalBatchSend(c.batchContext, database, rp, actual)
				}
				value = actual
			}
			value <- &sendBatchWithCB{
				point:    point,
				callback: callback,
			}
		}
		return nil
	}

	buffer, err := c.encodePoint(c.batchContext, database, rp, point)
	if err != nil {
		return err
	}

	return c.writeBytesBuffer(c.batchContext, database, rp, buffer, len([]*Point{point}))
}

func (c *client) WriteBatchPointsWithRp(ctx context.Context, database string, rp string, bp []*Point) error {
	if len(bp) == 0 {
		return nil
	}

	buffer, err := c.encodeBatchPoints(ctx, database, rp, bp)
	if err != nil {
		return err
	}

	return c.writeBytesBuffer(ctx, database, rp, buffer, len(bp))
}

func (c *client) encodePoint(ctx context.Context, database, retentionPolicy string, point *Point) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	writer := c.newWriter(&buffer)

	enc := NewLineProtocolEncoder(writer)
	if err := enc.Encode(point); err != nil {
		return nil, errors.New("encode failed, error: " + err.Error())
	}

	return &buffer, nil
}

func (c *client) encodeBatchPoints(ctx context.Context, database, retentionPolicy string, bp []*Point) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	writer := c.newWriter(&buffer)
	enc := NewLineProtocolEncoder(writer)

	if err := enc.BatchEncode(bp); err != nil {
		return nil, errors.New("batchEncode failed: " + err.Error())
	}
	return &buffer, nil
}

func (c *client) writeBytesBuffer(ctx context.Context, database string, rp string, buffer *bytes.Buffer, pointCount int) error {
	parentCtx := ctx
	for _, interceptor := range c.interceptors {
		parentCtx = interceptor.WriteBefore(parentCtx, &OtelPoint{
			Database:        database,
			RetentionPolicy: rp,
			Measurement:     "batch_write",
			BatchCount:      pointCount,
		})
	}

	idx := c.prevIdx.Load()
	if idx >= int32(len(c.endpoints)) {
		return errors.New("no valid endpoint")
	}
	baseURL := c.endpoints[idx].url

	writeURL := fmt.Sprintf("%s/write?db=%s", baseURL, url.QueryEscape(database))
	if rp != "" {
		writeURL += fmt.Sprintf("&rp=%s", url.QueryEscape(rp))
	}

	req, err := http.NewRequestWithContext(parentCtx, "POST", writeURL, buffer)
	if err != nil {
		for _, interceptor := range c.interceptors {
			interceptor.WriteAfter(parentCtx, nil)
		}
		return fmt.Errorf("create request failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	response, err := c.cli.Do(req)

	for _, interceptor := range c.interceptors {
		interceptor.WriteAfter(parentCtx, response)
	}

	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode >= 300 {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("write failed, status: %d, body: %s", response.StatusCode, string(body))
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
	if c.config.CompressMethod == CompressMethodGzip {
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
	if c.config.CompressMethod == CompressMethodGzip {
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
