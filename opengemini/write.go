package opengemini

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type WriteCallback func(error)

func (c *client) WriteBatchPoints(database string, bp *BatchPoints) error {
	var buffer bytes.Buffer
	writer := c.newBuffer(&buffer)

	for _, p := range bp.Points {
		if p == nil {
			continue
		}
		if _, err := io.WriteString(writer, p.String()); err != nil {
			return err
		}
		if _, err := writer.Write([]byte{'\n'}); err != nil {
			return err
		}
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
		reason, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.New("write failed and couldn't get the error for " + err.Error())
		}
		return errors.New("write failed for " + string(reason))
	}
	return nil
}

type sendBatchWithCB struct {
	point    *Point
	callback WriteCallback
}

func (c *client) WritePoint(database string, point *Point, callback WriteCallback) error {
	if c.config.BatchConfig != nil {
		collection, ok := c.dataChan[database]
		if !ok {
			collection = make(chan *sendBatchWithCB, c.config.BatchConfig.BatchSize*2)
			c.dataChan[database] = collection
			go c.internalBatchSend(database, collection)
		}
		collection <- &sendBatchWithCB{
			point:    point,
			callback: callback,
		}
		return nil
	}

	var buffer bytes.Buffer
	writer := c.newBuffer(&buffer)

	if _, err := io.WriteString(writer, point.String()); err != nil {
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
		var p []byte
		_, err = resp.Body.Read(p)
		if err != nil {
			callback(errors.New("write failed ,status code:" + resp.Status + ",get resp body error for " + err.Error()))
		} else {
			callback(errors.New(resp.Status + " :" + string(p)))
		}
	} else {
		callback(nil)
	}

	return nil
}

func (c *client) internalBatchSend(database string, resource <-chan *sendBatchWithCB) {
	var tickInterval = c.config.BatchConfig.BatchInterval
	var ticker = time.NewTicker(tickInterval)
	var points = new(BatchPoints)
	var cbs []WriteCallback
	var needFlush atomic.Bool
	for {
		select {
		case <-c.ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			needFlush.Store(true)
		case record := <-resource:
			points.AddPoint(record.point)
			cbs = append(cbs, record.callback)
		}
		if len(points.Points) >= c.config.BatchConfig.BatchSize || needFlush.Load() {
			err := c.WriteBatchPoints(database, points)
			for _, callback := range cbs {
				callback(err)
			}
			needFlush.Store(false)
			ticker.Reset(tickInterval)
			points.Points = []*Point{}
			cbs = []WriteCallback{}
		}
	}
}

func (c *client) newBuffer(buffer *bytes.Buffer) io.Writer {
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
