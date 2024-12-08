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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	HttpContentTypeMsgpack = "application/x-msgpack"
	HttpContentTypeJSON    = "application/json"
	HttpEncodingGzip       = "gzip"
	HttpEncodingZstd       = "zstd"
)

type Query struct {
	Database        string
	Command         string
	RetentionPolicy string
	Precision       Precision
}

// Query sends a command to the server
func (c *client) Query(q Query) (*QueryResult, error) {
	if err := checkCommand(q.Command); err != nil {
		return nil, err
	}

	req := buildRequestDetails(c.config, func(req *requestDetails) {
		req.queryValues.Add("db", q.Database)
		req.queryValues.Add("q", q.Command)
		req.queryValues.Add("rp", q.RetentionPolicy)
		req.queryValues.Add("epoch", q.Precision.Epoch())
	})

	// metric
	c.metrics.queryCounter.Add(1)
	c.metrics.queryDatabaseCounter.WithLabelValues(q.Database).Add(1)
	startAt := time.Now()

	resp, err := c.executeHttpGet(UrlQuery, req)

	cost := float64(time.Since(startAt).Milliseconds())
	c.metrics.queryLatency.Observe(cost)
	c.metrics.queryDatabaseLatency.WithLabelValues(q.Database).Observe(cost)

	if err != nil {
		return nil, errors.New("query request failed, error: " + err.Error())
	}
	qr, err := retrieveQueryResFromResp(resp)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func (c *client) queryPost(q Query) (*QueryResult, error) {
	req := buildRequestDetails(c.config, func(req *requestDetails) {
		req.queryValues.Add("db", q.Database)
		req.queryValues.Add("q", q.Command)
	})

	resp, err := c.executeHttpPost(UrlQuery, req)
	if err != nil {
		return nil, errors.New("request failed, error: " + err.Error())
	}
	qr, err := retrieveQueryResFromResp(resp)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func buildRequestDetails(c *Config, requestModifier func(*requestDetails)) requestDetails {
	req := requestDetails{
		queryValues: make(map[string][]string),
	}

	applyCodec(&req, c)

	if requestModifier != nil {
		requestModifier(&req)
	}

	return req
}

func applyCodec(req *requestDetails, config *Config) {
	if req.header == nil {
		req.header = make(http.Header)
	}

	switch config.ContentType {
	case ContentTypeMsgPack:
		req.header.Set("Accept", HttpContentTypeMsgpack)
	case ContentTypeJSON:
		req.header.Set("Accept", HttpContentTypeJSON)
	}

	switch config.CompressMethod {
	case CompressMethodGzip:
		req.header.Set("Accept-Encoding", HttpEncodingGzip)
	case CompressMethodZstd:
		req.header.Set("Accept-Encoding", HttpEncodingZstd)
	}

}

// retrieve query result from the response
func retrieveQueryResFromResp(resp *http.Response) (*QueryResult, error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("read resp failed, error: " + err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("error resp, code: " + resp.Status + "body: " + string(body))
	}
	contentType := resp.Header.Get("Content-Type")
	contentEncoding := resp.Header.Get("Content-Encoding")
	var qr = new(QueryResult)
	var decompressedBody []byte

	// First, handle decompression
	switch contentEncoding {
	case HttpEncodingZstd:
		decompressedBody, err = decodeZstdBody(body)
		if err != nil {
			return qr, err
		}
	case HttpEncodingGzip:
		decompressedBody, err = decodeGzipBody(body)
		if err != nil {
			return qr, err
		}
	default:
		decompressedBody = body
	}

	// Then, handle deserialization based on content type
	switch contentType {
	case HttpContentTypeMsgpack:
		return qr, unmarshalMsgpack(decompressedBody, qr)
	case HttpContentTypeJSON:
		return qr, unmarshalJson(decompressedBody, qr)
	default:
		return qr, fmt.Errorf("unsupported content type: %s", contentType)
	}
}

func decodeGzipBody(body []byte) ([]byte, error) {
	decoder, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("failed to create gzip decoder: " + err.Error())
	}
	defer decoder.Close()

	decompressedBody, err := io.ReadAll(decoder)
	if err != nil {
		return nil, errors.New("failed to decompress gzip body: " + err.Error())
	}

	return decompressedBody, nil
}

func decodeZstdBody(compressedBody []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, errors.New("failed to create zstd decoder: " + err.Error())
	}
	defer decoder.Close()

	decompressedBody, err := decoder.DecodeAll(compressedBody, nil)
	if err != nil {
		return nil, errors.New("failed to decompress zstd body: " + err.Error())
	}

	return decompressedBody, nil
}

func unmarshalMsgpack(body []byte, qr *QueryResult) error {
	err := msgpack.Unmarshal(body, qr)
	if err != nil {
		return errors.New("unmarshal msgpack body failed, error: " + err.Error())
	}
	return nil
}

func unmarshalJson(body []byte, qr *QueryResult) error {
	err := json.Unmarshal(body, qr)
	if err != nil {
		return errors.New("unmarshal json body failed, error: " + err.Error())
	}
	return nil
}
