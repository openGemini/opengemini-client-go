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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/vmihailenco/msgpack/v5"

	compressionPool "github.com/openGemini/opengemini-client-go/lib/pool"
)

const (
	HttpContentTypeMsgpack = "application/x-msgpack"
	HttpContentTypeJSON    = "application/json"
	HttpEncodingGzip       = "gzip"
	HttpEncodingZstd       = "zstd"
	HttpEncodingSnappy     = "snappy"
)

type Query struct {
	Database        string
	Command         string
	RetentionPolicy string
	Precision       Precision
	// Params is a server-side supported behavior that allows clients to query SQL using variable methods instead of
	// values in the where condition, a simple example is a measurement structure with
	// `weather,location=us-midwest temperature=82`, the client can use `select * from mst where v1=$var` to query data,
	// and specify params as `var:82`. For more cases, please refer to `ExampleQuery`
	Params map[string]any
}

// Query sends a command to the server
func (c *client) Query(q Query) (*QueryResult, error) {
	if err := checkCommand(q.Command); err != nil {
		return nil, err
	}

	var err error
	req := buildRequestDetails(c.config, func(req *requestDetails) {
		req.queryValues.Add("db", q.Database)
		req.queryValues.Add("q", q.Command)
		req.queryValues.Add("rp", q.RetentionPolicy)
		req.queryValues.Add("epoch", q.Precision.Epoch())
		if len(q.Params) != 0 {
			var params []byte
			params, err = json.Marshal(q.Params)
			if err != nil {
				err = fmt.Errorf("marshal query bound parameter failed: %w", err)
				return
			}
			req.queryValues.Add("params", string(params))
		}
	})

	if err != nil {
		return nil, err
	}

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
	case CompressMethodSnappy:
		req.header.Set("Accept-Encoding", HttpEncodingSnappy)
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

	// handle decompression first
	decompressedBody, err := decompressBody(contentEncoding, body)
	if err != nil {
		return qr, err
	}

	// then handle deserialization based on content type
	err = deserializeBody(contentType, decompressedBody, qr)
	if err != nil {
		return qr, err
	}

	return qr, nil
}

func decompressBody(encoding string, body []byte) ([]byte, error) {
	switch encoding {
	case HttpEncodingZstd:
		return decodeZstdBody(body)
	case HttpEncodingGzip:
		return decodeGzipBody(body)
	case HttpEncodingSnappy:
		return decodeSnappyBody(body)
	default:
		return body, nil
	}
}

func decodeGzipBody(body []byte) ([]byte, error) {
	decoder, err := compressionPool.GetGzipReader(body)
	if err != nil {
		return nil, errors.New("failed to create gzip decoder: " + err.Error())
	}
	defer compressionPool.PutGzipReader(decoder)

	decompressedBody, err := io.ReadAll(decoder)
	if err != nil {
		return nil, errors.New("failed to decompress gzip body: " + err.Error())
	}

	return decompressedBody, nil
}

func decodeZstdBody(compressedBody []byte) ([]byte, error) {
	decoder, err := compressionPool.GetZstdDecoder(compressedBody)
	if err != nil {
		return nil, errors.New("failed to create zstd decoder: " + err.Error())
	}
	defer compressionPool.PutZstdDecoder(decoder)

	decompressedBody, err := decoder.DecodeAll(compressedBody, nil)
	if err != nil {
		return nil, errors.New("failed to decompress zstd body: " + err.Error())
	}

	return decompressedBody, nil
}

func decodeSnappyBody(compressedBody []byte) ([]byte, error) {
	decompressedBody, err := snappy.Decode(nil, compressedBody)
	if err != nil {
		return nil, errors.New("failed to decompress snappy body: " + err.Error())
	}
	return decompressedBody, nil
}

func deserializeBody(contentType string, body []byte, qr *QueryResult) error {
	switch contentType {
	case HttpContentTypeMsgpack:
		return unmarshalMsgpack(body, qr)
	case HttpContentTypeJSON:
		return unmarshalJson(body, qr)
	default:
		return fmt.Errorf("unsupported content type: %s", contentType)
	}
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
