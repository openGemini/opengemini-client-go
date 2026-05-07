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
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
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
	// Chunked tells the server to send back chunked responses. This places
	// less load on the server by sending back chunks of the response rather
	// than waiting for the entire response all at once.
	Chunked bool
	// ChunkSize sets the maximum number of rows that will be returned per
	// chunk. Chunks are either divided based on their series or if they hit
	// the chunk size limit.
	//
	// Chunked must be set to true for this option to be used.
	ChunkSize int
	// The consumer to invoke for each received QueryResult
	ConsumerChunk func(*QueryResult, error) bool
}

// Query sends a command to the server
func (c *client) Query(q Query) (*QueryResult, error) {
	if err := checkCommand(q.Command); err != nil {
		if q.ConsumerChunk != nil && q.Chunked {
			q.ConsumerChunk(nil, err)
		}
		return nil, err
	}

	var err error
	req := buildRequestDetails(c.config, func(req *requestDetails) {
		req.queryValues.Add("db", q.Database)
		req.queryValues.Add("q", q.Command)
		req.queryValues.Add("rp", q.RetentionPolicy)
		// Set chunk query parameters
		if q.Chunked {
			req.queryValues.Add("chunked", "true")
			if q.ChunkSize > 0 {
				req.queryValues.Add("chunk_size", strconv.Itoa(q.ChunkSize))
			}
		}
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
		if q.ConsumerChunk != nil && q.Chunked {
			q.ConsumerChunk(nil, err)
		}
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
		queryErr := errors.New("query request failed, error: " + err.Error())
		if q.ConsumerChunk != nil && q.Chunked {
			q.ConsumerChunk(nil, queryErr)
		}
		return nil, queryErr
	}

	if q.Chunked {
		// Use chunk query
		return retrieveChunkedQueryResFromResp(resp, q.ConsumerChunk)
	} else {
		qr, err := retrieveQueryResFromResp(resp)
		if err != nil {
			return nil, err
		}
		return qr, nil
	}
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
		return nil, errors.New("error resp, code: " + resp.Status + " body: " + string(body))
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

func retrieveChunkedQueryResFromResp(resp *http.Response, consumerChunk func(*QueryResult, error) bool) (*QueryResult, error) {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			respErr := errors.New("read resp failed, error: " + err.Error())
			if consumerChunk != nil {
				consumerChunk(nil, respErr)
			}
			return nil, respErr
		}
		respErr := errors.New("error resp, code: " + resp.Status + " body: " + string(body))
		if consumerChunk != nil {
			consumerChunk(nil, respErr)
		}
		return nil, respErr
	}
	contentType := resp.Header.Get("Content-Type")
	contentEncoding := resp.Header.Get("Content-Encoding")

	decodedReader, err := decodeChunkedBody(resp.Body, contentEncoding)
	if err != nil {
		if consumerChunk != nil {
			consumerChunk(nil, err)
		}
		return nil, err
	}

	cr := NewChunkedQueryResponse(decodedReader, contentType)

	var qr QueryResult
	for {
		chunk, err := cr.Next()
		if consumerChunk == nil {
			// no consumer callback
			if err != nil {
				return nil, err
			}
			if chunk == nil {
				break
			}
			qr.Results = append(qr.Results, chunk.Results...)
			if chunk.hasError() != nil {
				qr.Error = chunk.Error
				break
			}
		} else {
			// use consumer callback
			if continueFlag := consumerChunk(chunk, err); !continueFlag {
				break
			}
			if chunk == nil && err == nil {
				// stream exhausted
				break
			}
		}
	}
	return &qr, nil
}

// duplexReader reads responses and writes it to another writer while
// satisfying the reader interface.
type duplexReader struct {
	r io.Reader
	w io.Writer
}

func (r *duplexReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if err == nil {
		_, err = r.w.Write(p[:n])
	}
	return n, err
}

// decodeChunkedBody handles decompression and returns a ChunkedQueryResponse for streaming decode.
func decodeChunkedBody(body io.Reader, encoding string) (io.Reader, error) {
	switch encoding {
	case HttpEncodingZstd:
		return zstd.NewReader(body)
	case HttpEncodingGzip:
		return gzip.NewReader(body)
	case HttpEncodingSnappy:
		return nil, errors.New("snappy encoding is not supported for chunked responses")
	default:
		return body, nil
	}
}

// ChunkedQueryResponse reads a stream and produces query results from the stream.
// It supports both JSON and msgpack content types for chunked query responses.
type ChunkedQueryResponse struct {
	jsonDec    *json.Decoder
	msgpackDec *msgpack.Decoder
	duplex     *duplexReader
	buf        bytes.Buffer
}

// NewChunkedQueryResponse creates a new ChunkedQueryResponse from a reader.
func NewChunkedQueryResponse(r io.Reader, contentType string) *ChunkedQueryResponse {
	resp := &ChunkedQueryResponse{}
	resp.duplex = &duplexReader{r: r, w: &resp.buf}

	switch contentType {
	case HttpContentTypeJSON:
		resp.jsonDec = json.NewDecoder(resp.duplex)
	case HttpContentTypeMsgpack:
		resp.msgpackDec = msgpack.NewDecoder(resp.duplex)
	}

	return resp
}

// Next reads the next chunked result from the stream.
// Returns nil, nil when the stream is exhausted (EOF).
// Returns nil, error on decoding or I/O errors.
func (r *ChunkedQueryResponse) Next() (*QueryResult, error) {
	var qr QueryResult
	var err error

	if r.jsonDec != nil {
		err = r.jsonDec.Decode(&qr)
	} else if r.msgpackDec != nil {
		err = r.msgpackDec.Decode(&qr)
	} else {
		return nil, errors.New("no decoder available for content type")
	}

	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		_, err = io.Copy(io.Discard, r.duplex)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(strings.TrimSpace(r.buf.String()))
	}

	r.buf.Reset()
	return &qr, nil
}
