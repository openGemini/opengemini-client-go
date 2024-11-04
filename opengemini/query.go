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
	"io"
	"net/http"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type Query struct {
	Database        string
	Command         string
	RetentionPolicy string
	Precision       Precision
}

// Query sends a command to the server
func (c *client) Query(q Query) (*QueryResult, error) {
	req := requestDetails{
		queryValues: make(map[string][]string),
	}
	req.queryValues.Add("db", q.Database)
	req.queryValues.Add("q", q.Command)
	req.queryValues.Add("rp", q.RetentionPolicy)
	req.queryValues.Add("epoch", q.Precision.Epoch())

	//Encoding
	if c.config.Encoding == MSGPACK {
		if req.header == nil {
			req.header = make(http.Header)
		}
		req.header.Set("Accept", "application/x-msgpack")
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
	req := requestDetails{
		queryValues: make(map[string][]string),
	}

	req.queryValues.Add("db", q.Database)
	req.queryValues.Add("q", q.Command)

	if c.config.Encoding == MSGPACK {
		if req.header == nil {
			req.header = make(http.Header)
		}
		req.header.Set("Accept", "application/x-msgpack")
	}

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
	var qr = new(QueryResult)
	if contentType == "application/x-msgpack" {
		err = msgpack.Unmarshal(body, qr)
		if err != nil {
			return nil, errors.New("unmarshal msgpack body failed, error: " + err.Error())
		}
	} else {
		err = json.Unmarshal(body, qr)
		if err != nil {
			return nil, errors.New("unmarshal json body failed, error: " + err.Error())
		}
	}
	return qr, nil
}
