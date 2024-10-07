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
	"context"
	"errors"
	"io"
	"net/http"
)

// Ping check that status of cluster.
func (c *client) Ping(idx int) error {
	return c.ping(context.TODO(), idx)
}

func (c *client) ping(ctx context.Context, idx int) error {
	resp, err := c.executeHttpRequestByIdxWithContext(ctx, idx, http.MethodGet, UrlPing, requestDetails{})
	if err != nil {
		return errors.New("ping request failed, error: " + err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.New("read ping resp failed, error: " + err.Error())
		}
		return errors.New("ping error resp, code: " + resp.Status + "body: " + string(body))
	}
	return nil
}
