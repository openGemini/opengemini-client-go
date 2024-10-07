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
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestSetAuthorization(t *testing.T) {
	c := client{
		config: &Config{
			AuthConfig: &AuthConfig{
				AuthType: AuthTypePassword,
				Username: "test",
				Password: "test pwd",
			},
		},
	}

	header := c.updateAuthHeader(http.MethodGet, UrlPing, nil)
	require.Equal(t, "", header.Get("Authorization"))

	header = c.updateAuthHeader(http.MethodOptions, UrlQuery, nil)
	require.Equal(t, "", header.Get("Authorization"))

	header = c.updateAuthHeader(http.MethodGet, UrlQuery, nil)
	require.Equal(t, "Basic dGVzdDp0ZXN0IHB3ZA==", header.Get("Authorization"))
}
