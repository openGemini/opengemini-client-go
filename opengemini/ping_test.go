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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPingSuccess(t *testing.T) {
	c := testDefaultClient(t)

	err := c.Ping(0)
	require.Nil(t, err)
}

func TestPingFailForInaccessibleAddress(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}, {
			Host: "localhost",
			Port: 8087,
		}},
	})

	err := c.Ping(1)
	require.NotNil(t, err)
}

func TestPingFailForOutOfRangeIndex(t *testing.T) {
	c := testDefaultClient(t)

	err := c.Ping(1)
	require.NotNil(t, err)
	err = c.Ping(-1)
	require.NotNil(t, err)
}
