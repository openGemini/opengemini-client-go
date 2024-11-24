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

	"github.com/libgox/unicodex/letter"
	"github.com/stretchr/testify/require"
)

func testDefaultClient(t *testing.T) Client {
	return testNewClient(t, &Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
}

func testNewClient(t *testing.T, config *Config) Client {
	client, err := newClient(config)
	require.Nil(t, err)
	require.NotNil(t, client)
	return client
}

func randomDatabaseName() string {
	return letter.RandEnglish(8)
}

func randomRetentionPolicy() string {
	return letter.RandEnglish(8)
}

func randomMeasurement() string {
	return letter.RandEnglish(8)
}
