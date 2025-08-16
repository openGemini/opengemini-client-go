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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOtelClient_WriteInterceptors(t *testing.T) {
	c := testDefaultClient(t)

	//Register the OtelCClient interceptor
	c.Interceptors(NewOtelInterceptor())

	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	assert.NoError(t, err)
	time.Sleep(time.Second * 3)
	point := &Point{
		Measurement: "test_write",
		Precision:   PrecisionNanosecond,
		Timestamp:   time.Now().UnixNano(),
		Tags: map[string]string{
			"foo": "bar",
		},
		Fields: map[string]interface{}{
			"v1": 1,
		},
	}
	err = c.WritePoint(databaseName, point, CallbackDummy)
	require.Nil(t, err)
}

func TestOtelClient_ShowTagKeys(t *testing.T) {
	c := testDefaultClient(t)
	//Register the OtelCClient interceptor
	c.Interceptors(NewOtelInterceptor())

	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	assert.NoError(t, err)
	point := &Point{
		Measurement: "test_write",
		Precision:   PrecisionNanosecond,
		Timestamp:   time.Now().UnixNano(),
		Tags: map[string]string{
			"foo": "bar",
		},
		Fields: map[string]interface{}{
			"v1": 1,
		},
	}
	err = c.WritePoint(databaseName, point, CallbackDummy)
	require.Nil(t, err)
	measurement := randomMeasurement()
	cmd := fmt.Sprintf("CREATE MEASUREMENT %s (tag1 TAG,tag2 TAG,tag3 TAG, field1 INT64 FIELD, field2 BOOL, field3 STRING, field4 FLOAT64)", measurement)
	_, err = c.Query(Query{Command: cmd, Database: databaseName})
	assert.Nil(t, err)
	// SHOW TAG KEYS FROM measurement limit 3 OFFSET 0
	tagKeyResult, err := c.ShowTagKeys(NewShowTagKeysBuilder().Database(databaseName).Measurement(measurement).Limit(3).Offset(0))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(tagKeyResult))
	assert.Equal(t, 3, len(tagKeyResult[measurement]))
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestOtelShowDatabase(t *testing.T) {
	c := testDefaultClient(t)
	//Register the OtelCClient interceptor
	c.Interceptors(NewOtelInterceptor())

	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	if err != nil {
		t.Logf("Error creating database %q: %v", databaseName, err)
		return
	}
	require.Nil(t, err)

	result, err := c.ShowDatabases()
	require.Nil(t, err)
	assert.NotEmpty(t, result)
}

func TestOtelWritePoint(t *testing.T) {
	c := testDefaultClient(t)
	//Register the OtelCClient interceptor
	c.Interceptors(NewOtelInterceptor())

	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	assert.Nil(t, err, "failure:%v", err)

	point := &Point{
		Measurement: "test_write",
		Precision:   PrecisionNanosecond,
		Timestamp:   time.Now().UnixNano(),
		Tags: map[string]string{
			"foo": "bar",
		},
		Fields: map[string]interface{}{
			"v1": 1,
		},
	}

	err = c.WritePoint(databaseName, point, CallbackDummy)
	assert.Nil(t, err)
}

func TestOtelCreateAndQueryMeasurement(t *testing.T) {
	c := testDefaultClient(t)
	//Register the OtelCClient interceptor
	c.Interceptors(NewOtelInterceptor())
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)

	measurement := randomMeasurement()

	createCmd := fmt.Sprintf("CREATE MEASUREMENT %s (tag1 TAG, tag2 TAG, field1 INT64 FIELD)", measurement)
	createQuery := Query{Command: createCmd, Database: databaseName}
	_, err = c.Query(createQuery)
	require.Nil(t, err)

	queryCmd := fmt.Sprintf("SELECT * FROM %s", measurement)
	queryQuery := Query{Command: queryCmd, Database: databaseName}
	result, err := c.Query(queryQuery)
	require.Nil(t, err)
	assert.NotEmpty(t, result)
}
