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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientShowTagKeys(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
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

func TestClientShowTagKeys_WithOffset(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()
	cmd := fmt.Sprintf("CREATE MEASUREMENT %s (tag1 TAG,tag2 TAG,tag3 TAG, field1 INT64 FIELD, field2 BOOL, field3 STRING, field4 FLOAT64)", measurement)
	_, err = c.Query(Query{Command: cmd, Database: databaseName})
	assert.Nil(t, err)
	// SHOW TAG KEYS FROM measurement limit 3 OFFSET 0
	tagKeyResult, err := c.ShowTagKeys(NewShowTagKeysBuilder().Database(databaseName).Measurement(measurement).Limit(0).Offset(1))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(tagKeyResult))
	// skip tag1, so value is []string{"tag2", "tag3"}
	assert.Equal(t, 2, len(tagKeyResult[measurement]))
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientShowTagKeys_WithLimit(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()
	cmd := fmt.Sprintf("CREATE MEASUREMENT %s (tag1 TAG,tag2 TAG,tag3 TAG, field1 INT64 FIELD, field2 BOOL, field3 STRING, field4 FLOAT64)", measurement)
	_, err = c.Query(Query{Command: cmd, Database: databaseName})
	assert.Nil(t, err)
	// SHOW TAG KEYS FROM measurement limit 3 OFFSET 0
	tagKeyResult, err := c.ShowTagKeys(NewShowTagKeysBuilder().Database(databaseName).Measurement(measurement).Limit(1).Offset(1))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(tagKeyResult))
	// offset 1 and skip tag1, so value is []string{"tag2"}
	assert.Equal(t, 1, len(tagKeyResult[measurement]))
	assert.Equal(t, "tag2", tagKeyResult[measurement][0])
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientShowTagKeys_Error_NoDatabase(t *testing.T) {
	c := testDefaultClient(t)
	measurement := randomMeasurement()
	tagKeyResult, err := c.ShowTagKeys(NewShowTagKeysBuilder().Database("").Measurement(measurement).Limit(3).Offset(0))
	assert.Error(t, err)
	assert.Nil(t, tagKeyResult)
}

func TestClient_ShowFieldKeys(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()
	cmd := fmt.Sprintf("CREATE MEASUREMENT %s (tag1 TAG,tag2 TAG,tag3 TAG, field1 INT64 FIELD, field2 BOOL, field3 STRING, field4 FLOAT64)", measurement)
	_, err = c.Query(Query{Command: cmd, Database: databaseName})
	assert.Nil(t, err)
	tagFieldResult, err := c.ShowFieldKeys(databaseName, measurement)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(tagFieldResult))
	// TODO return value is different with create command, confirm the reason with the community
	assert.EqualValues(t, "integer", tagFieldResult[measurement]["field1"])
	assert.EqualValues(t, "boolean", tagFieldResult[measurement]["field2"])
	assert.EqualValues(t, "string", tagFieldResult[measurement]["field3"])
	assert.EqualValues(t, "float", tagFieldResult[measurement]["field4"])
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClient_ShowTagValues(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()
	defer func() {
		err := c.DropDatabase(databaseName)
		assert.Nil(t, err)
	}()
	callback := func(err error) {
		assert.Nil(t, err)
	}

	points := []*Point{
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "c1",
				"country":  "cn",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 25.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "c2",
				"country":  "cn",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 26.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "u1",
				"country":  "us",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 35.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "u2",
				"country":  "us",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 36.0,
			},
		},
	}

	for _, point := range points {
		err := c.WritePoint(databaseName, point, callback)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second * 5)

	// SHOW TAG VALUES FROM measurement WITH KEY = location
	tagValueResult, err := c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("location"))
	assert.Nil(t, err)
	assert.Equal(t, 4, len(tagValueResult))
	expValues := []string{"c1", "c2", "u1", "u2"}
	sort.Strings(expValues)
	sort.Strings(tagValueResult)
	assert.EqualValues(t, expValues, tagValueResult)

	// SHOW TAG VALUES FROM measurement WITH KEY = location LIMIT 2 OFFSET 0
	tagValueResult, err = c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("location").Limit(2).Offset(0))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(tagValueResult))
	expValues = []string{"c1", "c2"}
	sort.Strings(expValues)
	sort.Strings(tagValueResult)
	assert.EqualValues(t, expValues, tagValueResult)

	// SHOW TAG VALUES FROM measurement WITH KEY = location LIMIT 2 OFFSET 2
	tagValueResult, err = c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("location").Limit(2).Offset(2))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(tagValueResult))
	expValues = []string{"u1", "u2"}
	sort.Strings(expValues)
	sort.Strings(tagValueResult)
	assert.EqualValues(t, expValues, tagValueResult)

	// SHOW TAG VALUES FROM measurement WITH KEY = location LIMIT 2 OFFSET 2 WHERE country = cn
	tagValueResult, err = c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("location").Limit(2).Offset(2).Where("country", Equals, "cn"))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(tagValueResult))
}

func TestClient_ShowTagValues_WithRegexp(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()
	defer func() {
		err := c.DropDatabase(databaseName)
		assert.Nil(t, err)
	}()
	callback := func(err error) {
		assert.Nil(t, err)
	}

	points := []*Point{
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "c1",
				"country":  "cn",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 25.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "c2",
				"country":  "cn",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 26.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "u1",
				"country":  "us",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 35.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "u2",
				"country":  "us",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 36.0,
			},
		},
	}

	for _, point := range points {
		err := c.WritePoint(databaseName, point, callback)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second * 5)

	// SHOW TAG VALUES FROM measurement WITH KEY = /loc.*/
	tagValueResult, err := c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("/loc.*/"))
	assert.Nil(t, err)
	assert.Equal(t, 4, len(tagValueResult))
	expValues := []string{"c1", "c2", "u1", "u2"}
	sort.Strings(expValues)
	sort.Strings(tagValueResult)
	assert.EqualValues(t, expValues, tagValueResult)

	// SHOW TAG VALUES FROM measurement WITH KEY = /loc./ LIMIT 2 OFFSET 0
	tagValueResult, err = c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("/loc.*/").Limit(2).Offset(0))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(tagValueResult))
	expValues = []string{"c1", "c2"}
	sort.Strings(expValues)
	sort.Strings(tagValueResult)
	assert.EqualValues(t, expValues, tagValueResult)

	// SHOW TAG VALUES FROM measurement WITH KEY = /loc./ LIMIT 2 OFFSET 2
	tagValueResult, err = c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("/loc.*/").Limit(2).Offset(2))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(tagValueResult))
	expValues = []string{"u1", "u2"}
	sort.Strings(expValues)
	sort.Strings(tagValueResult)
	assert.EqualValues(t, expValues, tagValueResult)

	// SHOW TAG VALUES FROM measurement WITH KEY = /loc./ LIMIT 2 OFFSET 2 WHERE country = cn
	tagValueResult, err = c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("/loc.*/").Limit(2).Offset(2).Where("country", Equals, "cn"))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(tagValueResult))
}

func TestClient_ShowTagValues_WithIn(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()
	defer func() {
		err := c.DropDatabase(databaseName)
		assert.Nil(t, err)
	}()
	callback := func(err error) {
		assert.Nil(t, err)
	}

	points := []*Point{
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "c1",
				"country":  "cn",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 25.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "c2",
				"country":  "cn",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 26.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "u1",
				"country":  "us",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 35.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"location": "u2",
				"country":  "us",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 36.0,
			},
		},
	}

	for _, point := range points {
		err := c.WritePoint(databaseName, point, callback)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second * 5)

	// SHOW TAG VALUES FROM measurement WITH KEY IN (location, country)
	tagValueResult, err := c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("location", "country"))
	assert.Nil(t, err)
	assert.Equal(t, 6, len(tagValueResult))
	expValues := []string{"c1", "c2", "u1", "u2", "cn", "us"}
	sort.Strings(expValues)
	sort.Strings(tagValueResult)
	assert.EqualValues(t, expValues, tagValueResult)

	// SHOW TAG VALUES FROM measurement WITH KEY IN (location, country) LIMIT 2 OFFSET 0
	tagValueResult, err = c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("location", "country").Limit(2).Offset(0))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(tagValueResult))

	// SHOW TAG VALUES FROM measurement WITH KEY IN (location, country) LIMIT 2 OFFSET 2
	tagValueResult, err = c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("location", "country").Limit(2).Offset(2))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(tagValueResult))

	// SHOW TAG VALUES FROM measurement WITH KEY IN (location, country) LIMIT 2 OFFSET 2 WHERE country = cn
	_, err = c.ShowTagValues(NewShowTagValuesBuilder().Database(databaseName).Measurement(measurement).
		With("location", "country").Limit(2).Offset(2).Where("country", Equals, "cn"))
	assert.Nil(t, err)
}

func TestClient_ShowTagValues_Error_NoWithKey(t *testing.T) {
	c := testDefaultClient(t)
	_, err := c.ShowTagValues(NewShowTagValuesBuilder().Database("not-exist-db").
		Measurement("not-exist-measurement"))
	assert.NotNil(t, err)
	assert.Equal(t, ErrEmptyTagKey, err)
}

func TestClient_ShowSeries(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()
	defer func() {
		err := c.DropDatabase(databaseName)
		assert.Nil(t, err)
	}()
	callback := func(err error) {
		assert.Nil(t, err)
	}

	points := []*Point{
		{
			Measurement: measurement,
			Tags: map[string]string{
				"vector1": "v1",
				"horizon": "h1",
			},
			Fields: map[string]interface{}{
				"temperature": 25.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"vector2": "c2",
				"country": "cn",
			},
			Fields: map[string]interface{}{
				"vector3":     "sun",
				"temperature": 26.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"vector3": "u1",
				"country": "us",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 35.0,
			},
		},
		{
			Measurement: measurement,
			Tags: map[string]string{
				"vector4": "u2",
				"country": "us",
			},
			Fields: map[string]interface{}{
				"weather":     "sun",
				"temperature": 36.0,
			},
		},
	}

	for _, point := range points {
		err := c.WritePoint(databaseName, point, callback)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second * 5)

	// SHOW SERIES FROM measurement
	seriesResult, err := c.ShowSeries(NewShowSeriesBuilder().Database(databaseName).Measurement(measurement))
	assert.Nil(t, err)
	assert.Equal(t, 4, len(seriesResult))

	// SHOW SERIES FROM measurement LIMIT 2 OFFSET 0
	seriesResult, err = c.ShowSeries(NewShowSeriesBuilder().Database(databaseName).Measurement(measurement).Limit(2).Offset(0))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(seriesResult))

	// SHOW SERIES FROM measurement LIMIT 2 OFFSET 2
	seriesResult, err = c.ShowSeries(NewShowSeriesBuilder().Database(databaseName).Measurement(measurement).Limit(2).Offset(2))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(seriesResult))

	// SHOW SERIES FROM measurement WHERE country = cn
	seriesResult, err = c.ShowSeries(NewShowSeriesBuilder().Database(databaseName).Measurement(measurement).Where("country", Equals, "cn"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(seriesResult))
}
