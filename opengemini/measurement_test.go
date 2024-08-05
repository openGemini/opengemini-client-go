package opengemini

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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
	// SHOW TAG KEYS FROM MEASUREMENT limit 3 OFFSET 0
	showKeyCmd := NewTagKeysBuilder().Limit(3).Measurement(measurement)
	tagKeyResult, err := c.ShowTagKeys(databaseName, showKeyCmd)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(tagKeyResult))
	assert.Equal(t, 3, len(tagKeyResult[measurement]))
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientShowTagValues(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()
	var ctx = context.Background()
	tag := RandStr(4)
	err = c.WritePoint(ctx, databaseName, &Point{
		Measurement: measurement,
		Precision:   PrecisionMillisecond,
		Tags: map[string]string{
			tag: "t1",
		},
		Fields: map[string]interface{}{
			"field1": "v1",
		},
	}, func(err error) {

	})
	require.Nil(t, err)
	time.Sleep(time.Second)
	err = c.WritePoint(ctx, databaseName, &Point{
		Measurement: measurement,
		Precision:   PrecisionMillisecond,
		Tags: map[string]string{
			tag: "t2",
		},
		Fields: map[string]interface{}{
			"field2": "v2",
		},
	}, func(err error) {

	})
	require.Nil(t, err)
	time.Sleep(time.Second * 3)
	// SHOW KEY VALUES FROM {MEASUREMENT} WITH KEY = "{tag}"
	cmd := NewTagValuesBuilder().Measurement(measurement).Key(tag)
	values, err := c.ShowTagValues(databaseName, cmd)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(values))
	assert.EqualValues(t, "t1", values[0])
	assert.EqualValues(t, "t2", values[1])
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
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
	builder := NewFieldKeysBuilder().Measurement(measurement)
	tagFieldResult, err := c.ShowFieldKeys(databaseName, builder)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(tagFieldResult))
	assert.Equal(t, 4, len(tagFieldResult[measurement]))
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}
