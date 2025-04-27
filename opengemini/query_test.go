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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueryWithEpoch(t *testing.T) {
	c := testDefaultClient(t)

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	testMeasurement := randomMeasurement()
	p := &Point{}
	p.Measurement = testMeasurement
	p.AddField("TestField", 123)

	err = c.WritePoint(database, p, func(err error) {
		assert.Nil(t, err)
	})
	assert.Nil(t, err)

	time.Sleep(time.Second * 5)

	PrecisionTimestampLength := make(map[Precision]int64)
	PrecisionTimestampLength[PrecisionNanosecond] = 19
	PrecisionTimestampLength[PrecisionMicrosecond] = 16
	PrecisionTimestampLength[PrecisionMillisecond] = 13
	PrecisionTimestampLength[PrecisionSecond] = 10
	PrecisionTimestampLength[PrecisionMinute] = 8
	PrecisionTimestampLength[PrecisionHour] = 6

	// check whether write success
	for precision, length := range PrecisionTimestampLength {
		q := Query{
			Database:  database,
			Command:   "select * from " + testMeasurement,
			Precision: precision,
		}
		result, err := c.Query(q)
		assert.Nil(t, err)
		v := int64(result.Results[0].Series[0].Values[0][0].(float64))
		assert.Equal(t, length, getTimestampLength(v))
	}
}

func TestQueryWithMsgPack(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}},
		ContentType: ContentTypeMsgPack,
	})

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	testMeasurement := randomMeasurement()
	p := &Point{}
	p.Measurement = testMeasurement
	p.AddField("TestField", 123)

	err = c.WritePoint(database, p, func(err error) {
		assert.Nil(t, err)
	})
	assert.Nil(t, err)

	time.Sleep(time.Second * 5)

	PrecisionTimestampLength := make(map[Precision]int64)
	PrecisionTimestampLength[PrecisionNanosecond] = 19
	PrecisionTimestampLength[PrecisionMicrosecond] = 16
	PrecisionTimestampLength[PrecisionMillisecond] = 13
	PrecisionTimestampLength[PrecisionSecond] = 10
	PrecisionTimestampLength[PrecisionMinute] = 8
	PrecisionTimestampLength[PrecisionHour] = 6

	// check whether write success
	for precision, length := range PrecisionTimestampLength {
		q := Query{
			Database:  database,
			Command:   "select * from " + testMeasurement,
			Precision: precision,
		}
		result, err := c.Query(q)
		assert.Nil(t, err)
		v, err := convertToInt64(result.Results[0].Series[0].Values[0][0])
		if err != nil {
			t.Fatalf("conversion error: %v", err)
		}
		assert.Equal(t, length, getTimestampLength(v))
	}
}

func TestQueryWithZSTD(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}},
		CompressMethod: CompressMethodZstd,
	})

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	testMeasurement := randomMeasurement()
	p := &Point{}
	p.Measurement = testMeasurement
	p.AddField("TestField", 123)

	err = c.WritePoint(database, p, func(err error) {
		assert.Nil(t, err)
	})
	assert.Nil(t, err)

	time.Sleep(time.Second * 5)

	PrecisionTimestampLength := make(map[Precision]int64)
	PrecisionTimestampLength[PrecisionNanosecond] = 19
	PrecisionTimestampLength[PrecisionMicrosecond] = 16
	PrecisionTimestampLength[PrecisionMillisecond] = 13
	PrecisionTimestampLength[PrecisionSecond] = 10
	PrecisionTimestampLength[PrecisionMinute] = 8
	PrecisionTimestampLength[PrecisionHour] = 6

	// check whether write success
	for precision, length := range PrecisionTimestampLength {
		q := Query{
			Database:  database,
			Command:   "select * from " + testMeasurement,
			Precision: precision,
		}
		result, err := c.Query(q)
		assert.Nil(t, err)
		v, err := convertToInt64(result.Results[0].Series[0].Values[0][0])
		if err != nil {
			t.Fatalf("conversion error: %v", err)
		}
		assert.Equal(t, length, getTimestampLength(v))
	}
}

func TestQueryWithSnappy(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}},
		CompressMethod: CompressMethodSnappy,
	})

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	testMeasurement := randomMeasurement()
	p := &Point{}
	p.Measurement = testMeasurement
	p.AddField("TestField", 123)

	err = c.WritePoint(database, p, func(err error) {
		assert.Nil(t, err)
	})
	assert.Nil(t, err)

	time.Sleep(time.Second * 5)

	PrecisionTimestampLength := make(map[Precision]int64)
	PrecisionTimestampLength[PrecisionNanosecond] = 19
	PrecisionTimestampLength[PrecisionMicrosecond] = 16
	PrecisionTimestampLength[PrecisionMillisecond] = 13
	PrecisionTimestampLength[PrecisionSecond] = 10
	PrecisionTimestampLength[PrecisionMinute] = 8
	PrecisionTimestampLength[PrecisionHour] = 6

	// check whether write success
	for precision, length := range PrecisionTimestampLength {
		q := Query{
			Database:  database,
			Command:   "select * from " + testMeasurement,
			Precision: precision,
		}
		result, err := c.Query(q)
		assert.Nil(t, err)
		v, err := convertToInt64(result.Results[0].Series[0].Values[0][0])
		if err != nil {
			t.Fatalf("conversion error: %v", err)
		}
		assert.Equal(t, length, getTimestampLength(v))
	}
}

func TestQueryWithZSTDAndMsgPack(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}},
		CompressMethod: CompressMethodZstd,
		ContentType:    ContentTypeMsgPack,
	})

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	testMeasurement := randomMeasurement()
	p := &Point{}
	p.Measurement = testMeasurement
	p.AddField("TestField", 123)

	err = c.WritePoint(database, p, func(err error) {
		assert.Nil(t, err)
	})
	assert.Nil(t, err)

	time.Sleep(time.Second * 5)

	PrecisionTimestampLength := make(map[Precision]int64)
	PrecisionTimestampLength[PrecisionNanosecond] = 19
	PrecisionTimestampLength[PrecisionMicrosecond] = 16
	PrecisionTimestampLength[PrecisionMillisecond] = 13
	PrecisionTimestampLength[PrecisionSecond] = 10
	PrecisionTimestampLength[PrecisionMinute] = 8
	PrecisionTimestampLength[PrecisionHour] = 6

	// check whether write success
	for precision, length := range PrecisionTimestampLength {
		q := Query{
			Database:  database,
			Command:   "select * from " + testMeasurement,
			Precision: precision,
		}
		result, err := c.Query(q)
		assert.Nil(t, err)
		v, err := convertToInt64(result.Results[0].Series[0].Values[0][0])
		if err != nil {
			t.Fatalf("conversion error: %v", err)
		}
		assert.Equal(t, length, getTimestampLength(v))
	}
}

func getTimestampLength(timestamp int64) int64 {
	var length int64 = 0
	for ; timestamp != 0; length++ {
		timestamp /= 10
	}
	return length
}

func convertToInt64(value interface{}) (int64, error) {
	switch val := value.(type) {
	case float64:
		return int64(val), nil
	case int64:
		return val, nil
	case int32:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("unsupported type: %T", value)
	}
}

func TestQueryWithParams(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	bp := make([]*Point, 3)

	testMeasurement := randomMeasurement()
	p1 := &Point{
		Measurement: testMeasurement,
		Fields: map[string]any{
			"v1": 1,
			"v2": "string 1",
			"v3": 3.1415926,
			"v4": true,
		},
		Timestamp: time.Now().Add(-time.Second * 10).UnixNano(),
	}
	p2 := &Point{
		Measurement: testMeasurement,
		Fields: map[string]any{
			"v1": 2,
			"v2": "string 2",
			"v3": 2.0,
			"v4": false,
		},
		Timestamp: time.Now().Add(-time.Second * 5).UnixNano(),
	}
	p3 := &Point{
		Measurement: testMeasurement,
		Fields: map[string]any{
			"v1": 3,
			"v2": "string 3",
			"v3": 3.0,
			"v4": true,
		},
	}

	bp = append(bp, p1, p2, p3)
	err = c.WriteBatchPoints(context.Background(), database, bp)
	assert.Nil(t, err)

	time.Sleep(time.Second * 5)

	q := Query{
		Database: database,
		Command:  fmt.Sprintf("select * from %s where v1=$v1 and v2=$v2", testMeasurement),
		Params:   make(map[string]any),
	}
	q.Params["v1"] = 2
	q.Params["v2"] = "string 2"
	result, err := c.Query(q)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, len(result.Results[0].Series[0].Values))
}

// ExampleQuery
func ExampleQuery() {
	c, err := newClient(&Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	if err != nil {
		// do something
		return
	}

	// create a test database with rand suffix
	database := "test_db"
	err = c.CreateDatabase(database)
	if err != nil {
		// do something
		return
	}

	bp := make([]*Point, 3)

	testMeasurement := "weather"
	p1 := &Point{
		Measurement: testMeasurement,
		Tags: map[string]string{
			"location": "us-midwest",
		},
		Fields: map[string]any{
			"temperature": 82,
			"describe":    "ok",
		},
		Timestamp: time.Now().Add(-time.Second * 10).UnixNano(),
	}
	p2 := &Point{
		Measurement: testMeasurement,
		Tags: map[string]string{
			"location": "us-midwest",
		},
		Fields: map[string]any{
			"temperature": 83,
			"describe":    "good",
		},
		Timestamp: time.Now().Add(-time.Second * 5).UnixNano(),
	}
	p3 := &Point{
		Measurement: testMeasurement,
		Tags: map[string]string{
			"location": "us-midwest",
		},
		Fields: map[string]any{
			"temperature": 84,
			"describe":    "great",
		},
	}

	bp = append(bp, p1, p2, p3)
	err = c.WriteBatchPoints(context.Background(), database, bp)
	if err != nil {
		// do something
		return
	}

	// wait till data flush to disk
	time.Sleep(time.Second * 5)

	q := Query{
		Database: database,
		Command:  fmt.Sprintf("select * from %s where temperature=$temp", testMeasurement),
		Params:   make(map[string]any),
	}
	// bound parameter mode
	q.Params["temp"] = 83
	result, err := c.Query(q)
	if err != nil {
		// do something
		return
	}
	body, err := json.Marshal(result)
	if err != nil {
		// do something
		return
	}
	fmt.Println(string(body))
}
