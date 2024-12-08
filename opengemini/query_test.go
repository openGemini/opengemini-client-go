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
	p.Time = time.Now()

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
	p.Time = time.Now()

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
	p.Time = time.Now()

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
	p.Time = time.Now()

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
