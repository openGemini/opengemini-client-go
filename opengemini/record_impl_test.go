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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testDefaultRPCClient(t *testing.T) Client {
	return testNewClient(t, &Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}},
		GrpcConfig: &GrpcConfig{
			Addresses: []Address{{
				Host: "localhost",
				Port: 8305,
			}},
		},
	})
}

func TestNewRPCClient(t *testing.T) {
	c := testDefaultRPCClient(t)
	ctx := context.Background()
	testMeasurement := randomMeasurement()
	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	time.Sleep(time.Second)

	// 列builder
	builder, err := NewWriteRequestBuilder(database, "autogen")
	assert.Nil(t, err)
	// 行组装
	recordBuilder, err := NewRecordBuilder(testMeasurement)
	assert.Nil(t, err)

	writeRequest, err := builder.AddRecord(
		recordBuilder.NewLine().AddTag("t1", "t1").AddTag("t2", "t2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().Add(-time.Second*10).UnixNano()),
		recordBuilder.NewLine().AddTag("a1", "a1").AddTag("a2", "a2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().Add(-time.Second*5).UnixNano()),
		recordBuilder.NewLine().AddTag("b1", "b1").AddTag("b2", "b2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().UnixNano()),
	).Build()

	assert.Nil(t, err)
	err = c.WriteByGrpc(ctx, writeRequest)
	assert.Nil(t, err)

	// query
	time.Sleep(time.Second * 5)
	var cmd = "select * from " + testMeasurement
	queryResult, err := c.Query(Query{Command: cmd, Database: database})
	assert.NoError(t, err)
	assert.NotNil(t, queryResult.Results)
	assert.EqualValues(t, 1, len(queryResult.Results))
	assert.EqualValues(t, 1, len(queryResult.Results[0].Series))
	assert.NotNil(t, 1, queryResult.Results[0].Series[0])
	assert.EqualValues(t, 3, len(queryResult.Results[0].Series[0].Values))
}

func TestNewRPCClient_record_failed(t *testing.T) {
	testMeasurement := randomMeasurement()
	// create a test database with rand suffix
	database := randomDatabaseName()

	time.Sleep(time.Second)

	builder, err := NewWriteRequestBuilder(database, "autogen")
	assert.Nil(t, err)

	recordBuilder, err := NewRecordBuilder(testMeasurement)
	assert.Nil(t, err)

	_, err = builder.AddRecord(
		recordBuilder.NewLine().AddTag("time", "a1").AddTag("a2", "a2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().UnixNano()),
	).Build()

	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "key can't be time")
}

func TestNewRPCClient_multi_measurements(t *testing.T) {
	c := testDefaultRPCClient(t)
	ctx := context.Background()
	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	time.Sleep(time.Second)

	mst1 := randomMeasurement()
	mst2 := randomMeasurement()
	mst3 := randomMeasurement()

	builder, err := NewWriteRequestBuilder(database, "autogen")
	assert.Nil(t, err)
	recordBuilder1, err := NewRecordBuilder(mst1)
	assert.Nil(t, err)
	recordBuilder2, err := NewRecordBuilder(mst2)
	assert.Nil(t, err)
	recordBuilder3, err := NewRecordBuilder(mst3)
	assert.Nil(t, err)

	writeRequest, err := builder.AddRecord(
		recordBuilder1.AddTag("t1", "t1").AddTag("t2", "t2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().Add(-time.Second*10).UnixNano()),
		recordBuilder1.NewLine().AddTag("a1", "a1").AddTag("a2", "a2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().Add(-time.Second*5).UnixNano()),
		recordBuilder2.AddTag("b1", "b1").AddTag("b2", "b2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().UnixNano()),
		recordBuilder2.NewLine().AddTag("b1", "b1").AddTag("b2", "b2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().Add(-time.Second*5).UnixNano()),
		recordBuilder3.AddTag("b1", "b1").AddTag("b2", "b2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().Add(-time.Second*5).UnixNano()),
		recordBuilder3.NewLine().AddTag("b1", "b1").AddTag("b2", "b2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().Add(-time.Second*3).UnixNano()),
		recordBuilder3.NewLine().AddTag("b1", "b1").AddTag("b2", "b2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().UnixNano()),
	).Build()

	assert.Nil(t, err)
	err = c.WriteByGrpc(ctx, writeRequest)
	assert.Nil(t, err)

	// query
	time.Sleep(time.Second * 5)
	var cmd = "select * from " + mst1
	queryResult, err := c.Query(Query{Command: cmd, Database: database})
	assert.NoError(t, err)
	assert.NotNil(t, queryResult.Results)
	assert.EqualValues(t, 1, len(queryResult.Results))
	assert.EqualValues(t, 1, len(queryResult.Results[0].Series))
	assert.NotNil(t, 1, queryResult.Results[0].Series[0])
	assert.EqualValues(t, 2, len(queryResult.Results[0].Series[0].Values))

	cmd = "select * from " + mst3
	queryResult, err = c.Query(Query{Command: cmd, Database: database})
	assert.NoError(t, err)
	assert.NotNil(t, queryResult.Results)
	assert.EqualValues(t, 1, len(queryResult.Results))
	assert.EqualValues(t, 1, len(queryResult.Results[0].Series))
	assert.NotNil(t, 1, queryResult.Results[0].Series[0])
	assert.EqualValues(t, 3, len(queryResult.Results[0].Series[0].Values))
}
