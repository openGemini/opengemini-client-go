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

	"github.com/stretchr/testify/assert"
)

func testDefaultRPCClient(t *testing.T) Client {
	return testNewClient(t, &Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}},
		RPCConfig: &GRPCConfig{
			Addresses: []Address{{
				Host: "localhost",
				Port: 8305,
			}},
			BatchConfig: &BatchConfig{},
		},
	})
}

func TestNewRPCClient(t *testing.T) {
	c := testDefaultRPCClient(t)
	ctx := context.Background()
	db := "db0"
	rp := "autogen"
	mst := "m0"
	// create a test database with rand suffix
	//err := c.CreateDatabase(db)
	//assert.Nil(t, err)

	// delete test database before exit test case
	//defer func() {
	//	err := c.DropDatabase(db)
	//	assert.Nil(t, err)
	//}()

	//time.Sleep(time.Second * 3)

	rec, err := NewRecordBuilder().Database(db).RetentionPolicy(rp).Measurement(mst).
		AddTag("t1", "t1").AddTag("t2", "t2").AddField("i", 100).
		AddField("b", true).AddField("f", 3.14).AddField("s1", "pi1").Build()
	assert.Nil(t, err)
	err = c.WriteByGRPC(ctx, rec)
	assert.Nil(t, err)
}

func TestNewRPCClient1(t *testing.T) {
	c := testDefaultRPCClient(t)
	ctx := context.Background()
	db := "db0"
	rp := "autogen"
	mst := "m0"
	// create a test database with rand suffix
	//err := c.CreateDatabase(db)
	//assert.Nil(t, err)

	// delete test database before exit test case
	//defer func() {
	//	err := c.DropDatabase(db)
	//	assert.Nil(t, err)
	//}()

	//time.Sleep(time.Second * 3)

	rec, err := NewRecordBuilder().Database(db).RetentionPolicy(rp).Measurement(mst).
		AddTag("t1", "t1").AddTag("t2", "t2").AddField("i", 100).
		AddField("b", true).AddField("f", 3.14).AddField("s1", "pi1").Build()
	assert.Nil(t, err)
	err = c.WriteByGRPC(ctx, rec)
	assert.Nil(t, err)

	rec1, err := NewRecordBuilder().Database(db).RetentionPolicy(rp).Measurement(mst).
		AddTag("t1", "t1").AddTag("t3", "t3").AddField("i", 100).
		AddField("b", true).AddField("f", 3.14).AddField("s1", "pi1").Build()
	assert.Nil(t, err)
	err = c.WriteByGRPC(ctx, rec1)
	assert.Nil(t, err)
}
