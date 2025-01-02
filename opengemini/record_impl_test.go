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
		RPCConfig: &GRPCConfig{
			Addresses: []Address{{
				Host: "localhost",
				Port: 8305,
			}},
		},
	})
}

func TestNewRPCClient1(t *testing.T) {
	c := testDefaultRPCClient(t)
	ctx := context.Background()
	db := "db0"
	rp := "autogen"
	mst := "m3"
	// create a test database with rand suffix
	//err := c.CreateDatabase(db)
	//assert.Nil(t, err)

	// delete test database before exit test case
	//defer func() {
	//	err := c.DropDatabase(db)
	//	assert.Nil(t, err)
	//}()

	//time.Sleep(time.Second * 3)

	var builder = NewRecordBuilder(db, rp)

	var recordBuilder = NewRecordLineBuilder(mst)

	writeRequest, err := builder.AddRecord(
		recordBuilder.AddTag("t1", "t1").AddTag("t2", "t2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().Add(-time.Second*10)),
		recordBuilder.AddTag("a1", "a1").AddTag("a2", "a2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now().Add(-time.Second*5)),
		recordBuilder.AddTag("b1", "b1").AddTag("b2", "b2").
			AddField("i", 100).AddField("b", true).AddField("f", 3.14).
			AddField("s1", "pi1").Build(time.Now()),
	).Build()

	assert.Nil(t, err)
	err = c.WriteByGRPC(ctx, writeRequest)
	assert.Nil(t, err)
}
