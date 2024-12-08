package opengemini

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func testDefaultRPCClient(t *testing.T) Client {
	return testNewClient(t, &Config{
		Addresses: []Address{{
			Host: "localhost",
			Port: 8086,
		}},
		RPCConfig: &RPCConfig{
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

	rec1, err := NewRecordBuilder().Database(db).RetentionPolicy(rp).Measurement(mst).
		AddTag("t1", "t1").AddTag("t3", "t3").AddField("i", 100).
		AddField("b", true).AddField("f", 3.14).AddField("s1", "pi1").Build()
	assert.Nil(t, err)
	err = c.WriteByGRPC(ctx, rec1)
}
