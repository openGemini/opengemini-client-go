package opengemini

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClientCreateDatabase(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.CreateDatabase("test4_database")
	require.Nil(t, err)
}

func TestClientCreateDatabaseEmptyDatabase(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.CreateDatabase("")
	require.NotNil(t, err)
}

func TestClientCreateDatabaseWithRp(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.CreateDatabaseWithRp("test4_database", RpConfig{Name: "test4", Duration: "1d", ShardGroupDuration: "1h", IndexDuration: "7h"})
	require.Nil(t, err)
}

func TestClientCreateDatabaseWithRpInvalid(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.CreateDatabaseWithRp("test4_database", RpConfig{Name: "test4", Duration: "1", ShardGroupDuration: "1h", IndexDuration: "7h"})
	require.NotNil(t, err)
}

func TestClientCreateDatabaseWithRpEmptyDatabase(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.CreateDatabaseWithRp("", RpConfig{Name: "test4", Duration: "1h", ShardGroupDuration: "1h", IndexDuration: "7h"})
	require.NotNil(t, err)
}

func TestClientShowDatabase(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	_, err := c.ShowDatabase()
	require.Nil(t, err)
}

func TestClientDropDatabase(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.DropDatabase("vvv_database")
	require.Nil(t, err)
}

func TestClientDropDatabaseEmptyDatabase(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.DropDatabase("")
	require.NotNil(t, err)
}
