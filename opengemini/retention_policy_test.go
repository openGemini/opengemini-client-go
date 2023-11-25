package opengemini

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClientCreateRetentionPolicy(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.CreateDatabase("test_database")
	require.Nil(t, err)
	err = c.CreateRetentionPolicy("test_database", RpConfig{Name: "test_rp1", Duration: "3d"}, false)
	require.Nil(t, err)
	err = c.CreateRetentionPolicy("test_database", RpConfig{Name: "test_rp2", Duration: "3d", ShardGroupDuration: "1h"}, false)
	require.Nil(t, err)
	err = c.CreateRetentionPolicy("test_database", RpConfig{Name: "test_rp3", Duration: "3d", ShardGroupDuration: "1h", IndexDuration: "7h"}, false)
	require.Nil(t, err)
	err = c.CreateRetentionPolicy("test_database", RpConfig{Name: "test_rp4", Duration: "3d"}, true)
	require.Nil(t, err)
	err = c.DropRetentionPolicy("test_rp4", "test_database")
	require.Nil(t, err)
	err = c.DropRetentionPolicy("test_rp3", "test_database")
	require.Nil(t, err)
	err = c.DropRetentionPolicy("test_rp2", "test_database")
	require.Nil(t, err)
	err = c.DropRetentionPolicy("test_rp1", "test_database")
	require.Nil(t, err)
	err = c.DropDatabase("test_database")
	require.Nil(t, err)
}

func TestClientCreateRetentionPolicyNotExistDatabase(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.CreateRetentionPolicy("test_db", RpConfig{Name: "test_rp1", Duration: "3d"}, false)
	require.NotNil(t, err)
}

func TestClientCreateRetentionPolicyEmptyDatabase(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.CreateRetentionPolicy("", RpConfig{Name: "test_rp1", Duration: "3d"}, false)
	require.NotNil(t, err)
}

func TestClientDropRetentionPolicy(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	err := c.DropRetentionPolicy("test_rp1", "test_database")
	require.Nil(t, err)
}

func TestClientShowRetentionPolicy(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
	rpResult, err := c.ShowRetentionPolicy("test_database")
	require.Nil(t, err)
	require.NotEqual(t, len(rpResult), 0)
}
