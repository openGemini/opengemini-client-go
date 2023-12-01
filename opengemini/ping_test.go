package opengemini

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPingSuccess(t *testing.T) {
	c := testDefaultClient(t)

	err := c.Ping(0)
	require.Nil(t, err)
}

func TestPingFailForInaccessibleAddress(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}, {
			Host: "localhost",
			Port: 8087,
		}},
	})

	err := c.Ping(1)
	require.NotNil(t, err)
}

func TestPingFailForOutOfRangeIndex(t *testing.T) {
	c := testDefaultClient(t)

	err := c.Ping(1)
	require.NotNil(t, err)
	err = c.Ping(-1)
	require.NotNil(t, err)
}
