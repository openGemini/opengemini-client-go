package opengemini

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func testDefaultClient(t *testing.T) Client {
	return testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "localhost",
			Port: 8086,
		}},
	})
}

func testNewClient(t *testing.T, config *Config) Client {
	client, err := newClient(config)
	require.Nil(t, err)
	require.NotNil(t, client)
	return client
}

func randomDatabaseName() string {
	return RandStr(8)
}

func randomRetentionPolicy() string {
	return RandStr(8)
}

func randomMeasurement() string {
	return RandStr(8)
}
