package opengemini

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func testNewClient(t *testing.T, config *Config) Client {
	client, err := newClient(config)
	require.Nil(t, err)
	require.NotNil(t, client)
	return client
}
