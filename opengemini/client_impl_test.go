package opengemini

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSelector(t *testing.T) {
	s := &selector{
		idx:        0,
		serverUrls: []string{"http://localhost:9086", "http://localhost:9087"},
	}

	serverUrl := s.searchByIdx(1)
	require.Equal(t, "http://localhost:9087", serverUrl)

	serverUrl = s.search()
	require.Equal(t, "http://localhost:9086", serverUrl)

	serverUrl = s.search()
	require.Equal(t, "http://localhost:9087", serverUrl)

	_, alive := s.searchAlive()
	require.Equal(t, false, alive)
}
