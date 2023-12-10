package opengemini

import (
	"context"
	"github.com/stretchr/testify/require"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func getTestClient() *client {
	ctx, cancel := context.WithCancel(context.Background())
	cli := &client{
		config: new(Config),
		serverUrls: []serverUrl{
			{
				url: "http://localhost1:8086",
			},
			{
				url: "http://localhost2:8086",
			},
		},
		cli: &http.Client{
			Timeout: time.Second * 5,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: time.Second * 5,
				}).DialContext,
			},
		},
		prevIdx: atomic.Int32{},
		ctx:     ctx,
		cancel:  cancel,
	}
	cli.prevIdx.Store(-1)
	return cli
}

func TestServersCheck(t *testing.T) {
	cli := getTestClient()
	require.NotNil(t, cli)

	url, err := cli.getServerUrl()
	require.NoError(t, err)
	require.Equal(t, "http://localhost1:8086", url)

	url, err = cli.getServerUrl()
	require.NoError(t, err)
	require.Equal(t, "http://localhost2:8086", url)
}
