package opengemini

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type endpoint struct {
	url    string
	isDown atomic.Bool
}

type client struct {
	config    *Config
	endpoints []endpoint
	cli       *http.Client
	prevIdx   atomic.Int32
	dataChan  sync.Map

	cancel context.CancelFunc
}

func newClient(c *Config) (Client, error) {
	if len(c.Addresses) == 0 {
		return nil, errors.New("must have at least one address")
	}
	if c.AuthConfig != nil {
		if c.AuthConfig.AuthType == AuthTypeToken && len(c.AuthConfig.Token) == 0 {
			return nil, errors.New("invalid auth config due to empty token")
		}
		if c.AuthConfig.AuthType == AuthTypePassword {
			if len(c.AuthConfig.Username) == 0 {
				return nil, errors.New("invalid auth config due to empty username")
			}
			if len(c.AuthConfig.Password) == 0 {
				return nil, errors.New("invalid auth config due to empty password")
			}
		}
	}
	if c.BatchConfig != nil {
		if c.BatchConfig.BatchInterval <= 0 {
			return nil, errors.New("batch enabled, batch interval must be great than 0")
		}
		if c.BatchConfig.BatchSize <= 0 {
			return nil, errors.New("batch enabled, batch size must be great than 0")
		}
	}
	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &client{
		config:    c,
		endpoints: buildEndpoints(c.Addresses, c.TlsEnabled),
		cli:       newHttpClient(*c),
		cancel:    cancel,
	}
	client.prevIdx.Store(-1)
	go client.endpointsCheck(ctx)
	return client, nil
}

func (c *client) Close() error {
	c.cancel()
	return nil
}

func buildEndpoints(addresses []*Address, tlsEnabled bool) []endpoint {
	urls := make([]endpoint, len(addresses))
	protocol := "http://"
	if tlsEnabled {
		protocol = "https://"
	}
	for i, addr := range addresses {
		urls[i] = endpoint{url: protocol + addr.Host + ":" + strconv.Itoa(addr.Port)}
	}
	return urls
}

func newHttpClient(config Config) *http.Client {
	if config.TlsEnabled {
		return &http.Client{
			Timeout: config.Timeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: config.ConnectTimeout,
				}).DialContext,
				TLSClientConfig: config.TlsConfig,
			},
		}
	} else {
		return &http.Client{
			Timeout: config.Timeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: config.ConnectTimeout,
				}).DialContext,
			},
		}
	}
}
