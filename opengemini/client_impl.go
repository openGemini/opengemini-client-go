package opengemini

import (
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type client struct {
	config     *Config
	serverUrls []string
	cli        *http.Client
	prevIdx    atomic.Int32
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
	client := &client{
		config:     c,
		serverUrls: buildServerUrls(c.Addresses, c.TlsEnabled),
		cli:        newHttpClient(*c),
	}
	client.prevIdx.Store(-1)
	return client, nil
}

func buildServerUrls(addresses []*Address, tlsEnabled bool) []string {
	urls := make([]string, len(addresses))
	protocol := "http://"
	if tlsEnabled {
		protocol = "https://"
	}
	for i, addr := range addresses {
		urls[i] = protocol + addr.Host + ":" + strconv.Itoa(addr.Port)
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
