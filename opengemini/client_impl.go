package opengemini

import (
	"errors"
	"net/http"
	"strconv"
)

type client struct {
	config     *Config
	serverUrls []string
	cli        *http.Client
}

func newClient(c *Config) (Client, error) {
	if len(c.AddressList) == 0 {
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
	client := &client{
		config:     c,
		serverUrls: buildServerUrls(c.AddressList, c.TlsEnabled),
		cli:        newHttpClient(*c),
	}
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
			Transport: &http.Transport{
				TLSClientConfig: config.TlsConfig,
			},
		}
	} else {
		return &http.Client{}
	}
}
