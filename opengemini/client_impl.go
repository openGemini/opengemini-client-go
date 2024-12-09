// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opengemini

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/libgox/gocollections/syncx"
)

type endpoint struct {
	url    string
	isDown atomic.Bool
}

type client struct {
	config      *Config
	endpoints   []endpoint
	cli         *http.Client
	prevIdx     atomic.Int32
	dataChanMap syncx.Map[dbRp, chan *sendBatchWithCB]
	metrics     *metrics
	rpcClient   *recordWriterClient

	batchContext       context.Context
	batchContextCancel context.CancelFunc

	logger *slog.Logger
}

func newClient(c *Config) (Client, error) {
	if len(c.Addresses) == 0 {
		return nil, ErrEmptyAddress
	}
	if c.AuthConfig != nil {
		if c.AuthConfig.AuthType == AuthTypeToken && len(c.AuthConfig.Token) == 0 {
			return nil, ErrEmptyAuthToken
		}
		if c.AuthConfig.AuthType == AuthTypePassword {
			if len(c.AuthConfig.Username) == 0 {
				return nil, ErrEmptyAuthUsername
			}
			if len(c.AuthConfig.Password) == 0 {
				return nil, ErrEmptyAuthPassword
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
	dbClient := &client{
		config:             c,
		endpoints:          buildEndpoints(c.Addresses, c.TlsConfig != nil),
		cli:                newHttpClient(*c),
		metrics:            newMetricsProvider(c.CustomMetricsLabels),
		batchContext:       ctx,
		batchContextCancel: cancel,
	}
	if c.Logger != nil {
		dbClient.logger = c.Logger
	} else {
		dbClient.logger = slog.Default()
	}
	if c.RPCConfig != nil {
		rc, err := newRecordWriterClient(c.RPCConfig)
		if err != nil {
			return nil, errors.New("failed to create rpc client: " + err.Error())
		}
		dbClient.rpcClient = rc
	}
	dbClient.prevIdx.Store(-1)
	if len(c.Addresses) > 1 {
		// if there are multiple addresses, start the health check
		go dbClient.endpointsCheck(ctx)
	}
	return dbClient, nil
}

func (c *client) Close() error {
	c.batchContextCancel()
	c.dataChanMap.Range(func(key dbRp, cb chan *sendBatchWithCB) bool {
		close(cb)
		c.dataChanMap.Delete(key)
		return true
	})
	return nil
}

func buildEndpoints(addresses []Address, tlsEnabled bool) []endpoint {
	urls := make([]endpoint, len(addresses))
	protocol := "http://"
	if tlsEnabled {
		protocol = "https://"
	}
	for i, addr := range addresses {
		urls[i] = endpoint{url: protocol + net.JoinHostPort(addr.Host, strconv.Itoa(addr.Port))}
	}
	return urls
}

func newHttpClient(config Config) *http.Client {
	return &http.Client{
		Timeout: config.Timeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: config.ConnectTimeout,
			}).DialContext,
			MaxConnsPerHost:     config.MaxConnsPerHost,
			MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
			TLSClientConfig:     config.TlsConfig,
		},
	}
}
