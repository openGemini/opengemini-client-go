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
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/exp/slog"

	"github.com/stretchr/testify/assert"
)

func setHandleFunc() {
	http.HandleFunc("/ping", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(204)
	})
}

func startServer() (int, *http.Server, error) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, nil, err
	}

	server := &http.Server{Handler: http.DefaultServeMux}
	go func() {
		if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("Error serving requests: %v", err)
		}
	}()

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = server.Close()
		return 0, nil, fmt.Errorf("failed to get listen port")
	}
	return addr.Port, server, nil
}

func TestServerCheck(t *testing.T) {
	setHandleFunc()
	port1, server1, err := startServer()
	assert.NoError(t, err)
	port2, server2, err := startServer()
	assert.NoError(t, err)
	cli := &client{
		config: &Config{},
		endpoints: []endpoint{
			{url: "http://localhost:" + strconv.Itoa(port1)},
			{url: "http://localhost:" + strconv.Itoa(port2)},
		},
		cli: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: time.Second * 3,
				}).DialContext,
			},
		},
		prevIdx: atomic.Int32{},
		logger:  slog.Default(),
	}
	cli.prevIdx.Store(-1)
	var ctx context.Context
	ctx, cli.batchContextCancel = context.WithCancel(context.Background())
	go cli.endpointsCheck(ctx)

	url, err := cli.getServerUrl()
	assert.NoError(t, err)
	assert.Equal(t, cli.endpoints[0].url, url)

	url, err = cli.getServerUrl()
	assert.NoError(t, err)
	assert.Equal(t, cli.endpoints[1].url, url)

	err = server1.Close()
	assert.NoError(t, err)

	time.Sleep(time.Second * 15)
	url, err = cli.getServerUrl()
	assert.NoError(t, err)
	assert.Equal(t, cli.endpoints[1].url, url)

	err = server2.Close()
	assert.NoError(t, err)

	time.Sleep(time.Second * 15)
	_, err = cli.getServerUrl()
	assert.Equal(t, ErrAllServersDown, err)

	err = cli.Close()
	assert.NoError(t, err)
}
