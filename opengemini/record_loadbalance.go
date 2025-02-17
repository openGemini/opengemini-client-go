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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/openGemini/opengemini-client-go/proto"
)

type grpcEndpoint struct {
	address string
	conn    *grpc.ClientConn
	client  proto.WriteServiceClient
	mu      sync.RWMutex
}

func (e *grpcEndpoint) isHealthy() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.conn.GetState() == connectivity.Ready
}

type grpcLoadBalance struct {
	endpoints []*grpcEndpoint
	current   atomic.Int32
	stopChan  chan struct{}
}

func newRPCLoadBalance(cfg *GrpcConfig) (*grpcLoadBalance, error) {
	var dialOptions = []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  time.Second,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   time.Second * 30,
			},
			MinConnectTimeout: time.Second * 20,
		}),
		grpc.WithInitialWindowSize(1 << 24),                                    // 16MB
		grpc.WithInitialConnWindowSize(1 << 24),                                // 16MB
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 * 1024 * 1024)), // 64MB
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64 * 1024 * 1024)), // 64MB
	}

	if cfg.TlsConfig == nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		cred := credentials.NewTLS(cfg.TlsConfig)
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(cred))
	}

	lb := &grpcLoadBalance{
		stopChan: make(chan struct{}),
	}

	for _, address := range cfg.Addresses {
		addr := address.String()
		conn, err := grpc.NewClient(addr, dialOptions...)
		if err != nil {
			_ = lb.Close()
			return nil, fmt.Errorf("connect to %s failed: %v", addr, err)
		}
		ep := &grpcEndpoint{
			address: addr,
			conn:    conn,
			client:  proto.NewWriteServiceClient(conn),
		}
		lb.endpoints = append(lb.endpoints, ep)
	}

	return lb, nil
}

// getClient use polling to return the next available client
func (r *grpcLoadBalance) getClient() proto.WriteServiceClient {
	attempts := len(r.endpoints)
	for i := 0; i < attempts; i++ {
		current := r.current.Add(1)
		idx := int(current) % len(r.endpoints)
		ep := r.endpoints[idx]

		if ep.isHealthy() {
			return ep.client
		}
	}

	// no healthy endpoint, return random endpoint
	return r.endpoints[random.Intn(attempts)].client
}

// Close all endpoint
func (r *grpcLoadBalance) Close() error {
	close(r.stopChan)
	var lastErr error
	for _, ep := range r.endpoints {
		if err := ep.conn.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
