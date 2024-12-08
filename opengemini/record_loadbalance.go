package opengemini

import (
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/openGemini/opengemini-client-go/proto"
)

type grpcEndpoint struct {
	address   string
	available atomic.Bool
	conn      *grpc.ClientConn
	client    proto.WriteServiceClient
}

type grpcLoadBalance struct {
	endpoints []*grpcEndpoint
}

func newRPCLoadBalance(cfg *RPCConfig) (*grpcLoadBalance, error) {
	var eps []*grpcEndpoint
	var dialOptions = []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithInitialWindowSize(1 << 24),                                    // 16MB
		grpc.WithInitialConnWindowSize(1 << 24),                                // 16MB
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 * 1024 * 1024)), // 64MB
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64 * 1024 * 1024)), // 64MB
	}

	if cfg.TlsConfig == nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		creds := credentials.NewTLS(cfg.TlsConfig)
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	}
	for _, address := range cfg.Addresses {
		addr := address.String()
		conn, err := grpc.NewClient(addr, dialOptions...)
		if err != nil {
			return nil, fmt.Errorf("connect to %s failed: %v", addr, err)
		}
		eps = append(eps, &grpcEndpoint{
			address: addr,
			conn:    conn,
			client:  proto.NewWriteServiceClient(conn)})
	}
	return &grpcLoadBalance{endpoints: eps}, nil
}

// getClient returns the next available WriteService client.
func (r *grpcLoadBalance) getClient() proto.WriteServiceClient {
	return r.endpoints[0].client
}

func (r *grpcLoadBalance) checkEndpoints() error {
	panic("implement me")
}

// Close closes the RPC load balancer and any underlying resources.
func (r *grpcLoadBalance) Close() error {
	panic("implement me")
}
