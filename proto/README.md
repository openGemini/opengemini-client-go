# proto

This package contains the OpenGemini protocol buffer definitions.

## Prerequisites

1. Install protoc (Protocol Buffers compiler)
2. Install protoc plugins for Go:

```shell
# Install protoc-gen-go
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# Install protoc-gen-go-grpc
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

## Usage

Run the following command in the project root directory to generate Go code:

```shell
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/*.proto
```

This command will:
- Generate Go code for protobuf messages (*.pb.go)
- Generate Go code for gRPC services (*.pb.grpc.go)
- Files will be generated in their respective directories according to source relative paths
