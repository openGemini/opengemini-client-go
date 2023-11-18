package opengemini

import (
	"crypto/tls"
)

const (
	AuthTypeToken AuthType = iota
	AuthTypePassword
)

type Client interface {
	Ping(idx int) error
}

type Config struct {
	AddressList []*Address
	AuthConfig  *AuthConfig
	BatchConfig *BatchConfig
	GzipEnabled bool
	TlsEnabled  bool
	TlsConfig   *tls.Config
}

type Address struct {
	Host string
	Port int
}

type AuthType int

type AuthConfig struct {
	AuthType AuthType
	Username string
	Password string
	Token    string
}

type BatchConfig struct {
	BatchInterval int
	BatchSize     int
}

func NewClient(config *Config) (Client, error) {
	return newClient(config)
}
