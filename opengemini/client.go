package opengemini

import (
	"crypto/tls"
)

const (
	// AuthTypePassword Basic Authentication with the provided username and password.
	AuthTypePassword AuthType = iota
	// AuthTypeToken Token Authentication with the provided token.
	AuthTypeToken
)

// Client represents a openGemini client.
type Client interface {
	// Ping check that status of cluster.
	Ping(idx int) error
}

// Config is used to construct a openGemini Client instance.
type Config struct {
	// AddressList Configure the service URL for the openGemini service.
	// This parameter is required.
	AddressList []*Address
	// AuthConfig configuration information for authentication.
	AuthConfig *AuthConfig
	// BatchConfig configuration information for batch processing.
	BatchConfig *BatchConfig
	// GzipEnabled determines whether to use gzip for data transmission.
	GzipEnabled bool
	// TlsEnabled determines whether to use TLS for data transmission.
	TlsEnabled bool
	// TlsConfig configuration information for tls.
	TlsConfig *tls.Config
}

// Address configuration for providing service.
type Address struct {
	// Host service ip or domain.
	Host string
	// Port exposed service port.
	Port int
}

// AuthType type of identity authentication.
type AuthType int

// AuthConfig represents the configuration information for authentication.
type AuthConfig struct {
	// AuthType type of identity authentication.
	AuthType AuthType
	// Username provided username when used AuthTypePassword.
	Username string
	// Password provided password when used AuthTypePassword.
	Password string
	// Token provided token when used AuthTypeToken.
	Token string
}

// BatchConfig represents the configuration information for batch processing.
type BatchConfig struct {
	// BatchInterval batch time interval that triggers batch processing. (unit: ms)
	BatchInterval int
	// BatchSize batch size that triggers batch processing.
	BatchSize int
}

// NewClient Creates a openGemini client instance
func NewClient(config *Config) (Client, error) {
	return newClient(config)
}
