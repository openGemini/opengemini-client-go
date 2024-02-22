package opengemini

import (
	"crypto/tls"
	"time"
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
	Query(query Query) (*QueryResult, error)

	// WritePoint write single point to assigned database
	WritePoint(database string, point *Point, callbackFunc WriteCallback) error
	// WriteBatchPoints write batch points to assigned database
	WriteBatchPoints(database string, bp *BatchPoints) error
	// CreateDatabase Create database
	CreateDatabase(database string) error
	// CreateDatabaseWithRp Create database with retention policy
	// rpConfig configuration information for retention policy
	CreateDatabaseWithRp(database string, rpConfig RpConfig) error
	ShowDatabases() ([]string, error)
	DropDatabase(database string) error

	// CreateRetentionPolicy
	// rpConfig configuration information for retention policy
	// isDefault can set the new retention policy as the default retention policy for the database
	CreateRetentionPolicy(database string, rpConfig RpConfig, isDefault bool) error
	ShowRetentionPolicy(database string) ([]RetentionPolicy, error)
	DropRetentionPolicy(database, retentionPolicy string) error

	ShowTagKeys(database, command string) ([]ValuesResult, error)
	ShowTagValues(database, command string) ([]ValuesResult, error)
	ShowFieldKeys(database, command string) ([]ValuesResult, error)
	// ShowSeries returns the series of specified databases
	// return [measurement1,tag1=value1 measurement2,tag2=value2]
	ShowSeries(database, command string) ([]string, error)

	// Close shut down resources, such as health check tasks
	Close() error
}

// Config is used to construct a openGemini Client instance.
type Config struct {
	// Addresses Configure the service URL for the openGemini service.
	// This parameter is required.
	Addresses []*Address
	// AuthConfig configuration information for authentication.
	AuthConfig *AuthConfig
	// BatchConfig configuration information for batch processing.
	BatchConfig *BatchConfig
	// Timeout default 30s
	Timeout time.Duration
	// ConnectTimeout default 10s
	ConnectTimeout time.Duration
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
	// BatchInterval batch time interval that triggers batch processing.
	BatchInterval time.Duration
	// BatchSize batch size that triggers batch processing.
	BatchSize int
}

// RpConfig represents the configuration information for retention policy
type RpConfig struct {
	// Name retention policy name
	Name string
	// Duration indicates how long the data will be retained
	Duration string
	// ShardGroupDuration determine the time range for sharding groups
	ShardGroupDuration string
	// IndexDuration determines the time range of the index group
	IndexDuration string
}

// NewClient Creates a openGemini client instance
func NewClient(config *Config) (Client, error) {
	return newClient(config)
}
