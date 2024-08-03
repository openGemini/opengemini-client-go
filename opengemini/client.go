package opengemini

import (
	"context"
	"crypto/tls"
	"github.com/prometheus/client_golang/prometheus"
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

	// WritePoint write single point to assigned database. If you don't want to implement callbackFunc to receive error
	// in writing, you cloud use opengemini.CallbackDummy.
	WritePoint(database string, point *Point, callbackFunc WriteCallback) error
	// WritePointWithRp write single point with retention policy. If you don't want to implement callbackFunc to
	//  receive error in writing, you cloud use opengemini.CallbackDummy.
	WritePointWithRp(database string, rp string, point *Point, callbackFunc WriteCallback) error
	// WriteBatchPoints write batch points to assigned database
	WriteBatchPoints(ctx context.Context, database string, bp []*Point) error
	// WriteBatchPointsWithRp write batch points with retention policy
	WriteBatchPointsWithRp(ctx context.Context, database string, rp string, bp []*Point) error

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
	ShowRetentionPolicies(database string) ([]RetentionPolicy, error)
	DropRetentionPolicy(database, retentionPolicy string) error

	ShowTagKeys(database, command string) ([]ValuesResult, error)
	ShowTagValues(database, command string) ([]ValuesResult, error)
	ShowFieldKeys(database, command string) ([]ValuesResult, error)
	// ShowSeries returns the series of specified databases
	// return [measurement1,tag1=value1 measurement2,tag2=value2]
	ShowSeries(database, command string) ([]string, error)

	// Close shut down resources, such as health check tasks
	Close() error

	// ExposeMetrics expose prometheus metrics, calling prometheus.MustRegister(metrics) to register
	ExposeMetrics() prometheus.Collector
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
	// MaxConnsPerHost optionally limits the total number of
	// connections per host. Zero means no limit. Default is 0
	MaxConnsPerHost int
	// MaxIdleConnsPerHost, if non-zero, controls the maximum idle
	// (keep-alive) connections to keep per-host. If zero,
	// DefaultMaxIdleConnsPerHost is used.
	MaxIdleConnsPerHost int
	// GzipEnabled determines whether to use gzip for data transmission.
	GzipEnabled bool
	// TlsEnabled determines whether to use TLS for data transmission.
	TlsEnabled bool
	// TlsConfig configuration information for tls.
	TlsConfig *tls.Config
	// CustomMetricsLabels add custom labels to all the metrics reported by this client instance
	CustomMetricsLabels map[string]string
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
	// BatchSize batch size that triggers batch processing, set the batch size appropriately, if set too large,
	// it may cause client overflow or server-side rejected the request.
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
