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
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// AuthTypePassword Basic Authentication with the provided username and password.
	AuthTypePassword AuthType = iota
	// AuthTypeToken Token Authentication with the provided token.
	AuthTypeToken
)

type Codec string

type ContentType string
type CompressMethod string

const (
	ContentTypeMsgPack ContentType = "MSGPACK"
	ContentTypeJSON    ContentType = "JSON"
)

const (
	CompressMethodZstd CompressMethod = "ZSTD"
	CompressMethodGzip CompressMethod = "GZIP"
	CompressMethodNone CompressMethod = "NONE"
)

// Define constants for different encode/decode config

const (
	CodecMsgPack Codec = "MsgPack"
	CodecZstd    Codec = "ZSTD"
)

// Client represents a openGemini client.
type Client interface {
	// Ping check that status of cluster.
	Ping(idx int) error
	Query(query Query) (*QueryResult, error)

	// WritePoint write single point to assigned database. If you don't want to implement callbackFunc to receive error
	// in writing, you could use opengemini.CallbackDummy.
	WritePoint(database string, point *Point, callbackFunc WriteCallback) error
	// WritePointWithRp write single point with retention policy. If you don't want to implement callbackFunc to
	//  receive error in writing, you could use opengemini.CallbackDummy.
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
	UpdateRetentionPolicy(database string, rpConfig RpConfig, isDefault bool) error
	ShowRetentionPolicies(database string) ([]RetentionPolicy, error)
	DropRetentionPolicy(database, retentionPolicy string) error

	// CreateMeasurement use command `CREATE MEASUREMENT` to create measurement, openGemini supports
	// automatic table creation when writing data, but in the following three situations, tables need
	// to be created in advance.
	//  - specify a tag as partition key
	//  - text search
	//  - high series cardinality storage engine(HSCE)
	// calling NewMeasurementBuilder().Database(databaseName).Measurement(measurement).
	//		Create().Tags([]string{"tag1", "tag2"}).FieldMap(map[string]fieldType{
	//		"f_int64":  FieldTypeInt64,
	//		"f_float":  FieldTypeFloat64,
	//		"f_bool":   FieldTypeBool,
	//		"f_string": FieldTypeString,
	//	}).ShardKeys([]string{"tag1"}) is the best way to set up the
	// builder, don't forget to set the database otherwise it will return an error
	CreateMeasurement(builder CreateMeasurementBuilder) error
	// ShowMeasurements use command `SHOW MEASUREMENT` to view the measurements created in the database, calling
	// NewMeasurementBuilder.Database("db0").RetentionPolicy("rp0").Show() is the best way to set up
	// the builder, don't forget to set the database otherwise it will return an error
	ShowMeasurements(builder ShowMeasurementBuilder) ([]string, error)
	// DropMeasurement use command `DROP MEASUREMENT` to delete measurement, deleting a measurement
	// will delete all indexes, series and data. if retentionPolicy is empty, use default retention policy, don't
	// forget to set the database and measurement otherwise it will return an error
	DropMeasurement(database, retentionPolicy, measurement string) error

	// ShowTagKeys view all TAG fields in the measurements, return {"measurement_name":["TAG1","TAG2"]}
	// calling `NewShowTagKeysBuilder().Database("db0").Measurement("m0")...` to setup builder, don't forget to set the
	// database otherwise it will return an error, if retention policy is empty, use default retention policy `autogen`,
	// if measurement is empty, show all measurements
	ShowTagKeys(builder ShowTagKeysBuilder) (map[string][]string, error)
	// ShowTagValues returns the tag value of the specified tag key in the database, return ["TAG1","TAG2"]
	// calling `NewShowTagValuesBuilder().Database("db0").Measurement("m0")...` to setup builder, don't forget to set the
	// database otherwise it will return an error, if retention policy is empty, use default retention policy `autogen`,
	// if tag key is empty it will return an error
	ShowTagValues(builder ShowTagValuesBuilder) ([]string, error)
	// ShowFieldKeys get measurement schema, return {"measurement_name": {"field_name":"field_type"}}
	// if measurement not exist, return all measurements in database, otherwise return the first measurement field keys
	ShowFieldKeys(database string, measurements ...string) (map[string]map[string]string, error)
	// ShowSeries returns the series of specified databases
	// return [h2o_pH,location=coyote_creek h2o_pH,location=santa_monica h2o_feet,location=coyote_creek...]
	// calling `NewShowSeriesBuilder().Database("db0")...` to setup builder, don't forget to set the database otherwise
	// it will return an error
	ShowSeries(builder ShowSeriesBuilder) ([]string, error)

	// Close shut down resources, such as health check tasks
	Close() error

	// ExposeMetrics expose prometheus metrics, calling prometheus.MustRegister(metrics) to register
	ExposeMetrics() prometheus.Collector
}

// Config is used to construct a openGemini Client instance.
type Config struct {
	// Addresses Configure the service URL for the openGemini service.
	// This parameter is required.
	Addresses []Address
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
	// ContentType determines the content type used for data transmission.
	ContentType ContentType
	// CompressMethod determines the compress method used for data transmission.
	CompressMethod CompressMethod
	// TlsConfig configuration information for tls.
	TlsConfig *tls.Config
	// CustomMetricsLabels add custom labels to all the metrics reported by this client instance
	CustomMetricsLabels map[string]string
	// Logger structured logger for logging operations
	Logger *slog.Logger
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
