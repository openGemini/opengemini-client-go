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
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type shardType string

const (
	ShardTypeHash  shardType = "HASH"
	ShardTypeRange shardType = "RANGE"
)

type fieldType string

const (
	FieldTypeInt64   fieldType = "INT64"
	FieldTypeFloat64 fieldType = "FLOAT64"
	FieldTypeString  fieldType = "STRING"
	FieldTypeBool    fieldType = "BOOL"
)

type engineType string

const (
	EngineTypeColumnStore engineType = "columnstore"
)

type measurementCommand string

const (
	MeasureCreate measurementCommand = "CREATE"
	MeasureShow   measurementCommand = "SHOW"
)

type measurementBase struct {
	database        string
	retentionPolicy string
	measurement     string
}

type measurementBuilder struct {
	// command specify the command type
	command measurementCommand
	// measurementBase is the base information of measurement
	measurementBase
	// filter use regexp to filter measurements
	filter *ComparisonCondition
	// tags is the tags schema for measurement
	tags []string
	// fields is the fields schema for measurement
	fields []string
	// shardType support ShardTypeHash and ShardTypeRange two ways to break up data
	shardType shardType
	// shardKeys specify tag as partition key
	shardKeys []string
	// indexType specify `text` full-text index on
	indexType string
	// indexList specify to create a full-text index on the fields
	indexList []string
	// engineType must be EngineTypeColumnStore
	engineType engineType
	// primaryKey storage engine will create indexes on these two fields
	primaryKey []string
	// sortKeys specify the data sorting method inside the storage engine
	sortKeys []string
}

func (m *measurementBuilder) Tags(tagList []string) CreateMeasurementBuilder {
	for _, tag := range tagList {
		m.tags = append(m.tags, tag+" TAG")
	}
	return m
}

func (m *measurementBuilder) FieldMap(fields map[string]fieldType) CreateMeasurementBuilder {
	for key, value := range fields {
		m.fields = append(m.fields, key+" "+string(value)+" FIELD")
	}
	return m
}

func (m *measurementBuilder) ShardKeys(shardKeys []string) CreateMeasurementBuilder {
	m.shardKeys = shardKeys
	return m
}

func (m *measurementBuilder) ShardType(shardType shardType) CreateMeasurementBuilder {
	m.shardType = shardType
	return m
}

func (m *measurementBuilder) FullTextIndex() CreateMeasurementBuilder {
	m.indexType = "text"
	return m
}

func (m *measurementBuilder) IndexList(indexList []string) CreateMeasurementBuilder {
	m.indexList = indexList
	return m
}

func (m *measurementBuilder) EngineType(engineType engineType) CreateMeasurementBuilder {
	m.engineType = engineType
	return m
}

func (m *measurementBuilder) PrimaryKey(primaryKey []string) CreateMeasurementBuilder {
	m.primaryKey = primaryKey
	return m
}

func (m *measurementBuilder) SortKeys(sortKeys []string) CreateMeasurementBuilder {
	m.sortKeys = sortKeys
	return m
}

func (m *measurementBuilder) Filter(operator ComparisonOperator, regex string) ShowMeasurementBuilder {
	m.filter = NewComparisonCondition("MEASUREMENT", operator, regex)
	return m
}

func (m *measurementBuilder) build() (string, error) {
	err := checkDatabaseName(m.database)
	if err != nil {
		return "", err
	}
	switch m.command {
	case MeasureCreate:
		if len(m.tags) == 0 && len(m.fields) == 0 {
			return "", ErrEmptyTagOrField
		}
		var buffer strings.Builder
		buffer.WriteString(`CREATE MEASUREMENT ` + m.measurement + " (")
		if len(m.tags) != 0 {
			buffer.WriteString(strings.Join(m.tags, ","))
		}
		if len(m.tags) != 0 && len(m.fields) != 0 {
			buffer.WriteString(",")
		}
		if len(m.fields) != 0 {
			buffer.WriteString(strings.Join(m.fields, ","))
		}
		buffer.WriteString(")")
		var withIdentifier bool
		if m.indexType != "" && len(m.indexList) == 0 {
			return "", errors.New("empty index list")
		}
		if m.indexType != "" {
			withIdentifier = true
			buffer.WriteString(" WITH ")
			buffer.WriteString(" INDEXTYPE " + m.indexType)
			buffer.WriteString(" INDEXLIST " + strings.Join(m.indexList, ","))
		}
		if m.engineType != "" {
			if !withIdentifier {
				withIdentifier = true
				buffer.WriteString(" WITH ")
			}
			buffer.WriteString(" ENGINETYPE = " + string(m.engineType))
		}
		if len(m.shardKeys) != 0 {
			if !withIdentifier {
				withIdentifier = true
				buffer.WriteString(" WITH ")
			}
			buffer.WriteString(" SHARDKEY " + strings.Join(m.shardKeys, ","))
		}
		if m.shardType != "" {
			if !withIdentifier {
				withIdentifier = true
				buffer.WriteString(" WITH ")
			}
			buffer.WriteString(" TYPE " + string(m.shardType))
		}
		if len(m.primaryKey) != 0 {
			if !withIdentifier {
				withIdentifier = true
				buffer.WriteString(" WITH ")
			}
			buffer.WriteString(" PRIMARYKEY " + strings.Join(m.primaryKey, ","))
		}
		if len(m.sortKeys) != 0 {
			if !withIdentifier {
				buffer.WriteString(" WITH ")
			}
			buffer.WriteString(" SORTKEY " + strings.Join(m.sortKeys, ","))
		}
		return buffer.String(), nil
	case MeasureShow:
		var buf strings.Builder
		buf.WriteString(`SHOW MEASUREMENTS`)
		if m.filter != nil {
			// m.filter.Value can only be of string type due to Filter API
			buf.WriteString(" WITH MEASUREMENT " + string(m.filter.Operator) + " " + m.filter.Value.(string))
		}
		return buf.String(), nil
	default:
		return "", fmt.Errorf("invalid command: %s", m.command)
	}
}

func (m *measurementBuilder) Show() ShowMeasurementBuilder {
	m.command = MeasureShow
	return m
}

func (m *measurementBuilder) Create() CreateMeasurementBuilder {
	m.command = MeasureCreate
	return m
}

// Database specify measurement in database
func (m *measurementBuilder) Database(database string) MeasurementBuilder {
	m.database = database
	return m
}

// Measurement specify measurement name
func (m *measurementBuilder) Measurement(measurement string) MeasurementBuilder {
	m.measurement = measurement
	return m
}

// RetentionPolicy specify retention policy
func (m *measurementBuilder) RetentionPolicy(rp string) MeasurementBuilder {
	m.retentionPolicy = rp
	return m
}

// getMeasurementBase get measurement info base
func (m *measurementBuilder) getMeasurementBase() measurementBase {
	return m.measurementBase
}

type MeasurementBuilder interface {
	// Database specify measurement in database
	Database(database string) MeasurementBuilder
	// Measurement specify measurement name
	Measurement(measurement string) MeasurementBuilder
	// RetentionPolicy specify retention policy
	RetentionPolicy(rp string) MeasurementBuilder
	// Show use command `SHOW MEASUREMENT` to show measurements
	Show() ShowMeasurementBuilder
	// Create use command `CREATE MEASUREMENT` to create measurement
	Create() CreateMeasurementBuilder
}

// DropMeasurementBuilder drop measurement, if measurement not exist, return error
type DropMeasurementBuilder interface {
	build() (string, error)
	getMeasurementBase() measurementBase
}

type ShowMeasurementBuilder interface {
	// Filter use statements to filter measurements, operator support Match, NotMatch, Equals, NotEquals, when statement
	// is regular expression, use Match, NotMatch, else use Equals, NotEquals
	Filter(operator ComparisonOperator, regex string) ShowMeasurementBuilder
	build() (string, error)
	getMeasurementBase() measurementBase
}

type CreateMeasurementBuilder interface {
	// Tags specify tag list to create measurement
	Tags(tagList []string) CreateMeasurementBuilder
	// FieldMap specify field map to create measurement
	FieldMap(fields map[string]fieldType) CreateMeasurementBuilder
	// ShardType specify shard type to create measurement, support ShardTypeHash and ShardTypeRange two ways to
	// break up data, required when use high series cardinality storage engine(HSCE)
	ShardType(shardType shardType) CreateMeasurementBuilder
	// ShardKeys specify shard keys(tag as partition key) to create measurement, required when use
	// high series cardinality storage engine(HSCE)
	ShardKeys(shardKeys []string) CreateMeasurementBuilder
	// FullTextIndex required when want measurement support full-text index
	FullTextIndex() CreateMeasurementBuilder
	// IndexList required when specify which Field fields to create a full-text index on,
	// these fields must be 'string' data type
	IndexList(indexList []string) CreateMeasurementBuilder
	// EngineType required when want measurement support HSCE, set EngineTypeColumnStore
	EngineType(engineType engineType) CreateMeasurementBuilder
	// PrimaryKey required when use HSCE, such as the primary key is `location` and `direction`, which means that the
	// storage engine will create indexes on these two fields
	PrimaryKey(primaryKey []string) CreateMeasurementBuilder
	// SortKeys required when use HSCE, specify the data sorting method inside the storage engine, time means sorting
	// by time, and can also be changed to rtt or direction, or even other fields in the table
	SortKeys(sortKeys []string) CreateMeasurementBuilder
	build() (string, error)
	getMeasurementBase() measurementBase
}

func NewMeasurementBuilder() MeasurementBuilder {
	return &measurementBuilder{}
}

// ShowTagKeysBuilder view all TAG fields in the measurements
type ShowTagKeysBuilder interface {
	// Database specify measurement in database
	Database(database string) ShowTagKeysBuilder
	// Measurement specify measurement name
	Measurement(measurement string) ShowTagKeysBuilder
	// RetentionPolicy specify retention policy
	RetentionPolicy(rp string) ShowTagKeysBuilder
	// Limit specify limit
	Limit(limit int) ShowTagKeysBuilder
	// Offset specify offset
	Offset(offset int) ShowTagKeysBuilder
	build() (string, error)
	getMeasurementBase() measurementBase
}

type showTagKeysBuilder struct {
	// measurementBase is the base information of measurement
	measurementBase
	limit  int
	offset int
}

func (s *showTagKeysBuilder) Database(database string) ShowTagKeysBuilder {
	s.database = database
	return s
}

func (s *showTagKeysBuilder) Measurement(measurement string) ShowTagKeysBuilder {
	s.measurement = measurement
	return s
}

func (s *showTagKeysBuilder) RetentionPolicy(rp string) ShowTagKeysBuilder {
	s.retentionPolicy = rp
	return s
}

func (s *showTagKeysBuilder) Limit(limit int) ShowTagKeysBuilder {
	s.limit = limit
	return s
}

func (s *showTagKeysBuilder) Offset(offset int) ShowTagKeysBuilder {
	s.offset = offset
	return s
}

func (s *showTagKeysBuilder) build() (string, error) {
	var buf strings.Builder
	if s.database == "" {
		return "", ErrEmptyDatabaseName
	}
	buf.WriteString("SHOW TAG KEYS")

	if s.measurement != "" {
		buf.WriteString(fmt.Sprintf(" FROM %s", s.measurement))
	}
	if s.limit > 0 {
		buf.WriteString(" LIMIT " + strconv.Itoa(s.limit))
	}
	if s.offset > 0 {
		buf.WriteString(" OFFSET " + strconv.Itoa(s.offset))
	}
	return buf.String(), nil
}

func (s *showTagKeysBuilder) getMeasurementBase() measurementBase {
	return s.measurementBase
}

func NewShowTagKeysBuilder() ShowTagKeysBuilder {
	return &showTagKeysBuilder{}
}

type ShowTagValuesBuilder interface {
	// Database specify measurement in database
	Database(database string) ShowTagValuesBuilder
	// Measurement specify measurement name
	Measurement(measurement string) ShowTagValuesBuilder
	// RetentionPolicy specify retention policy
	RetentionPolicy(rp string) ShowTagValuesBuilder
	// Limit specify limit
	Limit(limit int) ShowTagValuesBuilder
	// Offset specify offset
	Offset(offset int) ShowTagValuesBuilder
	// OrderBy specify order by
	OrderBy(field string, order SortOrder) ShowTagValuesBuilder
	// With supports specifying a tag key, a regular expression or multiple tag keys, if set multiple keys, it will
	// return all tag field values, if set keys length is one and such as /regex/ it will match the regex, otherwise it
	// show one tag field values.
	With(keys ...string) ShowTagValuesBuilder
	// Where filter other key condition
	Where(key string, operator ComparisonOperator, value string) ShowTagValuesBuilder
	build() (string, error)
	getMeasurementBase() measurementBase
}

type showTagValuesBuilder struct {
	// measurementBase is the base information of measurement
	measurementBase
	limit   int
	offset  int
	orders  []string
	withKey []string
	where   *ComparisonCondition
}

func (s *showTagValuesBuilder) OrderBy(field string, order SortOrder) ShowTagValuesBuilder {
	s.orders = append(s.orders, fmt.Sprintf("%s %s", field, order))
	return s
}

func NewShowTagValuesBuilder() ShowTagValuesBuilder {
	return &showTagValuesBuilder{}
}

func (s *showTagValuesBuilder) Database(database string) ShowTagValuesBuilder {
	s.database = database
	return s
}

func (s *showTagValuesBuilder) Measurement(measurement string) ShowTagValuesBuilder {
	s.measurement = measurement
	return s
}

func (s *showTagValuesBuilder) RetentionPolicy(rp string) ShowTagValuesBuilder {
	s.retentionPolicy = rp
	return s
}

func (s *showTagValuesBuilder) Limit(limit int) ShowTagValuesBuilder {
	s.limit = limit
	return s
}

func (s *showTagValuesBuilder) Offset(offset int) ShowTagValuesBuilder {
	s.offset = offset
	return s
}

func (s *showTagValuesBuilder) With(keys ...string) ShowTagValuesBuilder {
	s.withKey = keys
	return s
}

func (s *showTagValuesBuilder) Where(key string, operator ComparisonOperator, value string) ShowTagValuesBuilder {
	s.where = NewComparisonCondition(key, operator, value)
	return s
}

func (s *showTagValuesBuilder) build() (string, error) {
	if len(s.withKey) == 0 {
		return "", ErrEmptyTagKey
	}
	var buff strings.Builder
	buff.WriteString("SHOW TAG VALUES")
	if s.measurement != "" {
		buff.WriteString(" FROM " + s.measurement)
	}
	// must be set
	if len(s.withKey) == 1 {
		key := s.withKey[0]
		if strings.HasPrefix(key, "/") && strings.HasSuffix(key, "/") {
			buff.WriteString(" WITH KEY =~ " + key)
		} else {
			buff.WriteString(" WITH KEY = \"" + key + "\"")
		}
	}

	if len(s.withKey) > 1 {
		// append double quote, void keyword
		for i := range s.withKey {
			s.withKey[i] = "\"" + s.withKey[i] + "\""
		}
		buff.WriteString(" WITH KEY IN (" + strings.Join(s.withKey, ",") + ")")
	}

	if s.where != nil {
		buff.WriteString(" WHERE " + s.where.build())
	}

	if len(s.orders) != 0 {
		buff.WriteString(" ORDER BY " + strings.Join(s.orders, ","))
	}

	if s.limit > 0 {
		buff.WriteString(" LIMIT " + strconv.Itoa(s.limit))
	}

	if s.offset > 0 {
		buff.WriteString(" OFFSET " + strconv.Itoa(s.offset))
	}

	return buff.String(), nil
}

func (s *showTagValuesBuilder) getMeasurementBase() measurementBase {
	return s.measurementBase
}

type ShowSeriesBuilder interface {
	// Database specify measurement in database
	Database(database string) ShowSeriesBuilder
	// Measurement specify measurement name
	Measurement(measurement string) ShowSeriesBuilder
	// RetentionPolicy specify retention policy
	RetentionPolicy(rp string) ShowSeriesBuilder
	// Limit specify limit
	Limit(limit int) ShowSeriesBuilder
	// Offset specify offset
	Offset(offset int) ShowSeriesBuilder
	// Where filter other key condition, notice field comparisons are invalid
	Where(key string, operator ComparisonOperator, value string) ShowSeriesBuilder
	build() (string, error)
	getMeasurementBase() measurementBase
}

type showSeriesBuilder struct {
	// measurementBase is the base information of measurement
	measurementBase
	limit  int
	offset int
	where  *ComparisonCondition
}

func (s *showSeriesBuilder) Database(database string) ShowSeriesBuilder {
	s.database = database
	return s
}

func (s *showSeriesBuilder) Measurement(measurement string) ShowSeriesBuilder {
	s.measurement = measurement
	return s
}

func (s *showSeriesBuilder) RetentionPolicy(rp string) ShowSeriesBuilder {
	s.retentionPolicy = rp
	return s
}

func (s *showSeriesBuilder) Limit(limit int) ShowSeriesBuilder {
	s.limit = limit
	return s
}

func (s *showSeriesBuilder) Offset(offset int) ShowSeriesBuilder {
	s.offset = offset
	return s
}

func (s *showSeriesBuilder) Where(key string, operator ComparisonOperator, value string) ShowSeriesBuilder {
	s.where = NewComparisonCondition(key, operator, value)
	return s
}

func (s *showSeriesBuilder) build() (string, error) {
	if s.database == "" {
		return "", ErrEmptyDatabaseName
	}
	var buff strings.Builder
	buff.WriteString("SHOW SERIES")
	if s.measurement != "" {
		buff.WriteString(" FROM " + s.measurement)
	}
	if s.where != nil {
		buff.WriteString(" WHERE " + s.where.build())
	}
	if s.limit > 0 {
		buff.WriteString(" LIMIT " + strconv.Itoa(s.limit))
	}

	if s.offset > 0 {
		buff.WriteString(" OFFSET " + strconv.Itoa(s.offset))
	}
	return buff.String(), nil
}

func (s *showSeriesBuilder) getMeasurementBase() measurementBase {
	return s.measurementBase
}

func NewShowSeriesBuilder() ShowSeriesBuilder {
	return &showSeriesBuilder{}
}
