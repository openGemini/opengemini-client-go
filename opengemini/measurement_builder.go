package opengemini

import (
	"errors"
	"fmt"
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
