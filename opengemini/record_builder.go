package opengemini

import (
	"errors"
	"fmt"
	"github.com/openGemini/opengemini-client-go/lib/record"
	"time"
)

var _ RecordBuilder = (*RecordBuilderImpl)(nil)

type RecordBuilder interface {
	// Database specifies the name of the database, required
	Database(database string) RecordBuilder
	// RetentionPolicy specifies the retention policy, required
	RetentionPolicy(policy string) RecordBuilder
	// Measurement specifies the name of the measurement, required
	Measurement(measurement string) RecordBuilder
	// AddTag add a tag to the record.
	// If the key exists, it will be overwritten.
	// If the key is `time`, it will cause an error.
	// If the key is empty or the value is empty, it will be ignored.
	AddTag(key string, value string) RecordBuilder
	// AddTags add multiple tags to the record.
	// Each entry in the map represents a tag where the key is the tag name and the value is the tag value.
	AddTags(tags map[string]string) RecordBuilder
	// AddField add a field to the record.
	// If the key is empty, it will be ignored.
	// If the key is `time`, it will cause an error.
	// If the key already exists, its value will be overwritten.
	AddField(key string, value interface{}) RecordBuilder
	// AddFields add multiple fields to the record.
	// Each entry in the map represents a field where the key is the field name and the value is the field value.
	AddFields(fields map[string]interface{}) RecordBuilder
	// Time specifies the time of the record.
	// If the time is not specified or zero value, the current time will be used.
	Time(tt time.Time) RecordBuilder
	// Build returns the built RecordBuilderImpl and an error if any.
	// The returned RecordBuilderImpl can be used to build a record.Record
	// by calling record.Record.FromBuilder.
	Build() (*RecordBuilderImpl, error)
}

func NewRecordBuilder() RecordBuilder {
	return &RecordBuilderImpl{}
}

type FieldTuple struct {
	record.Field
	Value interface{}
}

type RecordBuilderImpl struct {
	database        string
	retentionPolicy string
	measurement     string
	tags            []*FieldTuple
	fields          []*FieldTuple
	tt              time.Time

	err error
}

func (r *RecordBuilderImpl) Database(database string) RecordBuilder {
	r.database = database
	return r
}

func (r *RecordBuilderImpl) RetentionPolicy(policy string) RecordBuilder {
	r.retentionPolicy = policy
	return r
}

func (r *RecordBuilderImpl) Measurement(measurement string) RecordBuilder {
	r.measurement = measurement
	return r
}

func (r *RecordBuilderImpl) Build() (*RecordBuilderImpl, error) {
	return r, nil
}

func (r *RecordBuilderImpl) AddTag(key string, value string) RecordBuilder {
	if key == "" {
		r.err = errors.Join(r.err, fmt.Errorf("miss tag name: %w", ErrEmptyName))
		return r
	}
	if key == record.TimeField {
		r.err = errors.Join(r.err, fmt.Errorf("tag name %s invalid: %w", key, ErrInvalidTimeColumn))
		return r
	}
	r.tags = append(r.tags, &FieldTuple{
		Field: record.Field{
			Name: key,
			Type: record.FieldTypeTag,
		},
		Value: value,
	})
	return r
}

func (r *RecordBuilderImpl) AddTags(tags map[string]string) RecordBuilder {
	for key, value := range tags {
		r.AddTag(key, value)
	}
	return r
}

func (r *RecordBuilderImpl) AddField(key string, value interface{}) RecordBuilder {
	if key == "" {
		r.err = errors.Join(r.err, fmt.Errorf("miss field name: %w", ErrEmptyName))
		return r
	}
	if key == record.TimeField {
		r.err = errors.Join(r.err, fmt.Errorf("field name %s invalid: %w", key, ErrInvalidTimeColumn))
		return r
	}
	typ := record.FieldTypeUnknown
	switch value.(type) {
	case string:
		typ = record.FieldTypeString
	case float32, float64:
		typ = record.FieldTypeFloat
	case bool:
		typ = record.FieldTypeBoolean
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64, int:
		typ = record.FieldTypeInt
	}
	r.fields = append(r.fields, &FieldTuple{
		Field: record.Field{
			Name: key,
			Type: typ,
		},
		Value: value,
	})
	return r
}

func (r *RecordBuilderImpl) AddFields(fields map[string]interface{}) RecordBuilder {
	for key, value := range fields {
		r.AddField(key, value)
	}
	return r
}

func (r *RecordBuilderImpl) Time(tt time.Time) RecordBuilder {
	r.tt = tt
	return r
}
