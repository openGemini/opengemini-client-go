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
	"sync"
	"time"

	"github.com/openGemini/opengemini-client-go/lib/record"
	"github.com/openGemini/opengemini-client-go/proto"
)

var (
	_              RecordBuilder = (*RecordBuilderImpl)(nil)
	recordLinePool               = &sync.Pool{New: func() any {
		return &RecordLineBuilderImpl{}
	}}
)

type RecordLine interface{}

type RecordLineBuilder interface {
	// AddTag add a tag to the record.
	// If the key exists, it will be overwritten.
	// If the key is `time`, it will cause an error.
	// If the key is empty or the value is empty, it will be ignored.
	AddTag(key string, value string) RecordLineBuilder
	// AddTags add multiple tags to the record.
	// Each entry in the map represents a tag where the key is the tag name and the value is the tag value.
	AddTags(tags map[string]string) RecordLineBuilder
	// AddField add a field to the record.
	// If the key is empty, it will be ignored.
	// If the key is `time`, it will cause an error.
	// If the key already exists, its value will be overwritten.
	AddField(key string, value interface{}) RecordLineBuilder
	// AddFields add multiple fields to the record.
	// Each entry in the map represents a field where the key is the field name and the value is the field value.
	AddFields(fields map[string]interface{}) RecordLineBuilder
	CompressMethod(method CompressMethod) RecordLineBuilder
	Error() error
	// Build specifies the time of the record.
	// If the time is not specified or zero value, the current time will be used.
	Build(tt time.Time) RecordLine
}

type RecordBuilder interface {
	Authenticate(username, password string) RecordBuilder
	AddRecord(rlb ...RecordLine) RecordBuilder
	Build() (*proto.WriteRequest, error)
}

type FieldTuple struct {
	record.Field
	Value interface{}
}

type RecordBuilderImpl struct {
	database        string
	retentionPolicy string
	username        string
	password        string
	transform       transform
	err             error
}

func (r *RecordBuilderImpl) reset() {
	r.transform.reset()
}

func (r *RecordBuilderImpl) Authenticate(username, password string) RecordBuilder {
	r.username = username
	r.password = password
	return r
}

func NewRecordBuilder(database, retentionPolicy string) RecordBuilder {
	return &RecordBuilderImpl{database: database, retentionPolicy: retentionPolicy, transform: make(transform)}
}

func (r *RecordBuilderImpl) AddRecord(rlb ...RecordLine) RecordBuilder {
	for _, lineBuilder := range rlb {
		lb, ok := lineBuilder.(*RecordLineBuilderImpl)
		if !ok {
			continue
		}
		err := r.transform.AppendRecord(lb)
		recordLinePool.Put(lb)
		if err != nil {
			r.err = errors.Join(r.err, err)
			continue
		}
	}
	return r
}

func (r *RecordBuilderImpl) Build() (*proto.WriteRequest, error) {
	defer r.reset()

	if r.err != nil {
		return nil, r.err
	}

	var req = &proto.WriteRequest{
		Database:        r.database,
		RetentionPolicy: r.retentionPolicy,
		Username:        r.username,
		Password:        r.password,
	}

	for mst, rawRecord := range r.transform {
		rec, err := rawRecord.ToSrvRecords()
		if err != nil {
			return nil, fmt.Errorf("failed to convert records: %v", err)
		}
		var buff []byte
		buff, err = rec.Marshal(buff)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal record: %v", err)
		}

		req.Records = append(req.Records, &proto.Record{
			Measurement: mst,
			MinTime:     rawRecord.MinTime,
			MaxTime:     rawRecord.MaxTime,
			Block:       buff,
		})
	}

	return req, nil
}

type RecordLineBuilderImpl struct {
	measurement    string
	tags           []*FieldTuple
	fields         []*FieldTuple
	tt             time.Time
	compressMethod CompressMethod
	built          bool

	err error
}

func (r *RecordLineBuilderImpl) CompressMethod(method CompressMethod) RecordLineBuilder {
	r.compressMethod = method
	return r
}

func newRecordLineBuilder(measurement string) *RecordLineBuilderImpl {
	r := recordLinePool.Get().(*RecordLineBuilderImpl)
	r.measurement = measurement
	if len(r.tags) != 0 {
		r.tags = r.tags[:0]
	}
	if len(r.fields) != 0 {
		r.fields = r.fields[:0]
	}
	if !r.tt.IsZero() {
		r.tt = time.Time{}
	}
	r.built = false
	r.err = nil
	return r
}

func NewRecordLineBuilder(measurement string) RecordLineBuilder {
	return newRecordLineBuilder(measurement)
}

func (r *RecordLineBuilderImpl) Error() error {
	return r.err
}

func (r *RecordLineBuilderImpl) AddTag(key string, value string) RecordLineBuilder {
	if r.built {
		r = newRecordLineBuilder(r.measurement)
	}
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

func (r *RecordLineBuilderImpl) AddTags(tags map[string]string) RecordLineBuilder {
	if r.built {
		r = newRecordLineBuilder(r.measurement)
	}
	for key, value := range tags {
		r.AddTag(key, value)
	}
	return r
}

func (r *RecordLineBuilderImpl) AddField(key string, value interface{}) RecordLineBuilder {
	if r.built {
		r = newRecordLineBuilder(r.measurement)
	}
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

func (r *RecordLineBuilderImpl) AddFields(fields map[string]interface{}) RecordLineBuilder {
	if r.built {
		r = newRecordLineBuilder(r.measurement)
	}
	for key, value := range fields {
		r.AddField(key, value)
	}
	return r
}

func (r *RecordLineBuilderImpl) Build(tt time.Time) RecordLine {
	r.built = true
	if err := checkMeasurementName(r.measurement); err != nil {
		r.err = errors.Join(err, r.err)
	}
	r.tt = tt
	return r
}
