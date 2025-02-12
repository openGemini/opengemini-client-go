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
	"math/rand"
	"time"

	"github.com/openGemini/opengemini-client-go/lib/record"
	"github.com/openGemini/opengemini-client-go/proto"
)

var (
	_      WriteRequestBuilder = (*writeRequestBuilderImpl)(nil)
	random                     = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// RecordLine define an abstract record line structure.
type RecordLine any

// RecordBuilder build record line, it is not thread safe
type RecordBuilder interface {
	// NewLine start a new line, otherwise the added attributes will be in the default row
	NewLine() RecordBuilder
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
	// CompressMethod set compress method for request data.
	CompressMethod(method CompressMethod) RecordBuilder
	// Build specifies the time of the record.
	// If the time is not specified or zero value, the current time will be used.
	Build(timestamp int64) RecordLine
}

type WriteRequestBuilder interface {
	// Authenticate configuration write request information for authentication.
	Authenticate(username, password string) WriteRequestBuilder
	// AddRecord append Record for WriteRequest, you'd better use NewRecordBuilder to build RecordLine.
	AddRecord(rlb ...RecordLine) WriteRequestBuilder
	// Build generate WriteRequest.
	Build() (*proto.WriteRequest, error)
}

type fieldTuple struct {
	record.Field
	value interface{}
}

type writeRequestBuilderImpl struct {
	database        string
	retentionPolicy string
	username        string
	password        string
	transform       transform
	err             error
}

func (r *writeRequestBuilderImpl) reset() {
	r.transform.reset()
}

func (r *writeRequestBuilderImpl) Authenticate(username, password string) WriteRequestBuilder {
	r.username = username
	r.password = password
	return r
}

func NewWriteRequestBuilder(database, retentionPolicy string) (WriteRequestBuilder, error) {
	if err := checkDatabaseName(database); err != nil {
		return nil, err
	}
	return &writeRequestBuilderImpl{database: database, retentionPolicy: retentionPolicy, transform: make(transform)}, nil
}

func (r *writeRequestBuilderImpl) AddRecord(rlb ...RecordLine) WriteRequestBuilder {
	for _, lineBuilder := range rlb {
		lb, ok := lineBuilder.(*recordLineBuilderImpl)
		if !ok {
			continue
		}
		if lb.err != nil {
			r.err = errors.Join(r.err, lb.err)
			continue
		}
		err := r.transform.AppendRecord(lb)
		if err != nil {
			r.err = errors.Join(r.err, err)
			continue
		}
	}
	return r
}

func (r *writeRequestBuilderImpl) Build() (*proto.WriteRequest, error) {
	defer r.reset()

	if r.err != nil {
		return nil, r.err
	}

	if r.database == "" {
		return nil, ErrEmptyDatabaseName
	}

	if r.retentionPolicy == "" {
		r.retentionPolicy = "autogen"
	}

	var req = &proto.WriteRequest{
		Database:        r.database,
		RetentionPolicy: r.retentionPolicy,
		Username:        r.username,
		Password:        r.password,
	}

	for mst, rawRecord := range r.transform {
		rec, err := rawRecord.toSrvRecords()
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

type recordLineBuilderImpl struct {
	measurement    string
	tags           []*fieldTuple
	fields         []*fieldTuple
	timestamp      int64
	compressMethod CompressMethod
	err            error
}

func (r *recordLineBuilderImpl) NewLine() RecordBuilder {
	return &recordLineBuilderImpl{measurement: r.measurement}
}

func NewRecordBuilder(measurement string) (RecordBuilder, error) {
	if err := checkMeasurementName(measurement); err != nil {
		return nil, err
	}
	return &recordLineBuilderImpl{measurement: measurement}, nil
}

func (r *recordLineBuilderImpl) CompressMethod(method CompressMethod) RecordBuilder {
	r.compressMethod = method
	return r
}

func (r *recordLineBuilderImpl) AddTag(key string, value string) RecordBuilder {
	if key == "" {
		r.err = errors.Join(r.err, fmt.Errorf("miss tag name: %w", ErrEmptyName))
		return r
	}
	if key == record.TimeField {
		r.err = errors.Join(r.err, fmt.Errorf("tag name %s invalid: %w", key, ErrInvalidTimeColumn))
		return r
	}
	r.tags = append(r.tags, &fieldTuple{
		Field: record.Field{
			Name: key,
			Type: record.FieldTypeTag,
		},
		value: value,
	})
	return r
}

func (r *recordLineBuilderImpl) AddTags(tags map[string]string) RecordBuilder {
	for key, value := range tags {
		r.AddTag(key, value)
	}
	return r
}

func (r *recordLineBuilderImpl) AddField(key string, value interface{}) RecordBuilder {
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
	r.fields = append(r.fields, &fieldTuple{
		Field: record.Field{
			Name: key,
			Type: typ,
		},
		value: value,
	})
	return r
}

func (r *recordLineBuilderImpl) AddFields(fields map[string]interface{}) RecordBuilder {
	for key, value := range fields {
		r.AddField(key, value)
	}
	return r
}

func (r *recordLineBuilderImpl) Build(t int64) RecordLine {
	r.timestamp = t
	return r
}
