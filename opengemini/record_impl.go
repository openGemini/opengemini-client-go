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
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/openGemini/opengemini-client-go/lib/record"
	"github.com/openGemini/opengemini-client-go/proto"
)

func (c *client) WriteByGRPC(ctx context.Context, rbs ...*RecordBuilderImpl) error {
	if len(rbs) == 0 {
		return ErrEmptyRecord
	}
	return c.rpcClient.writeRecords(ctx, rbs...)
}

type recordWriterClient struct {
	cfg        *GRPCConfig
	mux        sync.RWMutex
	lb         *grpcLoadBalance
	transforms map[string]transform
}

func newRecordWriterClient(cfg *GRPCConfig) (*recordWriterClient, error) {
	if len(cfg.Addresses) == 0 {
		return nil, fmt.Errorf("no grpc addresses provided: %w", ErrEmptyAddress)
	}

	balance, err := newRPCLoadBalance(cfg)
	if err != nil {
		return nil, errors.New("create grpc load balance failed: " + err.Error())
	}

	rw := &recordWriterClient{transforms: make(map[string]transform), lb: balance, cfg: cfg}
	return rw, nil
}

func (r *recordWriterClient) writeRecord(ctx context.Context, rb *RecordBuilderImpl) error {
	if err := checkDatabaseAndPolicy(rb.database, rb.retentionPolicy); err != nil {
		return err
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	name := rb.database + rb.retentionPolicy
	transform, ok := r.transforms[name]
	if !ok {
		transform = newTransform()
	}

	if err := transform.AppendRecord(rb); err != nil {
		return err
	}

	r.transforms[name] = transform

	if r.cfg.BatchConfig == nil {
		return r.flush(ctx, rb.database, rb.retentionPolicy)
	}

	if transform[rb.measurement].RowCount == 2 {
		return r.flush(ctx, rb.database, rb.retentionPolicy)
	}
	return nil
}

func (r *recordWriterClient) writeRecords(ctx context.Context, rbs ...*RecordBuilderImpl) error {
	for _, rb := range rbs {
		if err := r.writeRecord(ctx, rb); err != nil {
			return err
		}
	}
	return nil
}

func (r *recordWriterClient) flush(ctx context.Context, database, retentionPolicy string) (err error) {
	if err := checkDatabaseAndPolicy(database, retentionPolicy); err != nil {
		return err
	}

	name := database + retentionPolicy
	t, ok := r.transforms[name]
	if !ok {
		return ErrEmptyRecord
	}

	if len(t) == 0 {
		return ErrEmptyRecord
	}

	var req = &proto.WriteRequest{
		Database:        database,
		RetentionPolicy: retentionPolicy,
	}

	if r.cfg.AuthConfig != nil {
		req.Username = r.cfg.AuthConfig.Username
		req.Password = r.cfg.AuthConfig.Password
	}

	// 使用带超时的上下文
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for mst, rawRecord := range t {
		rec, err := rawRecord.ToSrvRecords()
		if err != nil {
			return fmt.Errorf("failed to convert records: %v", err)
		}
		var buff []byte
		buff, err = rec.Marshal(buff)
		if err != nil {
			return fmt.Errorf("failed to marshal record: %v", err)
		}

		req.Records = append(req.Records, &proto.Record{
			Measurement: mst,
			MinTime:     rawRecord.MinTime,
			MaxTime:     rawRecord.MaxTime,
			Block:       buff,
		})
	}

	response, err := r.lb.getClient().Write(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to write rows: %v", err)
	}

	t.reset()

	if response.Code != proto.ResponseCode_Success {
		return fmt.Errorf("failed to write rows: %s", response.String())
	}

	return nil
}

func (r *recordWriterClient) Close() error {
	return nil
}

var (
	ErrInvalidTimeColumn = errors.New("key can't be time")
	ErrEmptyName         = errors.New("empty name not allowed")
	ErrInvalidFieldType  = errors.New("invalid field type")
	ErrUnknownFieldType  = errors.New("unknown field type")
)

type Column struct {
	schema record.Field
	cv     record.ColVal
}

type columner struct {
	RowCount    int
	MinTime     int64
	MaxTime     int64
	Columns     map[string]*Column
	fillChecker map[string]bool
}

type transform map[string]*columner

// newTransform creates a new transform instance with configuration
func newTransform() transform {
	return make(transform)
}

// AppendRecord writes data by row with improved error handling
func (t *transform) AppendRecord(rbi *RecordBuilderImpl) error {
	if err := checkMeasurementName(rbi.measurement); err != nil {
		return err
	}

	c, ok := (*t)[rbi.measurement]
	if !ok {
		c = &columner{
			Columns:     make(map[string]*Column),
			fillChecker: make(map[string]bool),
		}
	}

	// process tags
	if err := c.processTagColumns(rbi.tags); err != nil {
		return err
	}

	// process fields
	if err := c.processFieldColumns(rbi.fields); err != nil {
		return err
	}

	// process timestamp
	if err := c.processTimestamp(rbi.tt); err != nil {
		return err
	}

	c.RowCount++

	// fill another field or tag
	if err := c.processMissValueColumns(); err != nil {
		return err
	}

	(*t)[rbi.measurement] = c

	return nil
}

func (t *transform) reset() {
	for k := range *t {
		delete(*t, k)
	}
}

func (c *columner) createColumn(name string, fieldType int) (*Column, error) {
	column := &Column{
		schema: record.Field{
			Type: fieldType,
			Name: name,
		},
		cv: record.ColVal{},
	}
	column.cv.Init()
	if err := c.appendFieldNulls(column, c.RowCount); err != nil {
		return nil, err
	}

	return column, nil
}

func (c *columner) appendFieldNulls(column *Column, count int) error {
	switch column.schema.Type {
	case record.FieldTypeTag, record.FieldTypeString:
		column.cv.AppendStringNulls(count)
		return nil
	case record.FieldTypeInt, record.FieldTypeUInt:
		column.cv.AppendIntegerNulls(count)
		return nil
	case record.FieldTypeBoolean:
		column.cv.AppendBooleanNulls(count)
		return nil
	case record.FieldTypeFloat:
		column.cv.AppendFloatNulls(count)
		return nil
	default:
		return ErrInvalidFieldType
	}
}

// appendFieldValue appends field value to the column
func (c *columner) appendFieldValue(column *Column, value interface{}) error {
	switch v := value.(type) {
	case string:
		column.cv.AppendString(v)
		return nil
	case bool:
		column.cv.AppendBoolean(v)
		return nil
	case float64:
		column.cv.AppendFloat(v)
		return nil
	case float32:
		column.cv.AppendFloat(float64(v))
		return nil
	case int:
		column.cv.AppendInteger(int64(v))
		return nil
	case int64:
		column.cv.AppendInteger(v)
		return nil
	case int32:
		column.cv.AppendInteger(int64(v))
		return nil
	case uint:
		column.cv.AppendInteger(int64(v))
		return nil
	case uint32:
		column.cv.AppendInteger(int64(v))
		return nil
	case uint64:
		column.cv.AppendInteger(int64(v))
		return nil
	}
	// For unknown types, try to throw error
	return ErrUnknownFieldType
}

func (c *columner) processTagColumns(tags []*FieldTuple) (err error) {
	for _, tag := range tags {
		tagColumn, ok := c.Columns[tag.Name]
		if !ok {
			tagColumn, err = c.createColumn(tag.Name, record.FieldTypeTag)
			if err != nil {
				return err
			}
		}
		// write the tag value to column, Value must be string
		tagColumn.cv.AppendString(tag.Value.(string))
		c.fillChecker[tag.Name] = true
		c.Columns[tag.Name] = tagColumn
	}
	return nil
}

func (c *columner) processFieldColumns(fields []*FieldTuple) (err error) {
	for _, field := range fields {
		fieldColumn, ok := c.Columns[field.Name]
		if !ok {
			fieldColumn, err = c.createColumn(field.Name, field.Type)
			if err != nil {
				return err
			}
		}

		if err := c.appendFieldValue(fieldColumn, field.Value); err != nil {
			return err
		}

		c.fillChecker[field.Name] = true
		c.Columns[field.Name] = fieldColumn
	}
	return nil
}

// processTimestamp handles timestamp processing with validation
func (c *columner) processTimestamp(tt time.Time) (err error) {
	var timestamp = time.Now().UnixNano()
	if !tt.IsZero() {
		timestamp = tt.UnixNano()
	}

	timeCol, exists := c.Columns[record.TimeField]
	if !exists {
		timeCol, err = c.createColumn(record.TimeField, record.FieldTypeInt)
		if err != nil {
			return err
		}
	}

	timeCol.cv.AppendInteger(timestamp)
	c.Columns[record.TimeField] = timeCol

	// Update min/max time
	if timestamp < c.MinTime {
		c.MinTime = timestamp
	}
	if timestamp > c.MaxTime {
		c.MaxTime = timestamp
	}
	return nil
}

func (c *columner) processMissValueColumns() error {
	for fieldName, ok := range c.fillChecker {
		if ok {
			continue
		}
		column, ok := c.Columns[fieldName]
		if !ok {
			continue
		}
		offset := c.RowCount - column.cv.Len
		if offset == 0 {
			continue
		}
		if err := c.appendFieldNulls(column, offset); err != nil {
			return err
		}
	}
	c.resetFillChecker()
	return nil
}

// ToSrvRecords converts to record.Record with improved sorting and validation
func (c *columner) ToSrvRecords() (*record.Record, error) {
	if len(c.Columns) == 0 {
		return nil, ErrEmptyRecord
	}

	rec := &record.Record{}
	rec.Schema = make([]record.Field, 0, len(c.Columns))
	rec.ColVals = make([]record.ColVal, 0, len(c.Columns))

	for _, column := range c.Columns {
		rec.Schema = append(rec.Schema, column.schema)
		rec.ColVals = append(rec.ColVals, column.cv)
	}

	// Sort and validate the record
	sort.Sort(rec)
	if err := record.CheckRecord(rec); err != nil {
		return nil, err
	}

	rec = record.NewColumnSortHelper().Sort(rec)

	return rec, nil
}

// resetFillChecker clears fill checker map
func (c *columner) resetFillChecker() {
	for key := range c.fillChecker {
		c.fillChecker[key] = false
	}
}
