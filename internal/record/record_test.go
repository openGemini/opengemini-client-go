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

package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRecordBuilder(t *testing.T) {
	schema := []Field{
		{Name: "field1", Type: FieldTypeInt},
		{Name: "field2", Type: FieldTypeString},
		{Name: TimeField, Type: FieldTypeInt},
	}

	rec := NewRecordBuilder(schema)
	assert.NotNil(t, rec)
	assert.Equal(t, len(schema), len(rec.Schema))
	assert.Equal(t, len(schema), len(rec.ColVals))
	assert.Equal(t, schema, []Field(rec.Schema))
}

func TestRecordSort(t *testing.T) {
	schema := []Field{
		{Name: "field1", Type: FieldTypeInt},
		{Name: "field2", Type: FieldTypeString},
		{Name: TimeField, Type: FieldTypeInt},
	}
	rec := NewRecordBuilder(schema)

	t.Run("test Less", func(t *testing.T) {
		// TimeField should always be last
		assert.False(t, rec.Less(2, 1)) // time vs non-time
		assert.True(t, rec.Less(1, 2))  // non-time vs time
		assert.True(t, rec.Less(0, 1))  // field1 vs field2
	})

	t.Run("test Swap", func(t *testing.T) {
		rec.Swap(0, 1)
		assert.Equal(t, "field2", rec.Schema[0].Name)
		assert.Equal(t, "field1", rec.Schema[1].Name)
		assert.Equal(t, TimeField, rec.Schema[2].Name)
	})

	t.Run("test Len", func(t *testing.T) {
		assert.Equal(t, 3, rec.Len())
	})
}

func TestRecordString(t *testing.T) {
	schema := []Field{
		{Name: "int_field", Type: FieldTypeInt},
		{Name: "float_field", Type: FieldTypeFloat},
		{Name: "bool_field", Type: FieldTypeBoolean},
		{Name: "string_field", Type: FieldTypeString},
		{Name: TimeField, Type: FieldTypeInt},
	}
	rec := NewRecordBuilder(schema)

	// Add some values
	rec.ColVals[0].AppendInteger(123)
	rec.ColVals[1].AppendFloat(3.14)
	rec.ColVals[2].AppendBoolean(true)
	rec.ColVals[3].AppendString("test")
	rec.AppendTime(1000)

	str := rec.String()
	assert.Contains(t, str, "int_field")
	assert.Contains(t, str, "float_field")
	assert.Contains(t, str, "bool_field")
	assert.Contains(t, str, "string_field")
	assert.Contains(t, str, "time")
}

func TestRecordReset(t *testing.T) {
	schema := []Field{
		{Name: "field1", Type: FieldTypeInt},
		{Name: TimeField, Type: FieldTypeInt},
	}
	rec := NewRecordBuilder(schema)
	rec.AppendTime(100)

	rec.Reset()
	assert.Equal(t, 0, len(rec.Schema))
	assert.Equal(t, 0, len(rec.ColVals))
}

func TestRecordReserveColVal(t *testing.T) {
	schema := []Field{
		{Name: "field1", Type: FieldTypeInt},
	}
	rec := NewRecordBuilder(schema)

	t.Run("reserve within capacity", func(t *testing.T) {
		rec.ReserveColVal(1)
		assert.Equal(t, 2, cap(rec.ColVals))
		assert.Equal(t, 2, len(rec.ColVals))
	})

	t.Run("reserve beyond capacity", func(t *testing.T) {
		rec.ReserveColVal(10)
		assert.True(t, cap(rec.ColVals) >= 11)
		assert.Equal(t, 12, len(rec.ColVals))
	})
}

func TestRecordTimes(t *testing.T) {
	schema := []Field{
		{Name: "field1", Type: FieldTypeInt},
		{Name: TimeField, Type: FieldTypeInt},
	}
	rec := NewRecordBuilder(schema)

	t.Run("two fields", func(t *testing.T) {
		assert.Equal(t, 2, rec.Len())
	})

	t.Run("with timestamps", func(t *testing.T) {
		rec.AppendTime(100, 200, 300)
		times := rec.Times()
		assert.Equal(t, []int64{100, 200, 300}, times)
	})
}

func TestRecordMarshal(t *testing.T) {
	schema := []Field{
		{Name: "field1", Type: FieldTypeInt},
		{Name: TimeField, Type: FieldTypeInt},
	}
	rec := NewRecordBuilder(schema)
	rec.AppendTime(100)

	buf := make([]byte, 0)
	result, err := rec.Marshal(buf)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, len(result) > 0)
}

func TestRecordRowNums(t *testing.T) {
	t.Run("nil record", func(t *testing.T) {
		var rec *Record
		assert.Equal(t, 0, rec.RowNums())
	})

	t.Run("empty record", func(t *testing.T) {
		rec := &Record{}
		assert.Equal(t, 0, rec.RowNums())
	})

	t.Run("record with data", func(t *testing.T) {
		schema := []Field{
			{Name: "field1", Type: FieldTypeInt},
			{Name: TimeField, Type: FieldTypeInt},
		}
		rec := NewRecordBuilder(schema)
		rec.AppendTime(100, 200, 300)
		assert.Equal(t, 3, rec.RowNums())
	})
}

func TestCheckRecord(t *testing.T) {
	t.Run("invalid record", func(t *testing.T) {
		schema := []Field{
			{Name: "field1", Type: FieldTypeInt},
			{Name: TimeField, Type: FieldTypeInt},
		}
		rec := NewRecordBuilder(schema)
		rec.ColVals[0].AppendInteger(123)
		rec.AppendTime(100)

		err := CheckRecord(rec)
		assert.NoError(t, err)
	})

	t.Run("invalid time field position", func(t *testing.T) {
		schema := []Field{
			{Name: TimeField, Type: FieldTypeInt},
			{Name: "field1", Type: FieldTypeInt},
		}
		rec := NewRecordBuilder(schema)
		rec.ColVals[0].AppendInteger(100)
		rec.ColVals[1].AppendInteger(123)

		err := CheckRecord(rec)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid schema")
	})

	t.Run("duplicate field names", func(t *testing.T) {
		schema := []Field{
			{Name: "field1", Type: FieldTypeInt},
			{Name: "field1", Type: FieldTypeInt},
			{Name: TimeField, Type: FieldTypeInt},
		}
		rec := NewRecordBuilder(schema)
		rec.ColVals[0].AppendInteger(123)
		rec.ColVals[1].AppendInteger(456)
		rec.AppendTime(100)

		err := CheckRecord(rec)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "same schema")
	})

	t.Run("nil in time field", func(t *testing.T) {
		schema := []Field{
			{Name: "field1", Type: FieldTypeInt},
			{Name: TimeField, Type: FieldTypeInt},
		}
		rec := NewRecordBuilder(schema)
		rec.ColVals[0].AppendInteger(123)
		appendNull(&rec.ColVals[1])

		err := CheckRecord(rec)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid colvals")
	})

	t.Run("inconsistent column lengths", func(t *testing.T) {
		schema := []Field{
			{Name: "field1", Type: FieldTypeInt},
			{Name: TimeField, Type: FieldTypeInt},
		}
		rec := NewRecordBuilder(schema)
		rec.ColVals[0].AppendInteger(123)
		rec.ColVals[0].AppendInteger(456)
		rec.AppendTime(100)

		err := CheckRecord(rec)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid colvals length")
	})

	t.Run("incorrect data length for type", func(t *testing.T) {
		schema := []Field{
			{Name: "field1", Type: FieldTypeInt},
			{Name: TimeField, Type: FieldTypeInt},
		}
		rec := NewRecordBuilder(schema)
		rec.ColVals[0].AppendInteger(123)
		rec.AppendTime(100)

		err := CheckRecord(rec)
		assert.NoError(t, err)
	})

	t.Run("empty record", func(t *testing.T) {
		rec := NewRecordBuilder(nil)
		err := CheckRecord(rec)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid schema")
	})

	t.Run("string field type", func(t *testing.T) {
		schema := []Field{
			{Name: "str_field", Type: FieldTypeString},
			{Name: TimeField, Type: FieldTypeInt},
		}
		rec := NewRecordBuilder(schema)
		rec.ColVals[0].AppendString("test")
		rec.AppendTime(100)

		err := CheckRecord(rec)
		assert.NoError(t, err) // String type does not check data length
	})

	t.Run("valid record with multiple types", func(t *testing.T) {
		schema := []Field{
			{Name: "int_field", Type: FieldTypeInt},
			{Name: "float_field", Type: FieldTypeFloat},
			{Name: "bool_field", Type: FieldTypeBoolean},
			{Name: "string_field", Type: FieldTypeString},
			{Name: TimeField, Type: FieldTypeInt},
		}
		rec := NewRecordBuilder(schema)

		rec.ColVals[0].AppendInteger(123)
		rec.ColVals[1].AppendFloat(3.14)
		rec.ColVals[2].AppendBoolean(true)
		rec.ColVals[3].AppendString("test")
		rec.AppendTime(100)

		err := CheckRecord(rec)
		assert.NoError(t, err)
	})
}
