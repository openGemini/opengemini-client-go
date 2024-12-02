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

func TestNilCount(t *testing.T) {
	nc := &NilCount{}

	t.Run("init with zero total", func(t *testing.T) {
		nc.init(0, 5)
		assert.Equal(t, 0, nc.total)
		assert.Equal(t, 0, len(nc.value))
	})

	t.Run("init with values", func(t *testing.T) {
		nc.init(10, 5)
		assert.Equal(t, 10, nc.total)
		assert.Equal(t, 5, len(nc.value))
		assert.Equal(t, 0, nc.value[0])
	})

	t.Run("reuse existing slice", func(t *testing.T) {
		originalCap := cap(nc.value)
		nc.init(8, 3)
		assert.Equal(t, originalCap, cap(nc.value))
		assert.Equal(t, 3, len(nc.value))
	})
}

func TestSortAux(t *testing.T) {
	aux := &SortAux{}

	t.Run("init", func(t *testing.T) {
		times := []int64{100, 200, 150}
		aux.Init(times)

		assert.Equal(t, len(times), len(aux.Times))
		assert.Equal(t, len(times), len(aux.RowIds))

		// Check if RowIds are initialized correctly
		for i := 0; i < len(times); i++ {
			assert.Equal(t, int32(i), aux.RowIds[i])
		}

		// Check if Times are copied correctly
		assert.Equal(t, times, aux.Times)
	})

	t.Run("sort interface implementation", func(t *testing.T) {
		aux.Times = []int64{300, 100, 200}
		aux.RowIds = []int32{0, 1, 2}

		// Test Less
		assert.False(t, aux.Less(0, 1))
		assert.True(t, aux.Less(1, 2))

		// Test Len
		assert.Equal(t, 3, aux.Len())

		// Test Swap
		aux.Swap(0, 1)
		assert.Equal(t, int64(100), aux.Times[0])
		assert.Equal(t, int64(300), aux.Times[1])
		assert.Equal(t, int32(1), aux.RowIds[0])
		assert.Equal(t, int32(0), aux.RowIds[1])
	})

	t.Run("init sections", func(t *testing.T) {
		aux.Times = []int64{100, 100, 200, 300, 300}
		aux.RowIds = []int32{0, 1, 2, 3, 4}
		aux.sections = make([]int, 0)

		aux.InitSections()

		// Should create sections for same timestamps and non-consecutive RowIds
		assert.True(t, len(aux.sections) > 0)
	})
}

func TestColumnSortHelper(t *testing.T) {
	t.Run("new helper", func(t *testing.T) {
		hlp := NewColumnSortHelper()
		assert.NotNil(t, hlp)
	})

	t.Run("sort empty record", func(t *testing.T) {
		hlp := NewColumnSortHelper()
		rec := &Record{}
		result := hlp.Sort(rec)
		assert.Equal(t, rec, result)
	})

	t.Run("sort record with data", func(t *testing.T) {
		hlp := NewColumnSortHelper()

		// Create a test record with some data
		schema := []Field{
			{Name: "field1", Type: FieldTypeInt},
			{Name: "field2", Type: FieldTypeFloat},
		}
		rec := NewRecordBuilder(schema)

		// Add some timestamps
		rec.AppendTime(200)
		rec.AppendTime(100)
		rec.AppendTime(300)

		// Sort the record
		sorted := hlp.Sort(rec)

		// Verify timestamps are sorted
		times := sorted.Times()
		assert.Equal(t, int64(100), times[0])
		assert.Equal(t, int64(200), times[1])
		assert.Equal(t, int64(300), times[2])
	})
}

func TestColValDeleteLast(t *testing.T) {
	t.Run("delete from empty column", func(t *testing.T) {
		cv := &ColVal{}
		cv.deleteLast(FieldTypeInt)
		assert.Equal(t, 0, cv.Len)
	})

	t.Run("delete int value", func(t *testing.T) {
		cv := &ColVal{
			Val:    make([]byte, 8),
			Len:    1,
			Bitmap: []byte{0xFF}, // not nil
		}
		cv.deleteLast(FieldTypeInt)
		assert.Equal(t, 0, cv.Len)
		assert.Equal(t, 0, len(cv.Val))
	})

	t.Run("delete nil value", func(t *testing.T) {
		cv := &ColVal{
			Val:      make([]byte, 8),
			Len:      1,
			Bitmap:   []byte{0x00}, // nil value
			NilCount: 1,
		}
		cv.deleteLast(FieldTypeInt)
		assert.Equal(t, 0, cv.Len)
		assert.Equal(t, 0, cv.NilCount)
	})

	t.Run("delete string value", func(t *testing.T) {
		cv := &ColVal{
			Val:    []byte("test"),
			Len:    1,
			Bitmap: []byte{0xFF},
			Offset: []uint32{0, 4},
		}
		cv.deleteLast(FieldTypeString)
		assert.Equal(t, 0, cv.Len)
		assert.Equal(t, 0, len(cv.Val))
		assert.Equal(t, 0, len(cv.Offset))
	})
}

func TestAppendWithNilCount(t *testing.T) {
	t.Run("append int values", func(t *testing.T) {
		src := &ColVal{
			Val:    []byte{1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0},
			Len:    2,
			Bitmap: []byte{0xFF}, // no nil values
		}
		dst := &ColVal{}
		nc := &NilCount{total: 0}

		dst.AppendWithNilCount(src, FieldTypeInt, 0, 2, nc)
		assert.Equal(t, 2, dst.Len)
		assert.Equal(t, 16, len(dst.Val))
		assert.Equal(t, 0, dst.NilCount)
	})

	t.Run("append with nil values", func(t *testing.T) {
		src := &ColVal{
			Val:      []byte{1, 0, 0, 0, 0, 0, 0, 0},
			Len:      2,
			Bitmap:   []byte{0x01}, // second value is nil
			NilCount: 1,
		}
		dst := &ColVal{}
		nc := &NilCount{
			total: 1,
			value: []int{0, 1},
		}

		dst.AppendWithNilCount(src, FieldTypeInt, 0, 2, nc)
		assert.Equal(t, 2, dst.Len)
		assert.Equal(t, 8, len(dst.Val))
		assert.Equal(t, 1, dst.NilCount)
	})

	t.Run("append string values", func(t *testing.T) {
		src := &ColVal{
			Val:    []byte("test"),
			Len:    1,
			Bitmap: []byte{0xFF},
			Offset: []uint32{0, 4},
		}
		dst := &ColVal{}
		nc := &NilCount{total: 0}

		dst.AppendWithNilCount(src, FieldTypeString, 0, 1, nc)
		assert.Equal(t, 1, dst.Len)
		assert.Equal(t, 4, len(dst.Val))
		assert.Equal(t, 2, len(dst.Offset))
	})
}
