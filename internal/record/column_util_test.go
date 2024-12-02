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

func TestColVal_Init(t *testing.T) {
	cv := &ColVal{
		Val:          []byte{1, 2, 3},
		Offset:       []uint32{0, 1, 2},
		Bitmap:       []byte{0xFF},
		BitMapOffset: 1,
		Len:          3,
		NilCount:     1,
	}

	cv.Init()
	assert.Equal(t, 0, len(cv.Val))
	assert.Equal(t, 0, len(cv.Offset))
	assert.Equal(t, 0, len(cv.Bitmap))
	assert.Equal(t, 0, cv.BitMapOffset)
	assert.Equal(t, 0, cv.Len)
	assert.Equal(t, 0, cv.NilCount)
}

func TestColVal_ReserveOffset(t *testing.T) {
	cv := &ColVal{}

	t.Run("reserve new offset", func(t *testing.T) {
		cv.reserveOffset(3)
		assert.Equal(t, 3, len(cv.Offset))
		assert.True(t, cap(cv.Offset) >= 3)
	})

	t.Run("reserve within capacity", func(t *testing.T) {
		originalCap := cap(cv.Offset)
		cv.reserveOffset(1)
		assert.Equal(t, 4, len(cv.Offset))
		assert.Equal(t, originalCap, cap(cv.Offset))
	})
}

func TestColVal_BitMapOperations(t *testing.T) {
	cv := &ColVal{}

	t.Run("set bitmap", func(t *testing.T) {
		cv.setBitMap(0)
		assert.Equal(t, byte(1), cv.Bitmap[0])

		cv.setBitMap(1)
		assert.Equal(t, byte(3), cv.Bitmap[0])

		cv.setBitMap(7)
		assert.Equal(t, byte(0x83), cv.Bitmap[0])
	})

	t.Run("reset bitmap", func(t *testing.T) {
		cv.Init()
		cv.Bitmap = []byte{0xFF}
		cv.resetBitMap(0)
		assert.Equal(t, byte(0xFE), cv.Bitmap[0])

		cv.resetBitMap(1)
		assert.Equal(t, byte(0xFC), cv.Bitmap[0])
	})

	t.Run("with bitmap offset", func(t *testing.T) {
		cv.Init()
		cv.BitMapOffset = 1
		cv.setBitMap(0)
		assert.Equal(t, byte(1), cv.Bitmap[0])
	})
}

func TestAppendNulls(t *testing.T) {
	cv := &ColVal{}

	appendNulls(cv, 3)
	assert.Equal(t, 3, cv.Len)
	assert.Equal(t, 3, cv.NilCount)
	assert.Equal(t, 1, len(cv.Bitmap))
	assert.Equal(t, byte(0), cv.Bitmap[0])
}

func TestAppendValues(t *testing.T) {
	t.Run("append integers", func(t *testing.T) {
		cv := &ColVal{}
		appendValues(cv, int64(123), int64(456))
		assert.Equal(t, 2, cv.Len)
		assert.Equal(t, 0, cv.NilCount)
		assert.Equal(t, []int64{123, 456}, cv.IntegerValues())
	})

	t.Run("append floats", func(t *testing.T) {
		cv := &ColVal{}
		appendValues(cv, float64(1.23), float64(4.56))
		assert.Equal(t, 2, cv.Len)
		assert.Equal(t, 0, cv.NilCount)
		assert.Equal(t, []float64{1.23, 4.56}, cv.FloatValues())
	})

	t.Run("append booleans", func(t *testing.T) {
		cv := &ColVal{}
		appendValues(cv, true, false)
		assert.Equal(t, 2, cv.Len)
		assert.Equal(t, 0, cv.NilCount)
		assert.Equal(t, []bool{true, false}, cv.BooleanValues())
	})
}

func TestColVal_StringValues(t *testing.T) {
	cv := &ColVal{}

	t.Run("empty column", func(t *testing.T) {
		values := cv.StringValues(nil)
		assert.Empty(t, values)
	})

	t.Run("with strings", func(t *testing.T) {
		cv.Val = []byte("hello世界")
		cv.Offset = []uint32{0, 5, 11}
		cv.Bitmap = []byte{0x03}
		cv.Len = 2

		values := cv.StringValues(nil)
		assert.Equal(t, []string{"hello", "世界"}, values)
	})

	t.Run("with nil values", func(t *testing.T) {
		cv.Init()
		cv.Val = []byte("test")
		cv.Offset = []uint32{0, 4}
		cv.Bitmap = []byte{0x01}
		cv.Len = 2
		cv.NilCount = 1

		values := cv.StringValues(nil)
		assert.Equal(t, []string{"test"}, values)
	})
}

func TestColVal_IsNil(t *testing.T) {
	cv := &ColVal{}

	t.Run("empty column", func(t *testing.T) {
		assert.True(t, cv.IsNil(0))
	})

	t.Run("no nil values", func(t *testing.T) {
		cv.Bitmap = []byte{0xFF}
		cv.Len = 8
		for i := 0; i < 8; i++ {
			assert.False(t, cv.IsNil(i))
		}
	})

	t.Run("with nil values", func(t *testing.T) {
		cv.Bitmap = []byte{0x0F}
		cv.Len = 8
		cv.NilCount = 4
		for i := 0; i < 4; i++ {
			assert.False(t, cv.IsNil(i))
		}
		for i := 4; i < 8; i++ {
			assert.True(t, cv.IsNil(i))
		}
	})
}

func TestColVal_Marshal(t *testing.T) {
	cv := &ColVal{
		Val:          []byte{1, 2, 3},
		Offset:       []uint32{0, 1, 2},
		Bitmap:       []byte{0xFF},
		BitMapOffset: 1,
		Len:          3,
		NilCount:     1,
	}

	buf := make([]byte, 0)
	result, err := cv.Marshal(buf)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify size calculation
	assert.Equal(t, cv.Size(), len(result))
}

func TestColVal_AppendString(t *testing.T) {
	src := &ColVal{
		Val:    []byte("hello世界"),
		Offset: []uint32{0, 5, 11},
		Bitmap: []byte{0x03},
		Len:    2,
	}

	t.Run("append partial", func(t *testing.T) {
		dst := &ColVal{}
		dst.appendString(src, 0, 1)
		assert.Equal(t, []byte("hello"), dst.Val)
		assert.Equal(t, []uint32{0}, dst.Offset)
	})

	t.Run("append all", func(t *testing.T) {
		dst := &ColVal{}
		dst.appendString(src, 0, 2)
		assert.Equal(t, []byte("hello世界"), dst.Val)
		assert.Equal(t, []uint32{0, 5}, dst.Offset)
	})

	t.Run("append with existing content", func(t *testing.T) {
		dst := &ColVal{
			Val:    []byte("test"),
			Offset: []uint32{0},
		}
		dst.appendString(src, 0, 1)
		assert.Equal(t, []byte("testhello"), dst.Val)
		assert.Equal(t, []uint32{0, 4}, dst.Offset)
	})
}

func TestColVal_AppendAll(t *testing.T) {
	src := &ColVal{
		Val:      []byte{1, 2, 3},
		Offset:   []uint32{0, 1, 2},
		Bitmap:   []byte{0x03},
		Len:      2,
		NilCount: 1,
	}

	dst := &ColVal{}
	dst.appendAll(src)

	assert.Equal(t, src.Val, dst.Val)
	assert.Equal(t, src.Offset, dst.Offset)
	assert.Equal(t, src.Bitmap, dst.Bitmap)
	assert.Equal(t, src.Len, dst.Len)
	assert.Equal(t, src.NilCount, dst.NilCount)
}
