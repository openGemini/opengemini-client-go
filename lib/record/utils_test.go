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

func TestBytes2str(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "empty bytes",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "normal string",
			input:    []byte("hello world"),
			expected: "hello world",
		},
		{
			name:     "string with special chars",
			input:    []byte("hello\nworld\t!"),
			expected: "hello\nworld\t!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Bytes2str(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAppendString(t *testing.T) {
	tests := []struct {
		name     string
		initial  []byte
		str      string
		expected []byte
	}{
		{
			name:     "empty string to empty slice",
			initial:  []byte{},
			str:      "",
			expected: []byte{0, 0}, // length 0 as uint16 + empty string
		},
		{
			name:     "append hello",
			initial:  []byte{1, 2, 3},
			str:      "hello",
			expected: []byte{1, 2, 3, 0, 5, 'h', 'e', 'l', 'l', 'o'},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AppendString(tt.initial, tt.str)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestUint32Slice2byte(t *testing.T) {
	tests := []struct {
		name     string
		input    []uint32
		expected int // expected length of resulting byte slice
	}{
		{
			name:     "empty slice",
			input:    []uint32{},
			expected: 0,
		},
		{
			name:     "single uint32",
			input:    []uint32{123},
			expected: 4, // 4 bytes per uint32
		},
		{
			name:     "multiple uint32s",
			input:    []uint32{123, 456, 789},
			expected: 12, // 3 * 4 bytes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Uint32Slice2byte(tt.input)
			if tt.expected == 0 {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, tt.expected, len(result))
			}
		})
	}
}

func TestAppendUint32Slice(t *testing.T) {
	tests := []struct {
		name     string
		initial  []byte
		slice    []uint32
		expected int // expected length increase
	}{
		{
			name:     "empty slice",
			initial:  []byte{1, 2, 3},
			slice:    []uint32{},
			expected: 4, // just the length field (uint32)
		},
		{
			name:     "non-empty slice",
			initial:  []byte{1, 2, 3},
			slice:    []uint32{123, 456},
			expected: 12, // 4 (length) + 8 (2 * 4 bytes)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initialLen := len(tt.initial)
			result := AppendUint32Slice(tt.initial, tt.slice)
			assert.Equal(t, initialLen+tt.expected, len(result))
		})
	}
}

func TestSizeCalculations(t *testing.T) {
	t.Run("SizeOfString", func(t *testing.T) {
		str := "hello"
		size := SizeOfString(str)
		assert.Equal(t, len(str)+sizeOfUint16, size)
	})

	t.Run("SizeOfUint32", func(t *testing.T) {
		assert.Equal(t, 4, SizeOfUint32())
	})

	t.Run("SizeOfUint32Slice", func(t *testing.T) {
		slice := []uint32{1, 2, 3}
		size := SizeOfUint32Slice(slice)
		assert.Equal(t, len(slice)*4+MaxSliceSize, size)
	})

	t.Run("SizeOfByteSlice", func(t *testing.T) {
		slice := []byte{1, 2, 3}
		size := SizeOfByteSlice(slice)
		assert.Equal(t, len(slice)+SizeOfUint32(), size)
	})
}

func TestAppendIntegers(t *testing.T) {
	t.Run("AppendUint16", func(t *testing.T) {
		initial := []byte{1, 2, 3}
		result := AppendUint16(initial, 258) // 258 = 0x0102
		expected := []byte{1, 2, 3, 1, 2}
		assert.Equal(t, expected, result)
	})

	t.Run("AppendUint32", func(t *testing.T) {
		initial := []byte{1, 2, 3}
		result := AppendUint32(initial, 16909060) // 16909060 = 0x01020304
		expected := []byte{1, 2, 3, 1, 2, 3, 4}
		assert.Equal(t, expected, result)
	})

	t.Run("AppendInt64", func(t *testing.T) {
		initial := []byte{1, 2, 3}
		result := AppendInt64(initial, 123)
		assert.Equal(t, len(initial)+8, len(result))
	})

	t.Run("AppendInt", func(t *testing.T) {
		initial := []byte{1, 2, 3}
		result := AppendInt(initial, 123)
		assert.Equal(t, len(initial)+8, len(result))
	})
}

func TestAppendBytes(t *testing.T) {
	tests := []struct {
		name     string
		initial  []byte
		bytes    []byte
		expected int // expected length increase
	}{
		{
			name:     "empty bytes",
			initial:  []byte{1, 2, 3},
			bytes:    []byte{},
			expected: 4, // just the length field
		},
		{
			name:     "non-empty bytes",
			initial:  []byte{1, 2, 3},
			bytes:    []byte{4, 5, 6},
			expected: 7, // 4 (length) + 3 (data)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initialLen := len(tt.initial)
			result := AppendBytes(tt.initial, tt.bytes)
			assert.Equal(t, initialLen+tt.expected, len(result))
		})
	}
}
