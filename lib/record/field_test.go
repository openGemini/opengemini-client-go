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

func TestFieldTypeName(t *testing.T) {
	tests := []struct {
		fieldType int
		expected  string
	}{
		{FieldTypeUnknown, "Unknown"},
		{FieldTypeInt, "Integer"},
		{FieldTypeUInt, "Unsigned"},
		{FieldTypeFloat, "Float"},
		{FieldTypeString, "String"},
		{FieldTypeBoolean, "Boolean"},
		{FieldTypeTag, "Tag"},
		{FieldTypeLast, "Unknown"},
		{999, "Unknown"}, // Invalid type should return "Unknown"
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			name, exists := FieldTypeName[tt.fieldType]
			if tt.fieldType == 999 {
				assert.Empty(t, name)
				assert.False(t, exists)
			} else {
				assert.Equal(t, tt.expected, name)
				assert.True(t, exists)
			}
		})
	}
}

func TestField_String(t *testing.T) {
	tests := []struct {
		name     string
		field    Field
		expected string
	}{
		{
			name:     "integer field",
			field:    Field{Type: FieldTypeInt, Name: "test_int"},
			expected: "test_intInteger",
		},
		{
			name:     "string field",
			field:    Field{Type: FieldTypeString, Name: "test_str"},
			expected: "test_strString",
		},
		{
			name:     "unknown type field",
			field:    Field{Type: 999, Name: "test_unknown"},
			expected: "test_unknown",
		},
		{
			name:     "empty name field",
			field:    Field{Type: FieldTypeFloat, Name: ""},
			expected: "Float",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.field.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSchemas_String(t *testing.T) {
	tests := []struct {
		name     string
		schemas  Schemas
		expected string
	}{
		{
			name: "multiple fields",
			schemas: Schemas{
				{Type: FieldTypeInt, Name: "field1"},
				{Type: FieldTypeString, Name: "field2"},
			},
			expected: "field1Integer\nfield2String\n",
		},
		{
			name:     "empty schemas",
			schemas:  Schemas{},
			expected: "",
		},
		{
			name: "single field",
			schemas: Schemas{
				{Type: FieldTypeFloat, Name: "field1"},
			},
			expected: "field1Float\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.schemas.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestField_Marshal(t *testing.T) {
	tests := []struct {
		name      string
		field     Field
		initBuf   []byte
		expectLen int
	}{
		{
			name:      "empty field",
			field:     Field{Type: FieldTypeInt, Name: ""},
			initBuf:   []byte{},
			expectLen: 2 + SizeOfInt(), // 2 bytes for empty string length + int size
		},
		{
			name:      "normal field",
			field:     Field{Type: FieldTypeString, Name: "test"},
			initBuf:   []byte{1, 2, 3},
			expectLen: 3 + 2 + 4 + SizeOfInt(), // initial 3 bytes + 2 bytes for string length + 4 bytes for "test" + int size
		},
		{
			name:      "with initial buffer",
			field:     Field{Type: FieldTypeFloat, Name: "abc"},
			initBuf:   []byte{9, 9, 9},
			expectLen: 3 + 2 + 3 + SizeOfInt(), // initial 3 bytes + 2 bytes for string length + 3 bytes for "abc" + int size
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.field.Marshal(tt.initBuf)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectLen, len(result))

			// Verify the initial buffer is preserved
			if len(tt.initBuf) > 0 {
				assert.Equal(t, tt.initBuf, result[:len(tt.initBuf)])
			}
		})
	}
}

func TestField_Size(t *testing.T) {
	tests := []struct {
		name     string
		field    Field
		expected int
	}{
		{
			name:     "empty name",
			field:    Field{Type: FieldTypeInt, Name: ""},
			expected: SizeOfString("") + SizeOfInt(),
		},
		{
			name:     "normal field",
			field:    Field{Type: FieldTypeString, Name: "test"},
			expected: SizeOfString("test") + SizeOfInt(),
		},
		{
			name:     "long name",
			field:    Field{Type: FieldTypeFloat, Name: "very_long_field_name"},
			expected: SizeOfString("very_long_field_name") + SizeOfInt(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := tt.field.Size()
			assert.Equal(t, tt.expected, size)
		})
	}
}
