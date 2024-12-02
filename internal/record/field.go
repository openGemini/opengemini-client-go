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
	"strings"
)

const (
	FieldTypeUnknown = 0
	FieldTypeInt     = 1
	FieldTypeUInt    = 2
	FieldTypeFloat   = 3
	FieldTypeString  = 4
	FieldTypeBoolean = 5
	FieldTypeTag     = 6
	FieldTypeLast    = 7
)

var FieldTypeName = map[int]string{
	FieldTypeUnknown: "Unknown",
	FieldTypeInt:     "Integer",
	FieldTypeUInt:    "Unsigned",
	FieldTypeFloat:   "Float",
	FieldTypeString:  "String",
	FieldTypeBoolean: "Boolean",
	FieldTypeTag:     "Tag",
	FieldTypeLast:    "Unknown",
}

type Field struct {
	Type int
	Name string
}

func (f *Field) String() string {
	var sb strings.Builder
	sb.WriteString(f.Name)
	sb.WriteString(FieldTypeName[f.Type])
	return sb.String()
}

type Schemas []Field

func (sh *Schemas) String() string {
	sb := strings.Builder{}
	for _, f := range *sh {
		sb.WriteString(f.String() + "\n")
	}
	return sb.String()
}

func (f *Field) Marshal(buf []byte) ([]byte, error) {
	buf = AppendString(buf, f.Name)
	buf = AppendInt(buf, f.Type)
	return buf, nil
}

func (f *Field) Size() int {
	size := 0
	size += SizeOfString(f.Name)
	size += SizeOfInt()
	return size
}
