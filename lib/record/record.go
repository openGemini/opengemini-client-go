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
	"fmt"
	"sort"
	"strings"
)

const (
	TimeField = "time"
)

type Record struct {
	ColVals []ColVal
	Schema  Schemas
}

func NewRecordBuilder(schema []Field) *Record {
	return &Record{
		Schema:  schema,
		ColVals: make([]ColVal, len(schema)),
	}
}

func (rec *Record) Len() int {
	return len(rec.Schema)
}

func (rec *Record) Swap(i, j int) {
	rec.Schema[i], rec.Schema[j] = rec.Schema[j], rec.Schema[i]
	rec.ColVals[i], rec.ColVals[j] = rec.ColVals[j], rec.ColVals[i]
}

func (rec *Record) Less(i, j int) bool {
	if rec.Schema[i].Name == TimeField {
		return false
	} else if rec.Schema[j].Name == TimeField {
		return true
	} else {
		return rec.Schema[i].Name < rec.Schema[j].Name
	}
}

func (rec *Record) Column(i int) *ColVal {
	return &rec.ColVals[i]
}

func (rec *Record) String() string {
	var sb strings.Builder

	for i, f := range rec.Schema {
		var line string
		switch f.Type {
		case FieldTypeFloat:
			line = fmt.Sprintf("field(%v):%#v\n", f.Name, rec.Column(i).FloatValues())
		case FieldTypeString, FieldTypeTag:
			line = fmt.Sprintf("field(%v):%#v\n", f.Name, rec.Column(i).StringValues(nil))
		case FieldTypeBoolean:
			line = fmt.Sprintf("field(%v):%#v\n", f.Name, rec.Column(i).BooleanValues())
		case FieldTypeInt:
			line = fmt.Sprintf("field(%v):%#v\n", f.Name, rec.Column(i).IntegerValues())
		}
		sb.WriteString(line)
	}

	return sb.String()
}

func CheckRecord(rec *Record) error {
	colN := len(rec.Schema)
	if colN <= 1 || rec.Schema[colN-1].Name != TimeField {
		return fmt.Errorf("invalid schema: %v", rec.Schema)
	}

	if rec.ColVals[colN-1].NilCount != 0 {
		return fmt.Errorf("invalid colvals: %v", rec.String())
	}

	for i := 1; i < colN; i++ {
		if rec.Schema[i].Name == rec.Schema[i-1].Name {
			return fmt.Errorf("same schema; idx: %d, name: %v", i, rec.Schema[i].Name)
		}
	}
	isOrderSchema := true
	for i := 0; i < colN-1; i++ {
		f := &rec.Schema[i]
		col1, col2 := &rec.ColVals[i], &rec.ColVals[i+1]

		if col1.Len != col2.Len {
			return fmt.Errorf("invalid colvals length: %v", rec.String())
		}
		isOrderSchema = CheckSchema(i, rec, isOrderSchema)

		// check string data length
		if f.Type == FieldTypeString || f.Type == FieldTypeTag {
			continue
		}

		// check data length
		expLen := typeSize[f.Type] * (col1.Len - col1.NilCount)
		if expLen != len(col1.Val) {
			return fmt.Errorf("the length of rec.ColVals[%d].val is incorrect. exp: %d, got: %d\n%s",
				i, expLen, len(col1.Val), rec.String())
		}
	}
	if !isOrderSchema {
		sort.Sort(rec)
	}

	return nil
}

func CheckSchema(i int, rec *Record, isOrderSchema bool) bool {
	if isOrderSchema && i > 0 && rec.Schema[i-1].Name >= rec.Schema[i].Name {
		fmt.Printf("record schema is invalid; idx i-1: %d, name: %v, idx i: %d, name: %v\n",
			i-1, rec.Schema[i-1].Name, i, rec.Schema[i].Name)
		return false
	}
	return isOrderSchema
}

func (rec *Record) Reset() {
	rec.Schema = rec.Schema[:0]
	rec.ColVals = rec.ColVals[:0]
}

func (rec *Record) ReserveColVal(size int) {
	// resize col val
	colLen := len(rec.ColVals)
	colCap := cap(rec.ColVals)
	remain := colCap - colLen
	if delta := size - remain; delta > 0 {
		rec.ColVals = append(rec.ColVals[:colCap], make([]ColVal, delta)...)
	}
	rec.ColVals = rec.ColVals[:colLen+size]
	rec.InitColVal(colLen, colLen+size)
}

func (rec *Record) InitColVal(start, end int) {
	for i := start; i < end; i++ {
		cv := &rec.ColVals[i]
		cv.Init()
	}
}

func (rec *Record) RowNums() int {
	if rec == nil || len(rec.ColVals) == 0 {
		return 0
	}

	return rec.ColVals[len(rec.ColVals)-1].Len
}

func (rec *Record) Times() []int64 {
	if len(rec.ColVals) == 0 {
		return nil
	}
	cv := rec.ColVals[len(rec.ColVals)-1]
	return cv.IntegerValues()
}

func (rec *Record) AppendTime(time ...int64) {
	for _, t := range time {
		rec.ColVals[len(rec.ColVals)-1].AppendInteger(t)
	}
}

func (rec *Record) Marshal(buf []byte) ([]byte, error) {
	var err error
	// Schema
	buf = AppendUint32(buf, uint32(len(rec.Schema)))
	for i := 0; i < len(rec.Schema); i++ {
		buf = AppendUint32(buf, uint32(rec.Schema[i].Size()))
		buf, err = rec.Schema[i].Marshal(buf)
		if err != nil {
			return nil, err
		}
	}

	// ColVal
	buf = AppendUint32(buf, uint32(len(rec.ColVals)))
	for i := 0; i < len(rec.ColVals); i++ {
		buf = AppendUint32(buf, uint32(rec.ColVals[i].Size()))
		buf, err = rec.ColVals[i].Marshal(buf)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}
