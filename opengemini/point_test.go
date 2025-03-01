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
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPointToString(t *testing.T) {
	// line protocol without escaped chars
	assert.Equal(t, "test,T0=0 a=1i", encodePoint(assemblePoint("test", "T0", "0", "a", 1)))

	// line protocol measurement with escaped chars
	assert.Equal(t, "test\\,,T0=0 a=1i", encodePoint(assemblePoint("test,", "T0", "0", "a", 1)))
	assert.Equal(t, "test\\ ,T0=0 a=1i", encodePoint(assemblePoint("test ", "T0", "0", "a", 1)))

	// line protocol tag key with escaped chars
	assert.Equal(t, "test,T0\\,=0 a=1i", encodePoint(assemblePoint("test", "T0,", "0", "a", 1)))
	assert.Equal(t, "test,T0\\==0 a=1i", encodePoint(assemblePoint("test", "T0=", "0", "a", 1)))
	assert.Equal(t, "test,T0\\ =0 a=1i", encodePoint(assemblePoint("test", "T0 ", "0", "a", 1)))

	// line protocol tag value with escaped chars
	assert.Equal(t, "test,T0=0\\, a=1i", encodePoint(assemblePoint("test", "T0", "0,", "a", 1)))
	assert.Equal(t, "test,T0=0\\= a=1i", encodePoint(assemblePoint("test", "T0", "0=", "a", 1)))
	assert.Equal(t, "test,T0=0\\  a=1i", encodePoint(assemblePoint("test", "T0", "0 ", "a", 1)))

	// line protocol field key with escaped chars
	assert.Equal(t, "test,T0=0 a\\,=1i", encodePoint(assemblePoint("test", "T0", "0", "a,", 1)))
	assert.Equal(t, "test,T0=0 a\\==1i", encodePoint(assemblePoint("test", "T0", "0", "a=", 1)))
	assert.Equal(t, "test,T0=0 a\\ =1i", encodePoint(assemblePoint("test", "T0", "0", "a ", 1)))

	// line protocol field value with escaped chars
	assert.Equal(t, "test,T0=0 a=\"1\\\"\"", encodePoint(assemblePoint("test", "T0", "0", "a", "1\"")))
	assert.Equal(t, "test,T0=0 a=\"1\\\\\"", encodePoint(assemblePoint("test", "T0", "0", "a", "1\\")))
	assert.Equal(t, "test,T0=0 a=\"1\\\\\\\\\"", encodePoint(assemblePoint("test", "T0", "0", "a", "1\\\\")))
	assert.Equal(t, "test,T0=0 a=\"1\\\\\\\\\\\\\"", encodePoint(assemblePoint("test", "T0", "0", "a", "1\\\\\\")))

}

func assemblePoint(measurement, tagKey, tagValue, fieldKey string, filedValue interface{}) *Point {
	point := &Point{Measurement: measurement}
	point.AddTag(tagKey, tagValue)
	point.AddField(fieldKey, filedValue)
	return point
}

func encodePoint(p *Point) string {
	var buf bytes.Buffer
	enc := NewLineProtocolEncoder(&buf)
	_ = enc.Encode(p)
	return buf.String()
}

func TestPointEncode(t *testing.T) {
	point := &Point{}
	// encode Point which hasn't set measurement
	if strings.Compare(encodePoint(point), "") != 0 {
		t.Error("error translate for point hasn't set measurement")
	}
	point.Measurement = "measurement"
	// encode Point which hasn't own field
	if strings.Compare(encodePoint(point), "") != 0 {
		t.Error("error translate for point hasn't own field")
	}
	point.AddField("filed1", "string field")
	// encode Point which only has a field
	if strings.Compare(encodePoint(point),
		"measurement filed1=\"string field\"") != 0 {
		t.Error("parse point with a string filed failed")
	}
	point.AddTag("tag", "tag1")
	// encode Point which has a field with a tag
	if strings.Compare(encodePoint(point),
		"measurement,tag=tag1 filed1=\"string field\"") != 0 {
		t.Error("parse point with a tag failed")
	}
	point.Timestamp = time.Date(2023, 12, 1, 12, 32, 18, 132363612, time.UTC).UnixNano()
	if strings.Compare(encodePoint(point),
		"measurement,tag=tag1 filed1=\"string field\" 1701433938132363612") != 0 {
		t.Error("parse point with a tag failed")
	}
}
