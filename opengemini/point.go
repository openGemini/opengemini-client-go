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
	"io"
	"strconv"
	"strings"
	"time"
)

type Precision int

const (
	PrecisionNanosecond Precision = iota
	PrecisionMicrosecond
	PrecisionMillisecond
	PrecisionSecond
	PrecisionMinute
	PrecisionHour
	PrecisionRFC3339
)

func (p Precision) String() string {
	switch p {
	case PrecisionNanosecond:
		return "PrecisionNanosecond"
	case PrecisionMicrosecond:
		return "PrecisionMicrosecond"
	case PrecisionMillisecond:
		return "PrecisionMillisecond"
	case PrecisionSecond:
		return "PrecisionSecond"
	case PrecisionMinute:
		return "PrecisionMinute"
	case PrecisionHour:
		return "PrecisionHour"
	case PrecisionRFC3339:
		return "PrecisionRFC3339"
	}
	return ""
}

func (p Precision) Epoch() string {
	switch p {
	case PrecisionNanosecond:
		return "ns"
	case PrecisionMicrosecond:
		return "u"
	case PrecisionMillisecond:
		return "ms"
	case PrecisionSecond:
		return "s"
	case PrecisionMinute:
		return "m"
	case PrecisionHour:
		return "h"
	case PrecisionRFC3339:
		return "rfc3339"
	}
	return ""
}

func ToPrecision(epoch string) Precision {
	switch epoch {
	case "ns":
		return PrecisionNanosecond
	case "u":
		return PrecisionMicrosecond
	case "ms":
		return PrecisionMillisecond
	case "s":
		return PrecisionSecond
	case "m":
		return PrecisionMinute
	case "h":
		return PrecisionHour
	case "rfc3339":
		return PrecisionRFC3339
	}
	return PrecisionNanosecond
}

// Point represents a single point in the line protocol format.
// A Point is composed of a measurement name, zero or more tags, one or more fields, and a timestamp.
type Point struct {
	// Measurement is the line protocol measurement name definition.
	Measurement string
	// Precision Timestamp precision, default value is PrecisionNanosecond
	Precision Precision
	// Time is the line protocol time field definition.
	// Deprecated: Use Timestamp instead. Will be removed in 0.10.0.
	Time time.Time
	// Timestamp Point creation timestamp, default value is Now() in nanoseconds.
	// If p.Time is not zero, Timestamp will be set to p.Time.UnixNano() / int64(p.Precision).
	// If Timestamp is zero, Timestamp will be set to current time.
	Timestamp int64
	// Tags is the line protocol tag field definition.
	Tags map[string]string
	// Fields is the line protocol value field definition.
	Fields map[string]interface{}
}

func (p *Point) AddTag(key string, value string) {
	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}
	p.Tags[key] = value
}

func (p *Point) AddField(key string, value interface{}) {
	if p.Fields == nil {
		p.Fields = make(map[string]interface{})
	}
	p.Fields[key] = value
}

type LineProtocolEncoder struct {
	w io.Writer
}

func NewLineProtocolEncoder(w io.Writer) *LineProtocolEncoder {
	return &LineProtocolEncoder{w: w}
}

func (enc *LineProtocolEncoder) writeByte(b byte) error {
	_, err := enc.w.Write([]byte{b})
	return err
}

// writeString writes a string to the underlying writer, escaping characters
// with a backslash if necessary. Note that, for simplify, `charsToEscape` can
// only contain ASCII characters.
func (enc *LineProtocolEncoder) writeString(s string, charsToEscape string) error {
	for i := 0; i < len(s); i++ {
		c := s[i]

		needEscape := strings.IndexByte(charsToEscape, c) != -1

		if needEscape {
			if err := enc.writeByte('\\'); err != nil {
				return err
			}
		}

		if err := enc.writeByte(c); err != nil {
			return err
		}
	}

	return nil
}

func (enc *LineProtocolEncoder) writeFieldValue(v interface{}) error {
	var err error

	switch v := v.(type) {
	case string:
		if err = enc.writeByte('"'); err == nil {
			if err = enc.writeString(v, `"\`); err == nil {
				err = enc.writeByte('"')
			}
		}
	case int8:
		if _, err = io.WriteString(enc.w, strconv.FormatInt(int64(v), 10)); err == nil {
			err = enc.writeByte('i')
		}
	case uint8:
		if _, err = io.WriteString(enc.w, strconv.FormatUint(uint64(v), 10)); err == nil {
			err = enc.writeByte('u')
		}
	case int16:
		if _, err = io.WriteString(enc.w, strconv.FormatInt(int64(v), 10)); err == nil {
			err = enc.writeByte('i')
		}
	case uint16:
		if _, err = io.WriteString(enc.w, strconv.FormatUint(uint64(v), 10)); err == nil {
			err = enc.writeByte('u')
		}
	case int32:
		if _, err = io.WriteString(enc.w, strconv.FormatInt(int64(v), 10)); err == nil {
			err = enc.writeByte('i')
		}
	case uint32:
		if _, err = io.WriteString(enc.w, strconv.FormatUint(uint64(v), 10)); err == nil {
			err = enc.writeByte('u')
		}
	case int:
		if _, err = io.WriteString(enc.w, strconv.FormatInt(int64(v), 10)); err == nil {
			err = enc.writeByte('i')
		}
	case uint:
		if _, err = io.WriteString(enc.w, strconv.FormatUint(uint64(v), 10)); err == nil {
			err = enc.writeByte('u')
		}
	case int64:
		if _, err = io.WriteString(enc.w, strconv.FormatInt(v, 10)); err == nil {
			err = enc.writeByte('i')
		}
	case uint64:
		if _, err = io.WriteString(enc.w, strconv.FormatUint(v, 10)); err == nil {
			err = enc.writeByte('u')
		}
	case float32:
		_, err = io.WriteString(enc.w, strconv.FormatFloat(float64(v), 'f', -1, 32))
	case float64:
		_, err = io.WriteString(enc.w, strconv.FormatFloat(v, 'f', -1, 64))
	case bool:
		if v {
			err = enc.writeByte('T')
		} else {
			err = enc.writeByte('F')
		}
	default:
		err = ErrUnsupportedFieldValueType
	}

	return err
}

func (enc *LineProtocolEncoder) Encode(p *Point) error {
	if len(p.Measurement) == 0 || p.Fields == nil {
		return nil
	}

	if err := enc.writeString(p.Measurement, `, `); err != nil {
		return err
	}

	for k, v := range p.Tags {
		if err := enc.writeByte(','); err != nil {
			return err
		}
		if err := enc.writeString(k, `, =`); err != nil {
			return err
		}
		if err := enc.writeByte('='); err != nil {
			return err
		}
		if err := enc.writeString(v, `, =`); err != nil {
			return err
		}
	}

	sep := byte(' ')
	for k, v := range p.Fields {
		if err := enc.writeByte(sep); err != nil {
			return err
		}
		sep = ','

		if err := enc.writeString(k, `, =`); err != nil {
			return err
		}
		if err := enc.writeByte('='); err != nil {
			return err
		}
		if err := enc.writeFieldValue(v); err != nil {
			return err
		}
	}

	if p.Timestamp != 0 || !p.Time.IsZero() {
		if err := enc.writeByte(' '); err != nil {
			return err
		}
		if p.Timestamp != 0 {
			if _, err := io.WriteString(enc.w, strconv.FormatInt(p.Timestamp, 10)); err != nil {
				return err
			}
		} else if !p.Time.IsZero() {
			if _, err := io.WriteString(enc.w, formatTimestamp(p.Time, p.Precision)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (enc *LineProtocolEncoder) BatchEncode(bp []*Point) error {
	for _, p := range bp {
		if p == nil {
			continue
		}
		if err := enc.Encode(p); err != nil {
			return err
		}
		if err := enc.writeByte('\n'); err != nil {
			return err
		}
	}
	return nil
}

func formatTimestamp(t time.Time, p Precision) string {
	switch p {
	case PrecisionNanosecond:
		return strconv.FormatInt(t.UnixNano(), 10)
	case PrecisionMicrosecond:
		return strconv.FormatInt(t.Round(time.Microsecond).UnixNano(), 10)
	case PrecisionMillisecond:
		return strconv.FormatInt(t.Round(time.Millisecond).UnixNano(), 10)
	case PrecisionSecond:
		return strconv.FormatInt(t.Round(time.Second).UnixNano(), 10)
	case PrecisionMinute:
		return strconv.FormatInt(t.Round(time.Minute).UnixNano(), 10)
	case PrecisionHour:
		return strconv.FormatInt(t.Round(time.Hour).UnixNano(), 10)
	}
	return ""
}
