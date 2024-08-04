package opengemini

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	PrecisionNanosecond Precision = iota
	PrecisionMicrosecond
	PrecisionMillisecond
	PrecisionSecond
	PrecisionMinute
	PrecisionHour
)

type Precision int

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
	}
	return ""
}

type Point struct {
	Measurement string
	// Precision Timestamp precision ,default value is  PrecisionNanosecond
	Precision Precision
	Time      time.Time
	Tags      map[string]string
	Fields    map[string]interface{}
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
		if !needEscape && c == '\\' && i < len(s)-1 {
			c1 := s[i+1]
			needEscape = c1 == '\\' || strings.IndexByte(charsToEscape, c1) != -1
		}

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
			if err = enc.writeString(v, `"`); err == nil {
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
		err = errors.New("unsupported field value type")
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

	if !p.Time.IsZero() {
		if err := enc.writeByte(' '); err != nil {
			return err
		}
		if _, err := io.WriteString(enc.w, formatTimestamp(p.Time, p.Precision)); err != nil {
			return err
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
