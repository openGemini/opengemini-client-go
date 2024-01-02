package opengemini

import (
	"fmt"
	"strings"
	"time"
)

const (
	PrecisionNanoSecond PrecisionType = iota
	PrecisionMicrosecond
	PrecisionMillisecond
	PrecisionSecond
	PrecisionMinute
	PrecisionHour
)

type PrecisionType int

func (p PrecisionType) String() string {
	switch p {
	case PrecisionNanoSecond:
		return "PrecisionNanoSecond"
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

type Point struct {
	Measurement string
	// Precision Timestamp precision ,default value is  PrecisionNanoSecond
	Precision PrecisionType
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

func (p *Point) SetTime(t time.Time) {
	p.Time = t
}

func (p *Point) SetPrecision(precision PrecisionType) {
	p.Precision = precision
}

func (p *Point) SetMeasurement(name string) {
	p.Measurement = name
}

func (p *Point) String() string {
	if len(p.Measurement) == 0 || p.Fields == nil {
		return ""
	}
	var builder strings.Builder
	builder.WriteString(p.Measurement)
	if p.Tags != nil {
		builder.WriteByte(',')
		builder.WriteString(parseTags(p.Tags))
	}
	builder.WriteByte(' ')
	builder.WriteString(parseFields(p.Fields))
	if !p.Time.IsZero() {
		builder.WriteByte(' ')
		builder.WriteString(parseTimestamp(p.Precision, p.Time))
	}
	return builder.String()
}

type BatchPoints struct {
	Points []*Point
}

func (bp *BatchPoints) AddPoint(p *Point) {
	bp.Points = append(bp.Points, p)
}

func parseTags(tags map[string]string) string {
	var builder strings.Builder

	first := true
	for k, v := range tags {
		if !first {
			builder.WriteByte(',')
		} else {
			first = false
		}
		builder.WriteString(k)
		builder.WriteByte('=')
		builder.WriteString(v)
	}

	return builder.String()
}

func parseFields(fields map[string]interface{}) string {
	var builder strings.Builder

	first := true
	for k, v := range fields {
		if !first {
			builder.WriteByte(',')
		} else {
			first = false
		}
		builder.WriteString(k)
		builder.WriteByte('=')
		switch v := v.(type) {
		case string:
			builder.WriteString("\"" + v + "\"")
		case int8, uint8, int16, uint16, int, uint, int32, uint32, int64, uint64:
			builder.WriteString(fmt.Sprintf("%d", v))
		case float32, float64:
			builder.WriteString(fmt.Sprintf("%f", v))
		case bool:
			if v {
				builder.WriteByte('T')
			} else {
				builder.WriteByte('F')
			}
		}

	}
	return builder.String()
}

func parseTimestamp(precisionType PrecisionType, ptime time.Time) string {
	switch precisionType {
	case PrecisionNanoSecond:
		return fmt.Sprintf("%d", ptime.UnixNano())
	case PrecisionMicrosecond:
		return fmt.Sprintf("%d", ptime.Round(time.Microsecond).UnixNano())
	case PrecisionMillisecond:
		return fmt.Sprintf("%d", ptime.Round(time.Millisecond).UnixNano())
	case PrecisionSecond:
		return fmt.Sprintf("%d", ptime.Round(time.Second).UnixNano())
	case PrecisionMinute:
		return fmt.Sprintf("%d", ptime.Round(time.Minute).UnixNano())
	case PrecisionHour:
		return fmt.Sprintf("%d", ptime.Round(time.Hour).UnixNano())
	}
	return ""
}
