package opengemini

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func encodePoint(p *Point) string {
	var buf bytes.Buffer
	enc := NewLineProtocolEncoder(&buf)
	_ = enc.Encode(p)
	return buf.String()
}

func TestWriteString(t *testing.T) {
	cases := []struct {
		s, charsToEscape, result string
	}{
		{s: "foo", charsToEscape: "", result: "foo"},
		{s: `f\\oo`, charsToEscape: "", result: `f\\\oo`},
		{s: `\fo\o\`, charsToEscape: "", result: `\fo\o\`},
		{s: `foo bar`, charsToEscape: " ", result: `foo\ bar`},
		{s: `foo\ bar`, charsToEscape: " ", result: `foo\\\ bar`},
		{s: `foo,\ bar`, charsToEscape: ", ", result: `foo\,\\\ bar`},
		{s: `foo,\  bar`, charsToEscape: ", ", result: `foo\,\\\ \ bar`},
		{s: `foo=,\  ba\r`, charsToEscape: ",= ", result: `foo\=\,\\\ \ ba\r`},
	}

	for _, c := range cases {
		var buf bytes.Buffer
		enc := NewLineProtocolEncoder(&buf)
		_ = enc.writeString(c.s, c.charsToEscape)
		if buf.String() != c.result {
			t.Errorf("unexpected result: got %s, want %s", buf.String(), c.result)
		}
	}
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
	point.Time = time.Date(2023, 12, 1, 12, 32, 18, 132363612, time.UTC)
	if strings.Compare(encodePoint(point),
		"measurement,tag=tag1 filed1=\"string field\" 1701433938132363612") != 0 {
		t.Error("parse point with a tag failed")
	}
}

func TestFormatTimestamp(t *testing.T) {
	testTime := time.Date(2023, 12, 1, 12, 32, 18, 132363612, time.UTC)
	tests := []struct {
		precision Precision
		timestamp string
	}{
		{
			precision: PrecisionNanosecond,
			timestamp: "1701433938132363612",
		}, {
			precision: PrecisionMicrosecond,
			timestamp: "1701433938132364000",
		}, {
			precision: PrecisionMillisecond,
			timestamp: "1701433938132000000",
		}, {
			precision: PrecisionSecond,
			timestamp: "1701433938000000000",
		}, {
			precision: PrecisionMinute,
			timestamp: "1701433920000000000",
		}, {
			precision: PrecisionHour,
			timestamp: "1701435600000000000",
		},
	}
	for _, tt := range tests {
		if strings.Compare(formatTimestamp(testTime, tt.precision), tt.timestamp) != 0 {
			t.Errorf("parse timestamp error in %v", tt.precision.String())
		}
	}
}
