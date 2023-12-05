package opengemini

import (
	"strings"
	"testing"
	"time"
)

func TestPoint_String(t *testing.T) {
	point := &Point{}
	// parse Point which hasn't set measurement
	if strings.Compare(point.String(), "") != 0 {
		t.Error("error translate for point hasn't set measurement")
	}
	point.SetMeasurement("measurement")
	// parse Point which hasn't own field
	if strings.Compare(point.String(), "") != 0 {
		t.Error("error translate for point hasn't own field")
	}
	point.AddField("filed1", "string field")
	// parse Point which only has a field
	if strings.Compare(point.String(),
		"measurement filed1=\"string field\"") != 0 {
		t.Error("parse point with a string filed failed")
	}
	point.AddTag("tag", "tag1")
	// parse Point which has a field with a tag
	if strings.Compare(point.String(),
		"measurement,tag=tag1 filed1=\"string field\"") != 0 {
		t.Error("parse point with a tag failed")
	}
	point.SetTime(time.Date(2023, 12, 1, 12, 32, 18, 132363612, time.UTC))
	if strings.Compare(point.String(),
		"measurement,tag=tag1 filed1=\"string field\" 1701433938132363612") != 0 {
		t.Error("parse point with a tag failed")
	}
}

func TestParseTimestamp(t *testing.T) {
	testTime := time.Date(2023, 12, 1, 12, 32, 18, 132363612, time.UTC)
	tests := []struct {
		precision PrecisionType
		timestamp string
	}{
		{
			precision: PrecisionNanoSecond,
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
		if strings.Compare(parseTimestamp(tt.precision, testTime), tt.timestamp) != 0 {
			t.Errorf("parse timestamp error in %v", tt.precision.String())
		}
	}
}
