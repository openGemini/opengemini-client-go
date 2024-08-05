package opengemini

import (
	"strconv"
	"strings"
)

type TagKeysBuilder interface {
	// Measurement query the tag keys in a specified measurement, if measurement is not set,
	// all measurements in the database will be queried.
	Measurement(measurement string) TagKeysBuilder
	// Limit the number of entries returned by tag keys
	Limit(limit int) TagKeysBuilder
	// Offset skip the number of entries returned by tag keys
	Offset(offset int) TagKeysBuilder
	// Build generate query sql
	Build() string
}

type tagKeysBuilder struct {
	measurement string
	limit       int
	offset      int
}

func (t *tagKeysBuilder) Measurement(measurement string) TagKeysBuilder {
	t.measurement = measurement
	return t
}

func (t *tagKeysBuilder) Limit(limit int) TagKeysBuilder {
	t.limit = limit
	return t
}

func (t *tagKeysBuilder) Offset(offset int) TagKeysBuilder {
	t.offset = offset
	return t
}

func (t *tagKeysBuilder) Build() string {
	var buf strings.Builder
	buf.WriteString("SHOW TAG KEYS")
	if t.measurement != "" {
		buf.WriteString(" FROM " + t.measurement)
	}
	if t.limit > 0 {
		buf.WriteString(" LIMIT " + strconv.Itoa(t.limit))
	}
	if t.offset > 0 {
		buf.WriteString(" OFFSET " + strconv.Itoa(t.offset))
	}
	return buf.String()
}

func NewTagKeysBuilder() TagKeysBuilder {
	return &tagKeysBuilder{}
}
