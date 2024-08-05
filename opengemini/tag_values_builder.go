package opengemini

import (
	"strconv"
	"strings"
)

type TagValuesBuilder interface {
	// Measurement query the tag values in a specified measurement, this option must be set
	Measurement(measurement string) TagValuesBuilder
	// Limit the number of entries returned by tag values, default no limit
	Limit(limit int) TagValuesBuilder
	// Offset skip the number of entries than returned, default no offset
	Offset(offset int) TagValuesBuilder
	// Key query the tag values in a specified tag key, this option must be set
	Key(key string) TagValuesBuilder
	// Build generate query sql
	Build() string
}

type tagValuesBuilder struct {
	measurement string
	limit       int
	offset      int
	key         string
}

func (t *tagValuesBuilder) Measurement(measurement string) TagValuesBuilder {
	t.measurement = measurement
	return t
}

func (t *tagValuesBuilder) Limit(limit int) TagValuesBuilder {
	t.limit = limit
	return t
}

func (t *tagValuesBuilder) Offset(offset int) TagValuesBuilder {
	t.offset = offset
	return t
}

func (t *tagValuesBuilder) Key(key string) TagValuesBuilder {
	t.key = key
	return t
}

func (t *tagValuesBuilder) Build() string {
	var buf strings.Builder
	buf.WriteString("SHOW TAG VALUES")
	if t.measurement != "" {
		buf.WriteString(" FROM " + t.measurement)
	}
	if t.key != "" {
		buf.WriteString(" WITH KEY = \"" + t.key + "\"")
	}
	if t.limit > 0 {
		buf.WriteString(" LIMIT " + strconv.Itoa(t.limit))
	}
	if t.offset > 0 {
		buf.WriteString(" OFFSET " + strconv.Itoa(t.offset))
	}
	return buf.String()
}

func NewTagValuesBuilder() TagValuesBuilder {
	return &tagValuesBuilder{}
}
