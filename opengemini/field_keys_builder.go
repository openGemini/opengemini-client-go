package opengemini

import "strings"

type FieldKeysBuilder interface {
	// Measurement query the field keys in a specified measurement, if measurement is not set,
	// all measurements in the database will be queried.
	Measurement(string) FieldKeysBuilder
	Build() string
}

type fieldKeysBuilder struct {
	measurement string
}

func (f *fieldKeysBuilder) Measurement(measurement string) FieldKeysBuilder {
	f.measurement = measurement
	return f
}

func (f *fieldKeysBuilder) Build() string {
	var buf strings.Builder
	buf.WriteString("SHOW FIELD KEYS")
	if f.measurement != "" {
		buf.WriteString(" FROM " + f.measurement)
	}
	return buf.String()
}

func NewFieldKeysBuilder() FieldKeysBuilder {
	return &fieldKeysBuilder{}
}
