package opengemini

import "errors"

var (
	ErrEmptyDatabaseName = errors.New("empty database name")
	ErrRetentionPolicy   = errors.New("empty retention policy")
	ErrEmptyMeasurement  = errors.New("empty measurement")
	ErrEmptyCommand      = errors.New("empty command")
)
