package opengemini

import "errors"

var (
	ErrEmptyDatabaseName = errors.New("empty database name")
	ErrEmptyCommand      = errors.New("empty command")
	ErrRetentionPolicy   = errors.New("empty retention policy")
)
