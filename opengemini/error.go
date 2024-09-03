package opengemini

import "errors"

var (
	ErrEmptyDatabaseName = errors.New("empty database name")
	ErrRetentionPolicy   = errors.New("empty retention policy")
	ErrEmptyMeasurement  = errors.New("empty measurement")
	ErrEmptyCommand      = errors.New("empty command")
	ErrEmptyTagOrField   = errors.New("empty tag or field")
)

// checkDatabaseName checks if the database name is empty and returns an error if it is.
func checkDatabaseName(database string) error {
	if len(database) == 0 {
		return ErrEmptyDatabaseName
	}
	return nil
}

// checkMeasurementName checks if the measurement name is empty and returns an error if it is.
func checkMeasurementName(mst string) error {
	if len(mst) == 0 {
		return ErrEmptyMeasurement
	}
	return nil
}

func checkDatabaseAndPolicy(database, retentionPolicy string) error {
	if len(database) == 0 {
		return ErrEmptyDatabaseName
	}
	if len(retentionPolicy) == 0 {
		return ErrRetentionPolicy
	}
	return nil
}

// checkDatabaseAndCommand checks if the database name or command is empty and returns an appropriate error.
func checkDatabaseAndCommand(database, command string) error {
	if len(database) == 0 {
		return ErrEmptyDatabaseName
	}
	if len(command) == 0 {
		return ErrEmptyCommand
	}
	return nil
}
