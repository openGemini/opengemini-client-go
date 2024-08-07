package opengemini

import "errors"

var (
	ErrEmptyDatabaseName = errors.New("empty database name")
	ErrRetentionPolicy   = errors.New("empty retention policy")
	ErrEmptyMeasurement  = errors.New("empty measurement")
	ErrEmptyCommand      = errors.New("empty command")
)

// CheckDatabaseName checks if the database name is empty and returns an error if it is.
func CheckDatabaseName(database string) error {
	if len(database) == 0 {
		return ErrEmptyDatabaseName
	}
	return nil
}

func CheckDatabaseAndPolicy(database, retentionPolicy string) error {
	if len(database) == 0 {
		return ErrEmptyDatabaseName
	}
	if len(retentionPolicy) == 0 {
		return ErrRetentionPolicy
	}
	return nil
}

// CheckDatabaseAndPolicyAndMeasurement checks if the database name, retention policy, or measurement is empty and returns the appropriate error.
func CheckDatabaseAndPolicyAndMeasurement(database, retentionPolicy, measurement string) error {
	if len(database) == 0 {
		return ErrEmptyDatabaseName
	}
	if len(retentionPolicy) == 0 {
		return ErrRetentionPolicy
	}
	if len(measurement) == 0 {
		return ErrEmptyMeasurement
	}
	return nil
}

// CheckDatabaseAndCommand checks if the database name or command is empty and returns an appropriate error.
func CheckDatabaseAndCommand(database, command string) error {
	if len(database) == 0 {
		return ErrEmptyDatabaseName
	}
	if len(command) == 0 {
		return ErrEmptyCommand
	}
	return nil
}
