// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opengemini

import "errors"

var (
	ErrAllServersDown    = errors.New("all servers down")
	ErrEmptyAuthToken    = errors.New("empty auth token")
	ErrEmptyAuthUsername = errors.New("empty auth username")
	ErrEmptyAuthPassword = errors.New("empty auth password")
	ErrEmptyDatabaseName = errors.New("empty database name")
	ErrEmptyMeasurement  = errors.New("empty measurement")
	ErrEmptyCommand      = errors.New("empty command")
	ErrEmptyTagOrField   = errors.New("empty tag or field")
	ErrEmptyTagKey       = errors.New("empty tag key")
	ErrRetentionPolicy   = errors.New("empty retention policy")
	ErrEmptyRecord       = errors.New("empty record")
	ErrEmptyAddress      = errors.New("empty address, must have at least one address")
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

func checkCommand(cmd string) error {
	if len(cmd) == 0 {
		return ErrEmptyCommand
	}
	return nil
}
