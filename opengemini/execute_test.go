// Copyright 2025 openGemini Authors
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

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatementType_String(t *testing.T) {
	tests := []struct {
		stmtType StatementType
		expected string
	}{
		{StatementTypeQuery, "Query"},
		{StatementTypeCommand, "Command"},
		{StatementTypeInsert, "Insert"},
		{StatementTypeUnknown, "Unknown"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			result := test.stmtType.String()
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestStatementType_IsQueryLike(t *testing.T) {
	tests := []struct {
		name     string
		stmtType StatementType
		expected bool
	}{
		{"Query is query-like", StatementTypeQuery, true},
		{"Command is query-like", StatementTypeCommand, true},
		{"Insert is not query-like", StatementTypeInsert, false},
		{"Unknown is not query-like", StatementTypeUnknown, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.stmtType.IsQueryLike()
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestStatementType_IsWriteLike(t *testing.T) {
	tests := []struct {
		name     string
		stmtType StatementType
		expected bool
	}{
		{"Insert is write-like", StatementTypeInsert, true},
		{"Query is not write-like", StatementTypeQuery, false},
		{"Command is not write-like", StatementTypeCommand, false},
		{"Unknown is not write-like", StatementTypeUnknown, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.stmtType.IsWriteLike()
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestValidateStatement(t *testing.T) {
	tests := []struct {
		name     string
		stmt     Statement
		hasError bool
		errorMsg string
	}{
		{
			name: "valid statement",
			stmt: Statement{
				Database: "testdb",
				Command:  "SELECT * FROM weather",
			},
			hasError: false,
		},
		{
			name: "missing database",
			stmt: Statement{
				Database: "",
				Command:  "SELECT * FROM weather",
			},
			hasError: true,
			errorMsg: "empty database name",
		},
		{
			name: "missing command",
			stmt: Statement{
				Database: "testdb",
				Command:  "",
			},
			hasError: true,
			errorMsg: "empty command",
		},
		{
			name: "statement with params",
			stmt: Statement{
				Database: "testdb",
				Command:  "INSERT weather,location=$loc temperature=$temp",
				Params: map[string]any{
					"loc":  "beijing",
					"temp": 25.5,
				},
			},
			hasError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStatement(test.stmt)
			if test.hasError {
				assert.Error(t, err)
				if test.errorMsg != "" {
					assert.Contains(t, err.Error(), test.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRouteToWrite_ParseError(t *testing.T) {
	tests := []struct {
		name        string
		command     string
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid single point insert",
			command: "INSERT weather,location=beijing temperature=25.5",
			wantErr: false,
		},
		{
			name: "valid batch insert",
			command: `INSERT weather,location=beijing temperature=25.5
weather,location=shanghai temperature=28.0`,
			wantErr: false,
		},
		{
			name:        "invalid insert - not an insert",
			command:     "SELECT * FROM weather",
			wantErr:     true,
			errContains: "not an INSERT statement",
		},
		{
			name:        "invalid insert - no fields",
			command:     "INSERT weather,location=beijing",
			wantErr:     true,
			errContains: "at least one field is required",
		},
		{
			name:        "invalid insert - empty",
			command:     "INSERT",
			wantErr:     true,
			errContains: "no valid data points",
		},
		{
			name:        "invalid insert - only whitespace after INSERT",
			command:     "INSERT   \n  ",
			wantErr:     true,
			errContains: "no valid data points",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			points, err := parseInsertStatement(test.command)
			if test.wantErr {
				assert.Error(t, err)
				assert.Nil(t, points)
				if test.errContains != "" {
					assert.Contains(t, err.Error(), test.errContains)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, points)
				assert.Greater(t, len(points), 0)
			}
		})
	}
}

func TestRouteToWrite_ParameterReplacement(t *testing.T) {
	tests := []struct {
		name         string
		command      string
		params       map[string]any
		expectError  bool
		errorMessage string
		validate     func(*testing.T, []*Point)
	}{
		{
			name:    "valid parameter replacement",
			command: "INSERT weather,location=$loc temperature=$temp",
			params: map[string]any{
				"loc":  "beijing",
				"temp": 25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Len(t, points, 1)
				assert.Equal(t, "beijing", points[0].Tags["location"])
				assert.Equal(t, 25.5, points[0].Fields["temperature"])
			},
		},
		{
			name:    "missing parameter",
			command: "INSERT weather,location=$loc temperature=$temp",
			params: map[string]any{
				"loc": "beijing",
				// missing "temp"
			},
			expectError:  true,
			errorMessage: "temp",
		},
		{
			name: "batch insert with different parameters",
			command: `INSERT weather,location=$loc1 temperature=$temp1
weather,location=$loc2 temperature=$temp2`,
			params: map[string]any{
				"loc1":  "beijing",
				"temp1": 25.5,
				"loc2":  "shanghai",
				"temp2": 28.0,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Len(t, points, 2)
				assert.Equal(t, "beijing", points[0].Tags["location"])
				assert.Equal(t, 25.5, points[0].Fields["temperature"])
				assert.Equal(t, "shanghai", points[1].Tags["location"])
				assert.Equal(t, 28.0, points[1].Fields["temperature"])
			},
		},
		{
			name: "batch insert with missing parameter in second point",
			command: `INSERT weather,location=$loc1 temperature=$temp1
weather,location=$loc2 temperature=$temp2`,
			params: map[string]any{
				"loc1":  "beijing",
				"temp1": 25.5,
				"loc2":  "shanghai",
				// missing "temp2"
			},
			expectError:  true,
			errorMessage: "point 2",
		},
		{
			name:    "parameter with type preservation - int64",
			command: "INSERT weather,location=$loc humidity=$hum",
			params: map[string]any{
				"loc": "beijing",
				"hum": int64(60),
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Len(t, points, 1)
				assert.Equal(t, int64(60), points[0].Fields["humidity"])
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Parse the INSERT statement
			points, err := parseInsertStatement(test.command)
			assert.NoError(t, err, "Failed to parse INSERT statement")
			assert.NotNil(t, points)

			// Replace parameters
			var replaceErr error
			for i, point := range points {
				if err := replacePointParams(point, test.params); err != nil {
					replaceErr = err
					// Add point number to error if it's a batch
					if len(points) > 1 {
						replaceErr = fmt.Errorf("failed to replace parameters for point %d: %w", i+1, err)
					}
					break
				}
			}

			if test.expectError {
				assert.Error(t, replaceErr)
				if test.errorMessage != "" {
					assert.Contains(t, replaceErr.Error(), test.errorMessage)
				}
			} else {
				assert.NoError(t, replaceErr)
				if test.validate != nil {
					test.validate(t, points)
				}
			}
		})
	}
}

func TestExecuteResult_Fields(t *testing.T) {
	t.Run("ExecuteResult with query", func(t *testing.T) {
		queryResult := &QueryResult{
			Results: []*SeriesResult{
				{
					Series: []*Series{
						{
							Name:    "weather",
							Columns: []string{"time", "temperature"},
							Values:  SeriesValues{},
						},
					},
				},
			},
		}

		result := &ExecuteResult{
			QueryResult:   queryResult,
			AffectedRows:  0,
			StatementType: StatementTypeQuery,
			Error:         nil,
		}

		assert.NotNil(t, result.QueryResult)
		assert.Equal(t, StatementTypeQuery, result.StatementType)
		assert.Equal(t, int64(0), result.AffectedRows)
		assert.NoError(t, result.Error)
	})

	t.Run("ExecuteResult with insert", func(t *testing.T) {
		result := &ExecuteResult{
			QueryResult:   nil,
			AffectedRows:  3,
			StatementType: StatementTypeInsert,
			Error:         nil,
		}

		assert.Nil(t, result.QueryResult)
		assert.Equal(t, StatementTypeInsert, result.StatementType)
		assert.Equal(t, int64(3), result.AffectedRows)
		assert.NoError(t, result.Error)
	})

	t.Run("ExecuteResult with error", func(t *testing.T) {
		testError := assert.AnError

		result := &ExecuteResult{
			QueryResult:   nil,
			AffectedRows:  0,
			StatementType: StatementTypeUnknown,
			Error:         testError,
		}

		assert.Nil(t, result.QueryResult)
		assert.Equal(t, StatementTypeUnknown, result.StatementType)
		assert.Equal(t, int64(0), result.AffectedRows)
		assert.Error(t, result.Error)
		assert.Equal(t, testError, result.Error)
	})
}

func TestExecuteContext_ValidationError(t *testing.T) {
	tests := []struct {
		name        string
		stmt        Statement
		expectedErr error
	}{
		{
			name: "missing database",
			stmt: Statement{
				Database: "",
				Command:  "SELECT * FROM weather",
			},
			expectedErr: ErrEmptyDatabaseName,
		},
		{
			name: "missing command",
			stmt: Statement{
				Database: "testdb",
				Command:  "",
			},
			expectedErr: ErrEmptyCommand,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStatement(test.stmt)
			assert.Error(t, err)
			assert.Equal(t, test.expectedErr, err)
		})
	}
}

func TestParseInsertStatement_Integration(t *testing.T) {
	// Integration test: parse INSERT -> replace params -> verify result
	tests := []struct {
		name     string
		command  string
		params   map[string]any
		validate func(*testing.T, []*Point)
	}{
		{
			name:    "single point with parameters",
			command: "INSERT weather,location=$loc temperature=$temp",
			params: map[string]any{
				"loc":  "beijing",
				"temp": 25.5,
			},
			validate: func(t *testing.T, points []*Point) {
				assert.Len(t, points, 1)
				// Before replacement, tags/fields contain placeholders
				assert.Equal(t, "$loc", points[0].Tags["location"])
				assert.Equal(t, "$temp", points[0].Fields["temperature"])

				// After replacement
				err := replacePointParams(points[0], map[string]any{
					"loc":  "beijing",
					"temp": 25.5,
				})
				assert.NoError(t, err)
				assert.Equal(t, "beijing", points[0].Tags["location"])
				assert.Equal(t, 25.5, points[0].Fields["temperature"])
			},
		},
		{
			name: "batch insert with parameters",
			command: `INSERT weather,location=$loc1 temperature=$temp1
weather,location=$loc2 temperature=$temp2`,
			params: map[string]any{
				"loc1":  "beijing",
				"temp1": 25.5,
				"loc2":  "shanghai",
				"temp2": 28.0,
			},
			validate: func(t *testing.T, points []*Point) {
				assert.Len(t, points, 2)

				// Replace params for both points
				params := map[string]any{
					"loc1":  "beijing",
					"temp1": 25.5,
					"loc2":  "shanghai",
					"temp2": 28.0,
				}

				for _, point := range points {
					err := replacePointParams(point, params)
					assert.NoError(t, err)
				}

				// Verify first point
				assert.Equal(t, "beijing", points[0].Tags["location"])
				assert.Equal(t, 25.5, points[0].Fields["temperature"])

				// Verify second point
				assert.Equal(t, "shanghai", points[1].Tags["location"])
				assert.Equal(t, 28.0, points[1].Fields["temperature"])
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			points, err := parseInsertStatement(test.command)
			assert.NoError(t, err)
			test.validate(t, points)
		})
	}
}

func TestStatement_Structure(t *testing.T) {
	t.Run("Statement with all fields", func(t *testing.T) {
		stmt := Statement{
			Database:        "testdb",
			Command:         "INSERT weather,location=$loc temperature=$temp",
			RetentionPolicy: "default",
			Params: map[string]any{
				"loc":  "beijing",
				"temp": 25.5,
			},
		}

		assert.Equal(t, "testdb", stmt.Database)
		assert.Equal(t, "INSERT weather,location=$loc temperature=$temp", stmt.Command)
		assert.Equal(t, "default", stmt.RetentionPolicy)
		assert.Len(t, stmt.Params, 2)
		assert.Equal(t, "beijing", stmt.Params["loc"])
		assert.Equal(t, 25.5, stmt.Params["temp"])
	})

	t.Run("Statement with minimal fields", func(t *testing.T) {
		stmt := Statement{
			Database: "testdb",
			Command:  "SELECT * FROM weather",
		}

		assert.Equal(t, "testdb", stmt.Database)
		assert.Equal(t, "SELECT * FROM weather", stmt.Command)
		assert.Empty(t, stmt.RetentionPolicy)
		assert.Nil(t, stmt.Params)
	})
}

func TestAffectedRowsCount(t *testing.T) {
	tests := []struct {
		name          string
		command       string
		expectedCount int
	}{
		{
			name:          "single point insert",
			command:       "INSERT weather,location=beijing temperature=25.5",
			expectedCount: 1,
		},
		{
			name: "batch insert with 3 points",
			command: `INSERT weather,location=beijing temperature=25.5
weather,location=shanghai temperature=28.0
weather,location=guangzhou temperature=32.0`,
			expectedCount: 3,
		},
		{
			name: "batch insert with 5 points",
			command: `INSERT weather,location=beijing temperature=25.5
weather,location=shanghai temperature=28.0
weather,location=guangzhou temperature=32.0
weather,location=shenzhen temperature=30.0
weather,location=chengdu temperature=22.5`,
			expectedCount: 5,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			points, err := parseInsertStatement(test.command)
			assert.NoError(t, err)
			assert.Len(t, points, test.expectedCount)

			// The affected rows count should match the number of parsed points
			// This is what AffectedRows in ExecuteResult would be set to
			assert.Equal(t, test.expectedCount, len(points))
		})
	}
}

// TestParameterReplacement_EdgeCases tests edge cases in parameter replacement that might cause bugs
func TestParameterReplacement_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		command     string
		params      map[string]any
		expectError bool
		errorMsg    string
		validate    func(*testing.T, []*Point)
	}{
		{
			name:    "parameter value with special characters",
			command: "INSERT weather,location=$loc temperature=$temp",
			params: map[string]any{
				"loc":  "beijing-haidian",
				"temp": 25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Equal(t, "beijing-haidian", points[0].Tags["location"])
			},
		},
		{
			name:    "parameter value with unicode",
			command: "INSERT weather,location=$loc temperature=$temp",
			params: map[string]any{
				"loc":  "北京",
				"temp": 25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Equal(t, "北京", points[0].Tags["location"])
			},
		},
		{
			name:    "numeric parameter for tag",
			command: "INSERT weather,location=$loc,sensor=$sensorid temperature=$temp",
			params: map[string]any{
				"loc":      "beijing",
				"sensorid": 12345,
				"temp":     25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Equal(t, "12345", points[0].Tags["sensor"])
			},
		},
		{
			name:    "mixed types in parameters",
			command: "INSERT weather,location=$loc temperature=$temp,humidity=$hum,active=$active",
			params: map[string]any{
				"loc":    "beijing",
				"temp":   25.5,
				"hum":    int64(60),
				"active": true,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Equal(t, "beijing", points[0].Tags["location"])
				assert.Equal(t, 25.5, points[0].Fields["temperature"])
				assert.Equal(t, int64(60), points[0].Fields["humidity"])
				assert.Equal(t, true, points[0].Fields["active"])
			},
		},
		{
			name:    "parameter in measurement name",
			command: "INSERT $measurement,location=$loc temperature=$temp",
			params: map[string]any{
				"measurement": "weather",
				"loc":         "beijing",
				"temp":        25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Equal(t, "weather", points[0].Measurement)
			},
		},
		{
			name: "batch with partial parameters for each point",
			command: `INSERT weather,location=$loc1 temperature=$temp1
weather,location=$loc2 temperature=$temp2`,
			params: map[string]any{
				"loc1":  "beijing",
				"temp1": 25.5,
				"loc2":  "shanghai",
				"temp2": 28.0,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Len(t, points, 2)
				assert.Equal(t, "beijing", points[0].Tags["location"])
				assert.Equal(t, "shanghai", points[1].Tags["location"])
			},
		},
		{
			name:    "case sensitive parameter names",
			command: "INSERT weather,location=$Loc temperature=$TEMP",
			params: map[string]any{
				"Loc":  "beijing",
				"TEMP": 25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Equal(t, "beijing", points[0].Tags["location"])
				assert.Equal(t, 25.5, points[0].Fields["temperature"])
			},
		},
		{
			name:    "parameter with underscore in name",
			command: "INSERT weather,location=$city_name temperature=$temp_value",
			params: map[string]any{
				"city_name":  "beijing",
				"temp_value": 25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Equal(t, "beijing", points[0].Tags["location"])
				assert.Equal(t, 25.5, points[0].Fields["temperature"])
			},
		},
		{
			name:    "extra parameters provided (should not cause error)",
			command: "INSERT weather,location=$loc temperature=$temp",
			params: map[string]any{
				"loc":   "beijing",
				"temp":  25.5,
				"extra": "unused",
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Equal(t, "beijing", points[0].Tags["location"])
				assert.Equal(t, 25.5, points[0].Fields["temperature"])
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			points, err := parseInsertStatement(test.command)
			assert.NoError(t, err, "Failed to parse INSERT statement")

			var replaceErr error
			for i, point := range points {
				if err := replacePointParams(point, test.params); err != nil {
					replaceErr = err
					if len(points) > 1 {
						replaceErr = fmt.Errorf("failed to replace parameters for point %d: %w", i+1, err)
					}
					break
				}
			}

			if test.expectError {
				assert.Error(t, replaceErr)
				if test.errorMsg != "" {
					assert.Contains(t, replaceErr.Error(), test.errorMsg)
				}
			} else {
				assert.NoError(t, replaceErr)
				if test.validate != nil {
					test.validate(t, points)
				}
			}
		})
	}
}

// TestInjectionPrevention tests that the structured approach prevents injection attacks
func TestInjectionPrevention(t *testing.T) {
	tests := []struct {
		name        string
		command     string
		params      map[string]any
		expectError bool
		validate    func(*testing.T, []*Point)
	}{
		{
			name:    "injection attempt via tag value - comma",
			command: "INSERT weather,location=$loc temperature=$temp",
			params: map[string]any{
				"loc":  "beijing,fake_tag=evil", // Attempt to inject extra tag
				"temp": 25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				// The entire malicious string should be stored as ONE tag value
				assert.Equal(t, "beijing,fake_tag=evil", points[0].Tags["location"])
				// When encoded, it will be escaped: beijing\,fake_tag\=evil
				// So "fake_tag" won't be parsed as a separate tag
				assert.NotContains(t, points[0].Tags, "fake_tag")
			},
		},
		{
			name:    "injection attempt via tag value - space",
			command: "INSERT weather,location=$loc temperature=$temp",
			params: map[string]any{
				"loc":  "beijing extra=value", // Attempt to inject field
				"temp": 25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				assert.Equal(t, "beijing extra=value", points[0].Tags["location"])
				// Won't be parsed as a field because space is escaped
				assert.NotContains(t, points[0].Fields, "extra")
			},
		},
		{
			name:    "injection attempt via field string - quote escape",
			command: "INSERT weather,location=$loc description=$desc,temperature=$temp",
			params: map[string]any{
				"loc":  "beijing",
				"desc": "test\",hacked=true,evil=\"pwned", // Try to close quote
				"temp": 25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				// The entire string including quotes should be stored
				assert.Equal(t, "test\",hacked=true,evil=\"pwned", points[0].Fields["description"])
				// When encoded, quotes will be escaped: "test\",hacked=true,evil=\"pwned"
				assert.NotContains(t, points[0].Fields, "hacked")
			},
		},
		{
			name:    "injection attempt via newline",
			command: "INSERT weather,location=$loc temperature=$temp",
			params: map[string]any{
				"loc":  "beijing\nweather,location=evil temperature=999",
				"temp": 25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				// Newline is part of the tag value, won't create a new point
				assert.Contains(t, points[0].Tags["location"], "\n")
				assert.Contains(t, points[0].Tags["location"], "evil")
			},
		},
		{
			name:    "injection attempt via measurement",
			command: "INSERT $meas,location=$loc temperature=$temp",
			params: map[string]any{
				"meas": "weather,injected=tag",
				"loc":  "beijing",
				"temp": 25.5,
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				// Comma in measurement will be escaped
				assert.Equal(t, "weather,injected=tag", points[0].Measurement)
			},
		},
		{
			name:    "type preservation prevents injection",
			command: "INSERT weather,location=$loc temperature=$temp",
			params: map[string]any{
				"loc":  "beijing",
				"temp": int64(25), // int64 type
			},
			expectError: false,
			validate: func(t *testing.T, points []*Point) {
				// Type is preserved, so it's encoded as "25i", not parsed from string
				assert.Equal(t, int64(25), points[0].Fields["temperature"])
				// This prevents injection because the value never goes through string parsing
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			points, err := parseInsertStatement(test.command)
			assert.NoError(t, err)

			if len(test.params) > 0 {
				for _, point := range points {
					err := replacePointParams(point, test.params)
					if test.expectError {
						assert.Error(t, err)
						return
					}
					assert.NoError(t, err)
				}
			}

			if test.validate != nil {
				test.validate(t, points)
			}
		})
	}
}

// TestValidateStatement_EdgeCases tests edge cases in statement validation
func TestValidateStatement_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		stmt        Statement
		expectError bool
		expectedErr error
	}{
		{
			name: "valid statement with all fields",
			stmt: Statement{
				Database:        "testdb",
				Command:         "SELECT * FROM weather",
				RetentionPolicy: "default",
				Params:          map[string]any{"key": "value"},
			},
			expectError: false,
		},
		{
			name: "valid statement with minimal fields",
			stmt: Statement{
				Database: "testdb",
				Command:  "SELECT * FROM weather",
			},
			expectError: false,
		},
		{
			name: "whitespace only database name",
			stmt: Statement{
				Database: "   ",
				Command:  "SELECT * FROM weather",
			},
			expectError: false, // TrimSpace not applied in validate
		},
		{
			name: "whitespace only command",
			stmt: Statement{
				Database: "testdb",
				Command:  "   ",
			},
			expectError: false, // TrimSpace not applied in validate
		},
		{
			name: "very long database name",
			stmt: Statement{
				Database: "db_" + strings.Repeat("a", 1000),
				Command:  "SELECT * FROM weather",
			},
			expectError: false,
		},
		{
			name: "very long command",
			stmt: Statement{
				Database: "testdb",
				Command:  "SELECT * FROM " + strings.Repeat("weather_", 100),
			},
			expectError: false,
		},
		{
			name: "command with newlines",
			stmt: Statement{
				Database: "testdb",
				Command:  "SELECT *\nFROM weather\nWHERE location='beijing'",
			},
			expectError: false,
		},
		{
			name: "empty params map",
			stmt: Statement{
				Database: "testdb",
				Command:  "SELECT * FROM weather",
				Params:   map[string]any{},
			},
			expectError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStatement(test.stmt)
			if test.expectError {
				assert.Error(t, err)
				if test.expectedErr != nil {
					assert.Equal(t, test.expectedErr, err)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
