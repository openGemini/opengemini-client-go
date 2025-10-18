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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseStatementType(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected StatementType
	}{
		// Query statements
		{"select statement", "SELECT * FROM weather", StatementTypeQuery},
		{"select with case", "select * from weather", StatementTypeQuery},
		{"show statement", "SHOW MEASUREMENTS", StatementTypeQuery},
		{"explain statement", "EXPLAIN SELECT * FROM weather", StatementTypeQuery},
		{"describe statement", "DESCRIBE weather", StatementTypeQuery},
		{"desc statement", "DESC weather", StatementTypeQuery},
		{"with statement", "WITH data AS (SELECT * FROM weather) SELECT * FROM data", StatementTypeQuery},

		// Command statements
		{"create statement", "CREATE DATABASE test", StatementTypeCommand},
		{"drop statement", "DROP DATABASE test", StatementTypeCommand},
		{"alter statement", "ALTER RETENTION POLICY default ON test DURATION 1h", StatementTypeCommand},
		{"update statement", "UPDATE RETENTION POLICY default ON test DURATION 2h", StatementTypeCommand},
		{"delete statement", "DELETE FROM weather WHERE time < now() - 1h", StatementTypeCommand},

		// Insert statements
		{"insert statement", "INSERT weather,location=beijing temperature=25.5", StatementTypeInsert},
		{"insert with case", "insert weather,location=beijing temperature=25.5", StatementTypeInsert},

		// Unknown statements
		{"empty command", "", StatementTypeUnknown},
		{"unknown statement", "UNKNOWN COMMAND", StatementTypeUnknown},
		{"whitespace only", "   \t\n   ", StatementTypeUnknown},

		// With comments and extra whitespace
		{"query with comment", "SELECT * FROM weather -- this is a comment", StatementTypeQuery},
		{"command with multiline comment", "CREATE /* comment */ DATABASE test", StatementTypeCommand},
		{"statement with leading whitespace", "   SELECT * FROM weather", StatementTypeQuery},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := parseStatementType(test.command)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestIsQueryKeyword(t *testing.T) {
	queryKeywords := []string{"SELECT", "SHOW", "EXPLAIN", "DESCRIBE", "DESC", "WITH"}
	nonQueryKeywords := []string{"CREATE", "DROP", "INSERT", "UPDATE", "DELETE", "UNKNOWN"}

	for _, keyword := range queryKeywords {
		assert.True(t, isQueryKeyword(keyword), "Expected %s to be a query keyword", keyword)
	}

	for _, keyword := range nonQueryKeywords {
		assert.False(t, isQueryKeyword(keyword), "Expected %s to NOT be a query keyword", keyword)
	}
}

func TestIsCommandKeyword(t *testing.T) {
	commandKeywords := []string{"CREATE", "DROP", "ALTER", "UPDATE", "DELETE"}
	nonCommandKeywords := []string{"SELECT", "SHOW", "INSERT", "EXPLAIN", "UNKNOWN"}

	for _, keyword := range commandKeywords {
		assert.True(t, isCommandKeyword(keyword), "Expected %s to be a command keyword", keyword)
	}

	for _, keyword := range nonCommandKeywords {
		assert.False(t, isCommandKeyword(keyword), "Expected %s to NOT be a command keyword", keyword)
	}
}

func TestIsInsertKeyword(t *testing.T) {
	assert.True(t, isInsertKeyword("INSERT"))
	assert.False(t, isInsertKeyword("SELECT"))
	assert.False(t, isInsertKeyword("CREATE"))
	assert.False(t, isInsertKeyword("SHOW"))
	assert.False(t, isInsertKeyword("UNKNOWN"))
}

func TestCleanCommand(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected string
	}{
		{
			name:     "no comments",
			command:  "SELECT * FROM weather",
			expected: "SELECT * FROM weather",
		},
		{
			name:     "single line comment",
			command:  "SELECT * FROM weather -- this is a comment",
			expected: "SELECT * FROM weather",
		},
		{
			name:     "multiline comment",
			command:  "SELECT /* comment */ * FROM weather",
			expected: "SELECT  * FROM weather",
		},
		{
			name:     "multiple multiline comments",
			command:  "SELECT /* comment1 */ * /* comment2 */ FROM weather",
			expected: "SELECT  *  FROM weather",
		},
		{
			name:     "comment at start",
			command:  "/* comment */SELECT * FROM weather",
			expected: "SELECT * FROM weather",
		},
		{
			name:     "comment at end",
			command:  "SELECT * FROM weather /* comment */",
			expected: "SELECT * FROM weather",
		},
		{
			name:     "extra whitespace",
			command:  "   SELECT   *   FROM   weather   ",
			expected: "SELECT   *   FROM   weather",
		},
		{
			name:     "mixed comments and whitespace",
			command:  "  SELECT /* comment */ * FROM weather -- end comment  ",
			expected: "SELECT  * FROM weather",
		},
		{
			name:     "incomplete multiline comment",
			command:  "SELECT * FROM weather /* incomplete comment",
			expected: "SELECT * FROM weather /* incomplete comment",
		},
		{
			name:     "empty command",
			command:  "",
			expected: "",
		},
		{
			name:     "only comment",
			command:  "-- just a comment",
			expected: "",
		},
		{
			name:     "only multiline comment",
			command:  "/* just a comment */",
			expected: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := cleanCommand(test.command)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestParseFieldValue(t *testing.T) {
	tests := []struct {
		name     string
		valueStr string
		expected any
		hasError bool
	}{
		// Integer values
		{"integer with i suffix", "42i", int64(42), false},
		{"integer with I suffix", "123I", int64(123), false},
		{"negative integer", "-42i", int64(-42), false},

		// Unsigned integer values
		{"unsigned integer with u suffix", "42u", uint64(42), false},
		{"unsigned integer with U suffix", "123U", uint64(123), false},

		// String values
		{"quoted string", "\"hello world\"", "hello world", false},
		{"empty quoted string", "\"\"", "", false},
		{"string with spaces", "\"hello world with spaces\"", "hello world with spaces", false},

		// Boolean values
		{"boolean true", "true", true, false},
		{"boolean TRUE", "TRUE", true, false},
		{"boolean t", "t", true, false},
		{"boolean T", "T", true, false},
		{"boolean false", "false", false, false},
		{"boolean FALSE", "FALSE", false, false},
		{"boolean f", "f", false, false},
		{"boolean F", "F", false, false},

		// Float values
		{"positive float", "3.14", 3.14, false},
		{"negative float", "-2.5", -2.5, false},
		{"integer as float", "42", float64(42), false},
		{"scientific notation", "1.23e-4", 1.23e-4, false},

		// Edge cases
		{"unquoted string", "unquoted", "unquoted", false}, // Falls back to string
		{"mixed case boolean", "True", "True", false},      // Falls back to string since not exact match

		// Values with whitespace
		{"value with leading space", " 42i", int64(42), false},
		{"value with trailing space", "42i ", int64(42), false},
		{"quoted string with whitespace", " \"hello\" ", "hello", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseFieldValue(test.valueStr)
			if test.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expected, result)
			}
		})
	}
}

func TestParseLineProtocolToPoint(t *testing.T) {
	tests := []struct {
		name     string
		lp       string
		expected *Point
		hasError bool
	}{
		{
			name: "basic point with one field",
			lp:   "weather temperature=25.5",
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{},
				Fields:      map[string]any{"temperature": 25.5},
			},
			hasError: false,
		},
		{
			name: "point with tags and fields",
			lp:   "weather,location=beijing,sensor=001 temperature=25.5,humidity=60i",
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "beijing",
					"sensor":   "001",
				},
				Fields: map[string]any{
					"temperature": 25.5,
					"humidity":    int64(60),
				},
			},
			hasError: false,
		},
		{
			name: "point with timestamp",
			lp:   "weather,location=shanghai temperature=30.0 1609459200000000000",
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "shanghai",
				},
				Fields: map[string]any{
					"temperature": 30.0,
				},
				Timestamp: 1609459200000000000,
			},
			hasError: false,
		},
		{
			name: "point with string field",
			lp:   "weather,location=beijing status=\"sunny\",temperature=25.5",
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "beijing",
				},
				Fields: map[string]any{
					"status":      "sunny",
					"temperature": 25.5,
				},
			},
			hasError: false,
		},
		{
			name: "point with boolean fields",
			lp:   "weather,location=beijing active=true,offline=false",
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "beijing",
				},
				Fields: map[string]any{
					"active":  true,
					"offline": false,
				},
			},
			hasError: false,
		},
		{
			name: "point with unsigned integer",
			lp:   "weather,location=beijing count=100u,temperature=25.5",
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "beijing",
				},
				Fields: map[string]any{
					"count":       uint64(100),
					"temperature": 25.5,
				},
			},
			hasError: false,
		},

		// Error cases
		{
			name:     "missing fields",
			lp:       "weather,location=beijing",
			expected: nil,
			hasError: true,
		},
		{
			name:     "empty measurement",
			lp:       ",location=beijing temperature=25.5",
			expected: nil,
			hasError: true,
		},
		{
			name:     "invalid tag format",
			lp:       "weather,invalid_tag temperature=25.5",
			expected: nil,
			hasError: true,
		},
		{
			name:     "invalid field format",
			lp:       "weather,location=beijing invalid_field",
			expected: nil,
			hasError: true,
		},
		{
			name:     "invalid timestamp",
			lp:       "weather temperature=25.5 invalid_timestamp",
			expected: nil,
			hasError: true,
		},
		{
			name:     "empty line protocol",
			lp:       "",
			expected: nil,
			hasError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseLineProtocolToPoint(test.lp)
			if test.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, test.expected.Measurement, result.Measurement)
				assert.Equal(t, test.expected.Tags, result.Tags)
				assert.Equal(t, test.expected.Fields, result.Fields)
				if test.expected.Timestamp != 0 {
					assert.Equal(t, test.expected.Timestamp, result.Timestamp)
				}
			}
		})
	}
}

func TestParseInsertStatement(t *testing.T) {
	tests := []struct {
		name          string
		command       string
		expectedCount int
		expectedMeas  string
		hasError      bool
	}{
		{
			name:          "valid insert statement",
			command:       "INSERT weather,location=beijing temperature=25.5",
			expectedCount: 1,
			expectedMeas:  "weather",
			hasError:      false,
		},
		{
			name:          "insert with case insensitive",
			command:       "insert weather,location=shanghai temperature=30.0",
			expectedCount: 1,
			expectedMeas:  "weather",
			hasError:      false,
		},
		{
			name:          "insert with complex fields",
			command:       "INSERT weather,location=beijing,sensor=001 temperature=25.5,humidity=60i,status=\"active\"",
			expectedCount: 1,
			expectedMeas:  "weather",
			hasError:      false,
		},

		// Error cases
		{
			name:     "not an insert statement",
			command:  "SELECT * FROM weather",
			hasError: true,
		},
		{
			name:     "invalid line protocol",
			command:  "INSERT invalid_format",
			hasError: true,
		},
		{
			name:     "empty insert statement",
			command:  "INSERT",
			hasError: true,
		},
		{
			name:     "insert without fields",
			command:  "INSERT weather,location=beijing",
			hasError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseInsertStatement(test.command)
			if test.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Len(t, result, test.expectedCount)
				if len(result) > 0 {
					assert.Equal(t, test.expectedMeas, result[0].Measurement)
				}
			}
		})
	}
}

func TestParseInsertStatementComplexCases(t *testing.T) {
	// Test a complete INSERT statement with all types of fields
	command := "INSERT weather,location=beijing,sensor=sensor_001 temperature=25.5,humidity=60i,pressure=1013.25,status=\"sunny\",active=true,count=100u 1609459200000000000"

	points, err := parseInsertStatement(command)
	assert.NoError(t, err)
	assert.Len(t, points, 1)

	point := points[0]
	assert.Equal(t, "weather", point.Measurement)

	// Check tags
	expectedTags := map[string]string{
		"location": "beijing",
		"sensor":   "sensor_001",
	}
	assert.Equal(t, expectedTags, point.Tags)

	// Check fields
	assert.Equal(t, 25.5, point.Fields["temperature"])
	assert.Equal(t, int64(60), point.Fields["humidity"])
	assert.Equal(t, 1013.25, point.Fields["pressure"])
	assert.Equal(t, "sunny", point.Fields["status"])
	assert.Equal(t, true, point.Fields["active"])
	assert.Equal(t, uint64(100), point.Fields["count"])

	// Check timestamp
	assert.Equal(t, int64(1609459200000000000), point.Timestamp)
}

// TestParseLineProtocolWithEscapeCharacters tests escape character handling
func TestParseLineProtocolWithEscapeCharacters(t *testing.T) {
	tests := []struct {
		name     string
		lp       string
		expected *Point
		hasError bool
	}{
		{
			name: "escaped space in tag value",
			lp:   `weather,location=San\ Francisco temperature=25.5`,
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "San Francisco",
				},
				Fields: map[string]any{
					"temperature": 25.5,
				},
			},
			hasError: false,
		},
		{
			name: "escaped comma in tag value",
			lp:   `weather,location=Beijing\,China temperature=25.5`,
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "Beijing,China",
				},
				Fields: map[string]any{
					"temperature": 25.5,
				},
			},
			hasError: false,
		},
		{
			name: "escaped equals in tag value",
			lp:   `weather,equation=x\=y temperature=25.5`,
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"equation": "x=y",
				},
				Fields: map[string]any{
					"temperature": 25.5,
				},
			},
			hasError: false,
		},
		{
			name: "escaped backslash in tag value",
			lp:   `weather,path=C:\\Windows temperature=25.5`,
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"path": `C:\Windows`, // \\ becomes \
				},
				Fields: map[string]any{
					"temperature": 25.5,
				},
			},
			hasError: false,
		},
		{
			name: "multiple escaped characters",
			lp:   `weather,location=San\ Francisco\,CA,tag=value\=test temperature=25.5,humidity=60i`,
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "San Francisco,CA",
					"tag":      "value=test",
				},
				Fields: map[string]any{
					"temperature": 25.5,
					"humidity":    int64(60),
				},
			},
			hasError: false,
		},
		{
			name: "escaped space in measurement name",
			lp:   `my\ measurement temperature=25.5`,
			expected: &Point{
				Measurement: "my measurement",
				Tags:        map[string]string{},
				Fields: map[string]any{
					"temperature": 25.5,
				},
			},
			hasError: false,
		},
		{
			name: "complex case with escaped characters",
			lp:   `weather,location=New\ York\,NY,sensor=sensor\ 001 temperature=25.5,status="partly cloudy",count=100i`,
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "New York,NY",
					"sensor":   "sensor 001",
				},
				Fields: map[string]any{
					"temperature": 25.5,
					"status":      "partly cloudy",
					"count":       int64(100),
				},
			},
			hasError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseLineProtocolToPoint(test.lp)
			if test.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, test.expected.Measurement, result.Measurement)
				assert.Equal(t, test.expected.Tags, result.Tags)
				assert.Equal(t, test.expected.Fields, result.Fields)
			}
		})
	}
}

// TestParseInsertStatementWithEscapeCharacters tests INSERT statements with escape characters
func TestParseInsertStatementWithEscapeCharacters(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected *Point
		hasError bool
	}{
		{
			name:    "INSERT with escaped space",
			command: `INSERT weather,location=San\ Francisco temperature=25.5`,
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "San Francisco",
				},
				Fields: map[string]any{
					"temperature": 25.5,
				},
			},
			hasError: false,
		},
		{
			name:    "INSERT with multiple escaped characters",
			command: `INSERT weather,location=Beijing\,China,zone=UTC\+8 temperature=20.0,humidity=65i`,
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "Beijing,China",
					"zone":     "UTC+8",
				},
				Fields: map[string]any{
					"temperature": 20.0,
					"humidity":    int64(65),
				},
			},
			hasError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseInsertStatement(test.command)
			if test.hasError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Len(t, result, 1)
				point := result[0]
				assert.Equal(t, test.expected.Measurement, point.Measurement)
				assert.Equal(t, test.expected.Tags, point.Tags)
				assert.Equal(t, test.expected.Fields, point.Fields)
			}
		})
	}
}
