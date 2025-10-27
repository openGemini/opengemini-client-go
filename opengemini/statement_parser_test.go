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
	"strings"
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

		// Placeholder values (treated as string)
		{"placeholder", "$temp", "$temp", false},
		{"placeholder with underscore", "$my_value", "$my_value", false},

		// Edge cases
		{"unquoted string", "unquoted", "unquoted", false},
		{"mixed case boolean", "True", "True", false},

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
			name: "point with placeholders",
			lp:   "weather,location=$loc temperature=$temp",
			expected: &Point{
				Measurement: "weather",
				Tags: map[string]string{
					"location": "$loc",
				},
				Fields: map[string]any{
					"temperature": "$temp",
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
		{
			name: "insert multiple points (batch)",
			command: `INSERT weather,location=beijing temperature=25.5
weather,location=shanghai temperature=28.0
weather,location=guangzhou temperature=32.0`,
			expectedCount: 3,
			expectedMeas:  "weather",
			hasError:      false,
		},
		{
			name: "insert multiple points with empty lines",
			command: `INSERT weather,location=beijing temperature=25.5

weather,location=shanghai temperature=28.0

weather,location=guangzhou temperature=32.0`,
			expectedCount: 3,
			expectedMeas:  "weather",
			hasError:      false,
		},
		{
			name:          "insert with placeholders",
			command:       "INSERT weather,location=$loc temperature=$temp",
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
		{
			name: "insert multiple points with one invalid",
			command: `INSERT weather,location=beijing temperature=25.5
invalid_line_without_fields`,
			hasError: true,
		},
		{
			name:     "insert with only whitespace",
			command:  "INSERT    \n\n   ",
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

func TestReplacePointParams(t *testing.T) {
	tests := []struct {
		name     string
		point    *Point
		params   map[string]any
		expected *Point
		hasError bool
	}{
		{
			name: "replace tag value",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "$loc"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			params: map[string]any{"loc": "beijing"},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			hasError: false,
		},
		{
			name: "replace field value with type preservation",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": "$temp"},
			},
			params: map[string]any{"temp": 25.5},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			hasError: false,
		},
		{
			name: "replace field value with int type",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"humidity": "$hum"},
			},
			params: map[string]any{"hum": int64(60)},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"humidity": int64(60)},
			},
			hasError: false,
		},
		{
			name: "replace measurement",
			point: &Point{
				Measurement: "$meas",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			params: map[string]any{"meas": "weather"},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			hasError: false,
		},
		{
			name: "replace multiple parameters",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "$loc", "sensor": "$sensor"},
				Fields:      map[string]any{"temperature": "$temp", "humidity": "$hum"},
			},
			params: map[string]any{
				"loc":    "shanghai",
				"sensor": "001",
				"temp":   28.0,
				"hum":    int64(70),
			},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "shanghai", "sensor": "001"},
				Fields:      map[string]any{"temperature": 28.0, "humidity": int64(70)},
			},
			hasError: false,
		},
		{
			name: "no parameters to replace",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			params: map[string]any{},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			hasError: false,
		},

		// Error cases
		{
			name: "missing parameter",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "$loc"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			params:   map[string]any{"other": "value"},
			hasError: true,
		},
		{
			name: "missing field parameter",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": "$missing"},
			},
			params:   map[string]any{"other": "value"},
			hasError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := replacePointParams(test.point, test.params)
			if test.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expected.Measurement, test.point.Measurement)
				assert.Equal(t, test.expected.Tags, test.point.Tags)
				assert.Equal(t, test.expected.Fields, test.point.Fields)
			}
		})
	}
}

func TestFormatParamValueAsString(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"string value", "hello", "hello"},
		{"int value", 42, "42"},
		{"int64 value", int64(42), "42"},
		{"float64 value", 3.14, "3.14"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"uint value", uint(100), "100"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := formatParamValueAsString(test.value)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestIsPlaceholder(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"valid placeholder", "$temp", true},
		{"valid placeholder with underscore", "$my_value", true},
		{"valid placeholder with number", "$value123", true},
		{"not a placeholder - no dollar", "temp", false},
		{"not a placeholder - empty after dollar", "$", false},
		{"not a placeholder - special char", "$value!", false},
		{"not a placeholder - space", "$value name", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := isPlaceholder(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestExtractUnresolvedParams(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single unresolved parameter",
			input:    "value is $missing",
			expected: []string{"$missing"},
		},
		{
			name:     "multiple unresolved parameters",
			input:    "$param1 and $param2",
			expected: []string{"$param1", "$param2"},
		},
		{
			name:     "no unresolved parameters",
			input:    "no parameters here",
			expected: []string{},
		},
		{
			name:     "duplicate parameters",
			input:    "$param and $param again",
			expected: []string{"$param"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := extractUnresolvedParams(test.input)
			assert.ElementsMatch(t, test.expected, result)
		})
	}
}

// TestReplacePointParams_EdgeCases tests edge cases that might cause bugs
func TestReplacePointParams_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		point    *Point
		params   map[string]any
		expected *Point
		hasError bool
		errMsg   string
	}{
		{
			name: "placeholder name conflict - partial match",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "$loc"},
				Fields:      map[string]any{"temperature": "$temp", "temperature2": "$temperature"},
			},
			params: map[string]any{
				"loc":         "beijing",
				"temp":        25.5,
				"temperature": 30.0,
			},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": 25.5, "temperature2": 30.0},
			},
			hasError: false,
		},
		{
			name: "empty string parameter value",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "$loc"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			params: map[string]any{"loc": ""},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": ""},
				Fields:      map[string]any{"temperature": 25.5},
			},
			hasError: false,
		},
		{
			name: "placeholder in middle of string",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "city_$name_station"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			params: map[string]any{"name": "beijing"},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "city_beijing_station"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			hasError: false,
		},
		{
			name: "multiple placeholders in one value",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "$city-$country"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			params: map[string]any{
				"city":    "beijing",
				"country": "china",
			},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing-china"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			hasError: false,
		},
		{
			name: "field with zero value",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": "$temp"},
			},
			params: map[string]any{"temp": 0},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": 0},
			},
			hasError: false,
		},
		{
			name: "field with negative value",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": "$temp"},
			},
			params: map[string]any{"temp": -10.5},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"temperature": -10.5},
			},
			hasError: false,
		},
		{
			name: "field with boolean false",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"active": "$status"},
			},
			params: map[string]any{"status": false},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "beijing"},
				Fields:      map[string]any{"active": false},
			},
			hasError: false,
		},
		{
			name: "placeholder with special characters around it",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "[$loc]"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			params: map[string]any{"loc": "beijing"},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "[beijing]"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			hasError: false,
		},
		{
			name: "dollar sign but not placeholder",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "price:$100"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			params:   map[string]any{},
			hasError: true,
			errMsg:   "unresolved parameters",
		},
		{
			name: "placeholder key replacement",
			point: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"$tagkey": "value"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			params: map[string]any{"tagkey": "location"},
			expected: &Point{
				Measurement: "weather",
				Tags:        map[string]string{"location": "value"},
				Fields:      map[string]any{"temperature": 25.5},
			},
			hasError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := replacePointParams(test.point, test.params)
			if test.hasError {
				assert.Error(t, err)
				if test.errMsg != "" {
					assert.Contains(t, err.Error(), test.errMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expected.Measurement, test.point.Measurement)
				assert.Equal(t, test.expected.Tags, test.point.Tags)
				assert.Equal(t, test.expected.Fields, test.point.Fields)
			}
		})
	}
}

// TestParseInsertStatement_EdgeCases tests edge cases for INSERT parsing
func TestParseInsertStatement_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		validate func(*testing.T, []*Point, error)
	}{
		{
			name:    "mixed empty and valid lines",
			command: "INSERT weather,location=beijing temperature=25.5\n\n\nweather,location=shanghai temperature=28.0\n\n",
			validate: func(t *testing.T, points []*Point, err error) {
				assert.NoError(t, err)
				assert.Len(t, points, 2)
			},
		},
		{
			name:    "windows line endings (CRLF)",
			command: "INSERT weather,location=beijing temperature=25.5\r\nweather,location=shanghai temperature=28.0",
			validate: func(t *testing.T, points []*Point, err error) {
				assert.NoError(t, err)
				// Should handle at least one point correctly
				assert.NotNil(t, points)
			},
		},
		{
			name:    "leading and trailing whitespace per line",
			command: "INSERT   weather,location=beijing temperature=25.5  \n  weather,location=shanghai temperature=28.0  ",
			validate: func(t *testing.T, points []*Point, err error) {
				assert.NoError(t, err)
				assert.Len(t, points, 2)
			},
		},
		{
			name:    "INSERT keyword with different case",
			command: "InSeRt weather,location=beijing temperature=25.5",
			validate: func(t *testing.T, points []*Point, err error) {
				assert.NoError(t, err)
				assert.Len(t, points, 1)
			},
		},
		{
			name:    "measurement with numbers",
			command: "INSERT weather123,location=beijing temperature=25.5",
			validate: func(t *testing.T, points []*Point, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "weather123", points[0].Measurement)
			},
		},
		{
			name:    "very long field value",
			command: "INSERT weather,location=beijing description=\"" + strings.Repeat("a", 1000) + "\",temperature=25.5",
			validate: func(t *testing.T, points []*Point, err error) {
				assert.NoError(t, err)
				assert.Equal(t, strings.Repeat("a", 1000), points[0].Fields["description"])
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			points, err := parseInsertStatement(test.command)
			test.validate(t, points, err)
		})
	}
}

// TestParseLineProtocolToPoint_SpecialCases tests special cases in line protocol parsing
func TestParseLineProtocolToPoint_SpecialCases(t *testing.T) {
	tests := []struct {
		name     string
		lp       string
		validate func(*testing.T, *Point, error)
	}{
		{
			name: "field with very large integer",
			lp:   "weather,location=beijing temperature=9223372036854775807i",
			validate: func(t *testing.T, point *Point, err error) {
				assert.NoError(t, err)
				assert.Equal(t, int64(9223372036854775807), point.Fields["temperature"])
			},
		},
		{
			name: "field with very small negative integer",
			lp:   "weather,location=beijing temperature=-9223372036854775808i",
			validate: func(t *testing.T, point *Point, err error) {
				assert.NoError(t, err)
				assert.Equal(t, int64(-9223372036854775808), point.Fields["temperature"])
			},
		},
		{
			name: "field with scientific notation float",
			lp:   "weather,location=beijing temperature=1.23e10",
			validate: func(t *testing.T, point *Point, err error) {
				assert.NoError(t, err)
				assert.InDelta(t, 1.23e10, point.Fields["temperature"], 0.01)
			},
		},
		{
			name: "empty string field value",
			lp:   "weather,location=beijing description=\"\",temperature=25.5",
			validate: func(t *testing.T, point *Point, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "", point.Fields["description"])
			},
		},
		{
			name: "tag with underscore",
			lp:   "weather,location_name=beijing temperature=25.5",
			validate: func(t *testing.T, point *Point, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "beijing", point.Tags["location_name"])
			},
		},
		{
			name: "field with underscore",
			lp:   "weather,location=beijing temp_celsius=25.5",
			validate: func(t *testing.T, point *Point, err error) {
				assert.NoError(t, err)
				assert.Equal(t, 25.5, point.Fields["temp_celsius"])
			},
		},
		{
			name: "multiple fields with mixed types",
			lp:   "weather,location=beijing temp=25.5,humidity=60i,status=\"sunny\",active=true,count=100u",
			validate: func(t *testing.T, point *Point, err error) {
				assert.NoError(t, err)
				assert.Equal(t, 25.5, point.Fields["temp"])
				assert.Equal(t, int64(60), point.Fields["humidity"])
				assert.Equal(t, "sunny", point.Fields["status"])
				assert.Equal(t, true, point.Fields["active"])
				assert.Equal(t, uint64(100), point.Fields["count"])
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			point, err := parseLineProtocolToPoint(test.lp)
			test.validate(t, point, err)
		})
	}
}
