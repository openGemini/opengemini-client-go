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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatementTypeString(t *testing.T) {
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
		assert.Equal(t, test.expected, test.stmtType.String())
	}
}

func TestStatementTypeIsQueryLike(t *testing.T) {
	assert.True(t, StatementTypeQuery.IsQueryLike())
	assert.True(t, StatementTypeCommand.IsQueryLike())
	assert.False(t, StatementTypeInsert.IsQueryLike())
	assert.False(t, StatementTypeUnknown.IsQueryLike())
}

func TestStatementTypeIsWriteLike(t *testing.T) {
	assert.False(t, StatementTypeQuery.IsWriteLike())
	assert.False(t, StatementTypeCommand.IsWriteLike())
	assert.True(t, StatementTypeInsert.IsWriteLike())
	assert.False(t, StatementTypeUnknown.IsWriteLike())
}

func TestValidateStatement(t *testing.T) {
	tests := []struct {
		name     string
		stmt     Statement
		hasError bool
	}{
		{
			name: "valid statement",
			stmt: Statement{
				Database: "test_db",
				Command:  "SELECT * FROM measurement",
			},
			hasError: false,
		},
		{
			name: "missing database",
			stmt: Statement{
				Command: "SELECT * FROM measurement",
			},
			hasError: true,
		},
		{
			name: "missing command",
			stmt: Statement{
				Database: "test_db",
			},
			hasError: true,
		},
		{
			name: "empty database",
			stmt: Statement{
				Database: "",
				Command:  "SELECT * FROM measurement",
			},
			hasError: true,
		},
		{
			name: "empty command",
			stmt: Statement{
				Database: "test_db",
				Command:  "",
			},
			hasError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStatement(test.stmt)
			if test.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConvertParamValue(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
		hasError bool
	}{
		{"string", "hello", "hello", false},
		{"int", 42, "42i", false},
		{"int8", int8(8), "8i", false},
		{"int16", int16(16), "16i", false},
		{"int32", int32(32), "32i", false},
		{"int64", int64(64), "64i", false},
		{"uint", uint(42), "42u", false},
		{"uint8", uint8(8), "8u", false},
		{"uint16", uint16(16), "16u", false},
		{"uint32", uint32(32), "32u", false},
		{"uint64", uint64(64), "64u", false},
		{"float32", float32(3.14), "3.14", false},
		{"float64", 3.14159, "3.14159", false},
		{"bool true", true, "true", false},
		{"bool false", false, "false", false},
		{"nil value", nil, "", true},
		{"custom type", struct{ Name string }{"test"}, "{test}", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := convertParamValue(test.value)
			if test.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expected, result)
			}
		})
	}
}

func TestReplaceParams(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		params   map[string]any
		expected string
		hasError bool
	}{
		{
			name:     "no parameters",
			command:  "SELECT * FROM measurement",
			params:   map[string]any{},
			expected: "SELECT * FROM measurement",
			hasError: false,
		},
		{
			name:    "single parameter",
			command: "SELECT * FROM $table",
			params: map[string]any{
				"table": "weather",
			},
			expected: "SELECT * FROM weather",
			hasError: false,
		},
		{
			name:    "multiple parameters",
			command: "INSERT $measurement,location=$location temperature=$temp",
			params: map[string]any{
				"measurement": "weather",
				"location":    "beijing",
				"temp":        25.5,
			},
			expected: "INSERT weather,location=beijing temperature=25.5",
			hasError: false,
		},
		{
			name:    "unused parameters",
			command: "SELECT * FROM weather",
			params: map[string]any{
				"unused": "value",
			},
			expected: "SELECT * FROM weather",
			hasError: false,
		},
		{
			name:    "unresolved parameters",
			command: "SELECT * FROM $table WHERE id = $id",
			params: map[string]any{
				"table": "weather",
			},
			expected: "",
			hasError: true,
		},
		{
			name:    "invalid parameter value",
			command: "SELECT * FROM $table",
			params: map[string]any{
				"table": nil,
			},
			expected: "",
			hasError: true,
		},
		{
			name:    "parameter with different types",
			command: "INSERT weather,location=$loc temp=$temp,active=$active,count=$count",
			params: map[string]any{
				"loc":    "shanghai",
				"temp":   30.2,
				"active": true,
				"count":  100,
			},
			expected: "INSERT weather,location=shanghai temp=30.2,active=true,count=100i",
			hasError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := replaceParams(test.command, test.params)
			if test.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expected, result)
			}
		})
	}
}

func TestClientExecuteQueryStatement(t *testing.T) {
	c := testDefaultClient(t)

	// create a test database
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	require.NoError(t, err)

	// cleanup database
	defer func() {
		err := c.DropDatabase(database)
		assert.NoError(t, err)
	}()

	// Test SHOW statement (Query type)
	result, err := c.Execute(Statement{
		Database: database,
		Command:  "SHOW MEASUREMENTS",
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, StatementTypeQuery, result.StatementType)
	assert.NotNil(t, result.QueryResult)
	assert.Equal(t, int64(0), result.AffectedRows) // Query statements don't affect rows
	assert.NoError(t, result.Error)
}

func TestClientExecuteCommandStatement(t *testing.T) {
	c := testDefaultClient(t)

	database := randomDatabaseName()

	// Test CREATE DATABASE (Command type)
	result, err := c.Execute(Statement{
		Database: database,
		Command:  "CREATE DATABASE " + database,
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, StatementTypeCommand, result.StatementType)
	assert.NotNil(t, result.QueryResult)
	assert.Equal(t, int64(1), result.AffectedRows) // Command statements affect 1 row
	assert.NoError(t, result.Error)

	// cleanup
	defer func() {
		err := c.DropDatabase(database)
		assert.NoError(t, err)
	}()
}

func TestClientExecuteInsertStatement(t *testing.T) {
	c := testDefaultClient(t)

	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	require.NoError(t, err)

	defer func() {
		err := c.DropDatabase(database)
		assert.NoError(t, err)
	}()

	// Test INSERT statement (Insert type)
	result, err := c.Execute(Statement{
		Database: database,
		Command:  "INSERT weather,location=beijing temperature=25.5,humidity=60i",
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, StatementTypeInsert, result.StatementType)
	assert.Nil(t, result.QueryResult) // Insert statements don't return query results
	assert.Equal(t, int64(1), result.AffectedRows)
	assert.NoError(t, result.Error)
}

func TestClientExecuteWithParams(t *testing.T) {
	c := testDefaultClient(t)

	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	require.NoError(t, err)

	defer func() {
		err := c.DropDatabase(database)
		assert.NoError(t, err)
	}()

	// Test parameterized INSERT
	result, err := c.Execute(Statement{
		Database: database,
		Command:  "INSERT weather,location=$location temperature=$temp,humidity=$humidity",
		Params: map[string]any{
			"location": "shanghai",
			"temp":     30.2,
			"humidity": 70,
		},
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, StatementTypeInsert, result.StatementType)
	assert.Equal(t, int64(1), result.AffectedRows)
	assert.NoError(t, result.Error)
}

func TestClientExecuteContext(t *testing.T) {
	c := testDefaultClient(t)

	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	require.NoError(t, err)

	defer func() {
		err := c.DropDatabase(database)
		assert.NoError(t, err)
	}()

	ctx := context.Background()

	// Test ExecuteContext
	result, err := c.ExecuteContext(ctx, Statement{
		Database: database,
		Command:  "SHOW MEASUREMENTS",
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, StatementTypeQuery, result.StatementType)
	assert.NoError(t, result.Error)
}

func TestClientExecuteContextWithTimeout(t *testing.T) {
	c := testDefaultClient(t)

	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	require.NoError(t, err)

	defer func() {
		err := c.DropDatabase(database)
		assert.NoError(t, err)
	}()

	// Test ExecuteContext with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// This test might not always timeout, but it tests the context passing
	result, err := c.ExecuteContext(ctx, Statement{
		Database: database,
		Command:  "SHOW MEASUREMENTS",
	})

	// Either success or timeout error are acceptable
	if err != nil {
		// If there's an error, it should be in the result as well
		assert.NotNil(t, result)
		assert.Error(t, result.Error)
	} else {
		assert.NoError(t, result.Error)
	}
}

func TestClientExecuteInvalidStatement(t *testing.T) {
	c := testDefaultClient(t)

	// Test missing database
	result, err := c.Execute(Statement{
		Command: "SELECT * FROM weather",
	})

	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, StatementTypeUnknown, result.StatementType)
	assert.Error(t, result.Error)

	// Test missing command
	result, err = c.Execute(Statement{
		Database: "test_db",
	})

	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, StatementTypeUnknown, result.StatementType)
	assert.Error(t, result.Error)
}

func TestClientExecuteUnsupportedStatement(t *testing.T) {
	c := testDefaultClient(t)

	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	require.NoError(t, err)

	defer func() {
		err := c.DropDatabase(database)
		assert.NoError(t, err)
	}()

	// Test unsupported statement (should be parsed as Unknown)
	result, err := c.Execute(Statement{
		Database: database,
		Command:  "UNSUPPORTED STATEMENT",
	})

	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, StatementTypeUnknown, result.StatementType)
	assert.Error(t, result.Error)
}

func TestClientExecuteWithInvalidParams(t *testing.T) {
	c := testDefaultClient(t)

	database := randomDatabaseName()

	// Test with invalid parameter
	result, err := c.Execute(Statement{
		Database: database,
		Command:  "INSERT weather,location=$location temperature=$temp",
		Params: map[string]any{
			"location": "beijing",
			"temp":     nil, // invalid parameter
		},
	})

	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Error(t, result.Error)
}
