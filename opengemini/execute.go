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
	"errors"
	"fmt"
	"log"
	"strings"
)

// StatementType represents the category of SQL statement
type StatementType int

const (
	StatementTypeUnknown StatementType = iota
	StatementTypeQuery                 // SELECT, SHOW, EXPLAIN query statements → routed to Query()
	StatementTypeCommand               // CREATE, DROP, ALTER command statements → routed to Query()
	StatementTypeInsert                // INSERT write statements → routed to Write methods
)

func (s StatementType) String() string {
	switch s {
	case StatementTypeQuery:
		return "Query"
	case StatementTypeCommand:
		return "Command"
	case StatementTypeInsert:
		return "Insert"
	default:
		return "Unknown"
	}
}

// IsQueryLike returns true if the statement should be routed to Query method
func (s StatementType) IsQueryLike() bool {
	return s == StatementTypeQuery || s == StatementTypeCommand
}

// IsWriteLike returns true if the statement should be routed to Write methods
func (s StatementType) IsWriteLike() bool {
	return s == StatementTypeInsert
}

type Statement struct {
	Database        string
	Command         string
	Params          map[string]any
	RetentionPolicy string
}

// ExecuteResult represents the result of Execute operation
type ExecuteResult struct {
	QueryResult   *QueryResult  // Result for query/command statements (populated for Query/Command types)
	AffectedRows  int64         // Number of rows affected by write statements (populated for Insert type)
	StatementType StatementType // Type of executed statement
	Error         error         // Execution error (if any)
}

// Execute executes a SQL-like statement with automatic routing
func (c *client) Execute(stmt Statement) (*ExecuteResult, error) {
	return c.ExecuteContext(context.Background(), stmt)
}

func (c *client) ExecuteContext(ctx context.Context, stmt Statement) (*ExecuteResult, error) {
	if err := validateStatement(stmt); err != nil {
		return &ExecuteResult{
			StatementType: StatementTypeUnknown,
			Error:         err,
		}, err
	}

	stmtType := parseStatementType(stmt.Command)

	finalCommand := stmt.Command
	if len(stmt.Params) > 0 {
		var err error
		finalCommand, err = replaceParams(stmt.Command, stmt.Params)
		if err != nil {
			return &ExecuteResult{
				StatementType: stmtType,
				Error:         fmt.Errorf("parameter replacement failed: %w", err),
			}, err
		}
	}

	switch {
	case stmtType.IsQueryLike():
		return c.routeToQuery(ctx, stmt, finalCommand, stmtType)

	case stmtType.IsWriteLike():
		return c.routeToWrite(ctx, stmt, finalCommand, stmtType)

	default:
		err := fmt.Errorf("unsupported statement type: %s", stmtType)
		return &ExecuteResult{
			StatementType: stmtType,
			Error:         err,
		}, err
	}
}

func (c *client) routeToQuery(ctx context.Context, stmt Statement, finalCommand string, stmtType StatementType) (*ExecuteResult, error) {
	query := Query{
		Database:        stmt.Database,
		Command:         finalCommand,
		RetentionPolicy: stmt.RetentionPolicy,
	}

	queryResult, err := c.Query(query)
	if err != nil {
		return &ExecuteResult{
			StatementType: stmtType,
			Error:         err,
		}, err
	}

	affectedRows := int64(0)
	if stmtType == StatementTypeCommand {
		affectedRows = 1
	}

	return &ExecuteResult{
		QueryResult:   queryResult,
		StatementType: stmtType,
		AffectedRows:  affectedRows,
	}, nil
}

// routeToWrite routes INSERT statements to existing Write methods
func (c *client) routeToWrite(ctx context.Context, stmt Statement, command string, stmtType StatementType) (*ExecuteResult, error) {
	points, err := parseInsertStatement(command)
	if err != nil {
		return &ExecuteResult{
			StatementType: stmtType,
			Error:         fmt.Errorf("failed to parse INSERT statement: %w", err),
		}, err
	}

	// Debug: log parsed points for troubleshooting
	log.Println("parsed points count:", len(points))
	for i, point := range points {
		log.Printf("point[%d]: measurement=%s, tags=%v, fields=%v", i, point.Measurement, point.Tags, point.Fields)
	}

	// Call existing write methods
	if len(points) == 1 {
		// Single point write
		err = c.WritePointWithRp(stmt.Database, stmt.RetentionPolicy, points[0], CallbackDummy)
	} else {
		// Batch points write
		err = c.WriteBatchPointsWithRp(ctx, stmt.Database, stmt.RetentionPolicy, points)
	}

	if err != nil {
		return &ExecuteResult{
			StatementType: stmtType,
			Error:         err,
		}, err
	}

	return &ExecuteResult{
		StatementType: stmtType,
		AffectedRows:  int64(len(points)),
	}, nil
}

// validateStatement performs basic validation on the statement
func validateStatement(statement Statement) error {
	if statement.Database == "" {
		return errors.New("database name is required")
	}
	if statement.Command == "" {
		return errors.New("command is required")
	}
	return nil
}

// replaceParams safely replaces parameters in the command string
func replaceParams(command string, params map[string]any) (string, error) {
	result := command

	for key, value := range params {
		placeholder := "$" + key
		if !strings.Contains(result, placeholder) {
			continue
		}
		replacement, err := convertParamValue(value)
		if err != nil {
			return "", fmt.Errorf("invalid parameter '%s': %w", key, err)
		}
		result = strings.ReplaceAll(result, placeholder, replacement)
	}

	if strings.Contains(result, "$") {
		return "", errors.New("unresolved parameters found in command")
	}

	return result, nil
}

// convertParamValue converts parameter value to string representation
func convertParamValue(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil

	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%di", v), nil

	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%du", v), nil

	case float32, float64:
		return fmt.Sprintf("%g", v), nil

	case bool:
		return fmt.Sprintf("%t", v), nil

	case nil:
		return "", errors.New("nil value not allowed")

	default:
		// For other types, try to convert to string
		return fmt.Sprintf("%v", v), nil
	}
}
