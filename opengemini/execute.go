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
	"context"
	"fmt"
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

	switch {
	case stmtType.IsQueryLike():
		return c.routeToQuery(ctx, stmt, stmtType)

	case stmtType.IsWriteLike():
		return c.routeToWrite(ctx, stmt, stmtType)

	default:
		err := fmt.Errorf("unsupported statement type: %s", stmtType)
		return &ExecuteResult{
			StatementType: stmtType,
			Error:         err,
		}, err
	}
}

func (c *client) routeToQuery(ctx context.Context, stmt Statement, stmtType StatementType) (*ExecuteResult, error) {
	query := Query{
		Database:        stmt.Database,
		Command:         stmt.Command,
		RetentionPolicy: stmt.RetentionPolicy,
		Params:          stmt.Params,
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
func (c *client) routeToWrite(ctx context.Context, stmt Statement, stmtType StatementType) (*ExecuteResult, error) {
	points, err := parseInsertStatement(stmt.Command)
	if err != nil {
		return &ExecuteResult{
			StatementType: stmtType,
			Error:         fmt.Errorf("failed to parse INSERT statement: %w", err),
		}, err
	}

	// Replace parameters with structured values for each point if params exist
	if len(stmt.Params) > 0 {
		for i, point := range points {
			if err := replacePointParams(point, stmt.Params); err != nil {
				return &ExecuteResult{
					StatementType: stmtType,
					Error:         fmt.Errorf("failed to replace parameters for point %d: %w", i+1, err),
				}, err
			}
		}
	}

	// Use batch write method (supports both single and multiple points)
	err = c.WriteBatchPointsWithRp(ctx, stmt.Database, stmt.RetentionPolicy, points)
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
		return ErrEmptyDatabaseName
	}
	if statement.Command == "" {
		return ErrEmptyCommand
	}
	return nil
}
