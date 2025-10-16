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
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// parseStatementType determines the category of SQL statement
func parseStatementType(command string) StatementType {
	cleaned := cleanCommand(command)
	words := strings.Fields(strings.ToUpper(cleaned))

	if len(words) == 0 {
		return StatementTypeUnknown
	}

	firstWord := words[0]

	if isQueryKeyword(firstWord) {
		return StatementTypeQuery
	}

	if isCommandKeyword(firstWord) {
		return StatementTypeCommand
	}

	if isInsertKeyword(firstWord) {
		return StatementTypeInsert
	}

	return StatementTypeUnknown
}

// isQueryKeyword checks if the keyword indicates a query statement
func isQueryKeyword(word string) bool {
	queryKeywords := []string{
		"SELECT",
		"SHOW",
		"EXPLAIN",
		"DESCRIBE",
		"DESC",
		"WITH",
	}

	for _, keyword := range queryKeywords {
		if word == keyword {
			return true
		}
	}
	return false
}

// isCommandKeyword checks if the keyword indicates a command statement
func isCommandKeyword(word string) bool {
	commandKeywords := []string{
		"CREATE",
		"DROP",
		"ALTER",
		"UPDATE", // UPDATE RETENTION POLICY
		"DELETE",
	}

	for _, keyword := range commandKeywords {
		if word == keyword {
			return true
		}
	}
	return false
}

// isInsertKeyword checks if the keyword indicates an insert statement
func isInsertKeyword(word string) bool {
	return word == "INSERT"
}

// cleanCommand removes comments and extra whitespace
func cleanCommand(command string) string {
	trimmed := strings.TrimSpace(command)

	// Handle single line comments (-- comment)
	if idx := strings.Index(trimmed, "--"); idx != -1 {
		trimmed = strings.TrimSpace(trimmed[:idx])
	}

	// Handle multiline comments (/* comment */)
	for strings.Contains(trimmed, "/*") {
		start := strings.Index(trimmed, "/*")
		end := strings.Index(trimmed, "*/")
		if start != -1 && end != -1 && end > start {
			// Just remove the comment, don't add extra space
			before := trimmed[:start]
			after := trimmed[end+2:]

			// If there's no space before comment and no space after comment,
			// and both sides have content, add a space to separate words
			needSpace := false
			if len(before) > 0 && len(after) > 0 {
				beforeEndsWithSpace := strings.HasSuffix(before, " ")
				afterStartsWithSpace := strings.HasPrefix(after, " ")
				if !beforeEndsWithSpace && !afterStartsWithSpace {
					needSpace = true
				}
			}

			if needSpace {
				trimmed = before + " " + after
			} else {
				trimmed = before + after
			}
		} else {
			break
		}
	}

	return strings.TrimSpace(trimmed)
}

// parseInsertStatement parses INSERT statement into Point objects
func parseInsertStatement(command string) ([]*Point, error) {
	// Remove INSERT prefix from command
	trimmed := strings.TrimSpace(command)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") {
		return nil, errors.New("not an INSERT statement")
	}

	// Extract Line Protocol part after INSERT keyword
	// Format: INSERT measurement,tag1=value1 field1=value1,field2=value2
	lpPart := strings.TrimSpace(trimmed[6:]) // Remove "INSERT"

	// Parse Line Protocol to Point object
	point, err := parseLineProtocolToPoint(lpPart)
	if err != nil {
		return nil, fmt.Errorf("invalid line protocol format: %w", err)
	}

	return []*Point{point}, nil
}

// parseLineProtocolToPoint converts line protocol string to Point
func parseLineProtocolToPoint(lp string) (*Point, error) {
	// Line Protocol format: measurement[,tag1=val1,tag2=val2] field1=val1[,field2=val2] [timestamp]

	// Separate measurement+tags and fields parts
	parts := strings.SplitN(lp, " ", 3)
	if len(parts) < 2 {
		return nil, errors.New("invalid line protocol format: missing fields")
	}

	measurementAndTags := parts[0]
	fieldsStr := parts[1]

	// Parse measurement name and tags
	tagParts := strings.Split(measurementAndTags, ",")
	measurement := tagParts[0]

	if measurement == "" {
		return nil, errors.New("measurement name is required")
	}

	point := &Point{
		Measurement: measurement,
		Tags:        make(map[string]string),
		Fields:      make(map[string]any),
	}

	// Parse tags (starting from second element)
	for i := 1; i < len(tagParts); i++ {
		kv := strings.SplitN(tagParts[i], "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid tag format: %s", tagParts[i])
		}
		point.Tags[kv[0]] = kv[1]
	}

	// Parse fields with type inference
	fieldParts := strings.Split(fieldsStr, ",")
	for _, fieldPart := range fieldParts {
		kv := strings.SplitN(strings.TrimSpace(fieldPart), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid field format: %s", fieldPart)
		}

		key := kv[0]
		valueStr := kv[1]

		// Type inference and conversion
		value, err := parseFieldValue(valueStr)
		if err != nil {
			return nil, fmt.Errorf("invalid field value for key '%s': %w", key, err)
		}

		point.Fields[key] = value
	}

	// Parse timestamp (if exists)
	if len(parts) == 3 {
		timestampStr := strings.TrimSpace(parts[2])
		if timestampStr != "" {
			timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp: %s", timestampStr)
			}
			point.Timestamp = timestamp
		}
	}

	return point, nil
}

// parseFieldValue parses field value with type inference
func parseFieldValue(valueStr string) (any, error) {
	valueStr = strings.TrimSpace(valueStr)

	// Integer type (ends with 'i' or 'I')
	if strings.HasSuffix(valueStr, "i") || strings.HasSuffix(valueStr, "I") {
		intStr := valueStr[:len(valueStr)-1]
		if val, err := strconv.ParseInt(intStr, 10, 64); err == nil {
			return val, nil
		}
	}

	// Unsigned integer type (ends with 'u' or 'U')
	if strings.HasSuffix(valueStr, "u") || strings.HasSuffix(valueStr, "U") {
		uintStr := valueStr[:len(valueStr)-1]
		if val, err := strconv.ParseUint(uintStr, 10, 64); err == nil {
			return val, nil
		}
	}

	// String type (enclosed in double quotes)
	if strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"") && len(valueStr) >= 2 {
		return valueStr[1 : len(valueStr)-1], nil
	}

	// Boolean type
	if valueStr == "true" || valueStr == "TRUE" || valueStr == "t" || valueStr == "T" {
		return true, nil
	}
	if valueStr == "false" || valueStr == "FALSE" || valueStr == "f" || valueStr == "F" {
		return false, nil
	}

	// Float type (default for numbers without suffix)
	if val, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return val, nil
	}

	// If no match found, treat as string (this might cause type conflicts in database)
	return valueStr, nil
}
