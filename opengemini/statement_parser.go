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

// parseLineProtocolToPoint converts line protocol string to Point with escape character support
func parseLineProtocolToPoint(lp string) (*Point, error) {
	// Line Protocol format: measurement[,tag1=val1,tag2=val2] field1=val1[,field2=val2] [timestamp]

	point := &Point{
		Tags:   make(map[string]string),
		Fields: make(map[string]any),
	}

	// States: 0=measurement, 1=tagKey, 2=tagValue, 3=fieldKey, 4=fieldValue, 5=timestamp
	state := 0
	var measurement strings.Builder
	var currentKey strings.Builder
	var currentValue strings.Builder
	escape := false
	inQuote := false

	for i := 0; i < len(lp); i++ {
		ch := lp[i]

		// Handle escape character
		if ch == '\\' && !escape {
			escape = true
			continue
		}

		// Handle quotes (for field values)
		if ch == '"' && !escape && (state == 4 || state == 3) {
			if inQuote {
				inQuote = false
			} else {
				inQuote = true
			}
			continue
		}

		// Handle special characters based on state
		switch state {
		case 0: // Parsing measurement
			if ch == ',' && !escape {
				point.Measurement = measurement.String()
				if point.Measurement == "" {
					return nil, errors.New("measurement name is required")
				}
				state = 1 // Move to tag key
				continue
			} else if ch == ' ' && !escape {
				point.Measurement = measurement.String()
				if point.Measurement == "" {
					return nil, errors.New("measurement name is required")
				}
				state = 3 // Move to field key
				continue
			}
			measurement.WriteByte(ch)

		case 1: // Parsing tag key
			if ch == '=' && !escape {
				state = 2 // Move to tag value
				continue
			}
			currentKey.WriteByte(ch)

		case 2: // Parsing tag value
			if ch == ',' && !escape {
				// Save current tag and start next tag key
				point.Tags[currentKey.String()] = currentValue.String()
				currentKey.Reset()
				currentValue.Reset()
				state = 1 // Back to tag key
				continue
			} else if ch == ' ' && !escape {
				// Save last tag and move to fields
				point.Tags[currentKey.String()] = currentValue.String()
				currentKey.Reset()
				currentValue.Reset()
				state = 3 // Move to field key
				continue
			}
			currentValue.WriteByte(ch)

		case 3: // Parsing field key
			if ch == '=' && !escape && !inQuote {
				state = 4 // Move to field value
				continue
			}
			currentKey.WriteByte(ch)

		case 4: // Parsing field value
			if ch == ',' && !escape && !inQuote {
				// Save current field and start next field key
				value, err := parseFieldValue(currentValue.String())
				if err != nil {
					return nil, fmt.Errorf("invalid field value for key '%s': %w", currentKey.String(), err)
				}
				point.Fields[currentKey.String()] = value
				currentKey.Reset()
				currentValue.Reset()
				state = 3 // Back to field key
				continue
			} else if ch == ' ' && !escape && !inQuote {
				// Save last field and move to timestamp
				value, err := parseFieldValue(currentValue.String())
				if err != nil {
					return nil, fmt.Errorf("invalid field value for key '%s': %w", currentKey.String(), err)
				}
				point.Fields[currentKey.String()] = value
				currentKey.Reset()
				currentValue.Reset()
				state = 5 // Move to timestamp
				continue
			}
			currentValue.WriteByte(ch)

		case 5: // Parsing timestamp
			if ch >= '0' && ch <= '9' {
				currentValue.WriteByte(ch)
			} else {
				// Invalid character in timestamp
				return nil, fmt.Errorf("invalid timestamp: unexpected character '%c' at position %d", ch, i)
			}
		}

		escape = false
	}

	// Handle remaining data
	if state == 2 && currentKey.Len() > 0 {
		point.Tags[currentKey.String()] = currentValue.String()
	} else if state == 4 && currentKey.Len() > 0 {
		value, err := parseFieldValue(currentValue.String())
		if err != nil {
			return nil, fmt.Errorf("invalid field value for key '%s': %w", currentKey.String(), err)
		}
		point.Fields[currentKey.String()] = value
	} else if state == 5 && currentValue.Len() > 0 {
		timestamp, err := strconv.ParseInt(currentValue.String(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp: %s", currentValue.String())
		}
		point.Timestamp = timestamp
	}

	// Validate required fields
	if point.Measurement == "" {
		return nil, errors.New("measurement name is required")
	}
	if len(point.Fields) == 0 {
		return nil, errors.New("at least one field is required")
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
