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
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Parser states for line protocol parsing
const (
	parseStateMeasurement = iota // Parsing measurement name
	parseStateTagKey             // Parsing tag key
	parseStateTagValue           // Parsing tag value
	parseStateFieldKey           // Parsing field key
	parseStateFieldValue         // Parsing field value
	parseStateTimestamp          // Parsing timestamp
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

// Cached keyword maps for O(1) lookup complexity
var (
	queryKeywords = map[string]bool{
		"SELECT":   true,
		"SHOW":     true,
		"EXPLAIN":  true,
		"DESCRIBE": true,
		"DESC":     true,
		"WITH":     true,
	}

	commandKeywords = map[string]bool{
		"CREATE": true,
		"DROP":   true,
		"ALTER":  true,
		"UPDATE": true,
		"DELETE": true,
	}
)

// isQueryKeyword checks if the keyword indicates a query statement
func isQueryKeyword(word string) bool {
	return queryKeywords[word]
}

// isCommandKeyword checks if the keyword indicates a command statement
func isCommandKeyword(word string) bool {
	return commandKeywords[word]
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
	trimmed := strings.TrimSpace(command)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") {
		return nil, errors.New("not an INSERT statement")
	}

	// Remove INSERT keyword
	lpPart := strings.TrimSpace(trimmed[6:])

	// Split by newline to support multiple Line Protocol entries
	lines := strings.Split(lpPart, "\n")
	points := make([]*Point, 0, len(lines))

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue // Skip empty lines
		}

		point, err := parseLineProtocolToPoint(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		points = append(points, point)
	}

	if len(points) == 0 {
		return nil, errors.New("no valid data points found in INSERT statement")
	}

	return points, nil
}

// parseLineProtocolToPoint converts line protocol string to Point with escape character support
func parseLineProtocolToPoint(lp string) (*Point, error) {
	// Line Protocol format: measurement[,tag1=val1,tag2=val2] field1=val1[,field2=val2] [timestamp]

	point := &Point{
		Tags:   make(map[string]string),
		Fields: make(map[string]any),
	}

	state := parseStateMeasurement
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
		if ch == '"' && !escape && (state == parseStateFieldValue || state == parseStateFieldKey) {
			if inQuote {
				inQuote = false
			} else {
				inQuote = true
			}
			continue
		}

		// Handle special characters based on state
		switch state {
		case parseStateMeasurement:
			if ch == ',' && !escape {
				point.Measurement = measurement.String()
				if point.Measurement == "" {
					return nil, errors.New("measurement name is required")
				}
				state = parseStateTagKey
				continue
			} else if ch == ' ' && !escape {
				point.Measurement = measurement.String()
				if point.Measurement == "" {
					return nil, errors.New("measurement name is required")
				}
				state = parseStateFieldKey
				continue
			}
			measurement.WriteByte(ch)

		case parseStateTagKey:
			if ch == '=' && !escape {
				state = parseStateTagValue
				continue
			}
			currentKey.WriteByte(ch)

		case parseStateTagValue:
			if ch == ',' && !escape {
				// Save current tag and start next tag key
				point.Tags[currentKey.String()] = currentValue.String()
				currentKey.Reset()
				currentValue.Reset()
				state = parseStateTagKey
				continue
			} else if ch == ' ' && !escape {
				// Save last tag and move to fields
				point.Tags[currentKey.String()] = currentValue.String()
				currentKey.Reset()
				currentValue.Reset()
				state = parseStateFieldKey
				continue
			}
			currentValue.WriteByte(ch)

		case parseStateFieldKey:
			if ch == '=' && !escape && !inQuote {
				state = parseStateFieldValue
				continue
			}
			currentKey.WriteByte(ch)

		case parseStateFieldValue:
			if ch == ',' && !escape && !inQuote {
				// Save current field and start next field key
				value, err := parseFieldValue(currentValue.String())
				if err != nil {
					return nil, fmt.Errorf("invalid field value for key '%s': %w", currentKey.String(), err)
				}
				point.Fields[currentKey.String()] = value
				currentKey.Reset()
				currentValue.Reset()
				state = parseStateFieldKey
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
				state = parseStateTimestamp
				continue
			}
			currentValue.WriteByte(ch)

		case parseStateTimestamp:
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
	if state == parseStateTagValue && currentKey.Len() > 0 {
		point.Tags[currentKey.String()] = currentValue.String()
	} else if state == parseStateFieldValue && currentKey.Len() > 0 {
		value, err := parseFieldValue(currentValue.String())
		if err != nil {
			return nil, fmt.Errorf("invalid field value for key '%s': %w", currentKey.String(), err)
		}
		point.Fields[currentKey.String()] = value
	} else if state == parseStateTimestamp && currentValue.Len() > 0 {
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

// replacePointParams performs structured parameter replacement on a Point object
func replacePointParams(point *Point, params map[string]any) error {
	// 1. Replace placeholders in measurement
	if strings.Contains(point.Measurement, "$") {
		replaced, err := replaceStringParam(point.Measurement, params)
		if err != nil {
			return fmt.Errorf("measurement: %w", err)
		}
		point.Measurement = replaced
	}

	// 2. Replace placeholders in Tags (both key and value can contain placeholders)
	newTags := make(map[string]string)
	for key, value := range point.Tags {
		newKey := key
		newValue := value

		// Replace tag key
		if strings.Contains(key, "$") {
			replaced, err := replaceStringParam(key, params)
			if err != nil {
				return fmt.Errorf("tag key '%s': %w", key, err)
			}
			newKey = replaced
		}

		// Replace tag value
		if strings.Contains(value, "$") {
			replaced, err := replaceStringParam(value, params)
			if err != nil {
				return fmt.Errorf("tag '%s' value: %w", key, err)
			}
			newValue = replaced
		}

		newTags[newKey] = newValue
	}
	point.Tags = newTags

	// 3. Replace placeholders in Fields
	newFields := make(map[string]interface{})
	for key, value := range point.Fields {
		newKey := key
		newValue := value

		// Replace field key
		if strings.Contains(key, "$") {
			replaced, err := replaceStringParam(key, params)
			if err != nil {
				return fmt.Errorf("field key '%s': %w", key, err)
			}
			newKey = replaced
		}

		// Replace field value
		// Check if value is a placeholder string
		if strValue, ok := value.(string); ok {
			if strings.HasPrefix(strValue, "$") && isPlaceholder(strValue) {
				// Entire value is a placeholder, e.g., "$temp"
				paramName := strings.TrimPrefix(strValue, "$")
				if paramValue, exists := params[paramName]; exists {
					newValue = paramValue // Use parameter value directly, preserving type
				} else {
					return fmt.Errorf("parameter '$%s' not found", paramName)
				}
			} else if strings.Contains(strValue, "$") {
				// Value contains placeholders, e.g., "prefix_$var_suffix"
				replaced, err := replaceStringParam(strValue, params)
				if err != nil {
					return fmt.Errorf("field '%s' value: %w", key, err)
				}
				newValue = replaced
			}
		}

		newFields[newKey] = newValue
	}
	point.Fields = newFields

	return nil
}

// replaceStringParam replaces all $paramName placeholders in a string
func replaceStringParam(input string, params map[string]any) (string, error) {
	result := input

	// Replace all placeholders
	for paramName, paramValue := range params {
		placeholder := "$" + paramName
		if strings.Contains(result, placeholder) {
			valueStr := formatParamValueAsString(paramValue)
			result = strings.ReplaceAll(result, placeholder, valueStr)
		}
	}

	// Check if there are any unresolved placeholders
	if strings.Contains(result, "$") {
		remaining := extractUnresolvedParams(result)
		if len(remaining) > 0 {
			return "", fmt.Errorf("unresolved parameters: %v", remaining)
		}
	}

	return result, nil
}

// formatParamValueAsString converts parameter value to string (for tags/measurement)
func formatParamValueAsString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%g", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// isPlaceholder checks if a string is a single placeholder (e.g., "$temp")
func isPlaceholder(s string) bool {
	if !strings.HasPrefix(s, "$") {
		return false
	}
	// Check if all characters after $ are valid parameter name characters
	paramName := s[1:]
	if len(paramName) == 0 {
		return false
	}
	for _, ch := range paramName {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') || ch == '_') {
			return false
		}
	}
	return true
}

// extractUnresolvedParams extracts all unresolved $paramName placeholders from a string
func extractUnresolvedParams(s string) []string {
	var unresolved []string
	parts := strings.Split(s, "$")

	for i := 1; i < len(parts); i++ {
		paramName := ""
		for _, ch := range parts[i] {
			if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
				(ch >= '0' && ch <= '9') || ch == '_' {
				paramName += string(ch)
			} else {
				break
			}
		}
		if paramName != "" {
			placeholder := "$" + paramName
			// Avoid duplicate additions
			found := false
			for _, existing := range unresolved {
				if existing == placeholder {
					found = true
					break
				}
			}
			if !found {
				unresolved = append(unresolved, placeholder)
			}
		}
	}

	return unresolved
}
