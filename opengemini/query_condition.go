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
	"fmt"
	"strings"
)

type Condition interface {
	build() string
}

type ComparisonCondition struct {
	Column   string
	Operator ComparisonOperator
	Value    interface{}
}

func (c *ComparisonCondition) build() string {
	switch c.Value.(type) {
	case string:
		return fmt.Sprintf(`"%s" %s '%v'`, c.Column, c.Operator, c.Value)
	default:
		return fmt.Sprintf(`"%s" %s %v`, c.Column, c.Operator, c.Value)
	}
}

func NewComparisonCondition(column string, operator ComparisonOperator, value interface{}) *ComparisonCondition {
	return &ComparisonCondition{
		Column:   column,
		Operator: operator,
		Value:    value,
	}
}

type CompositeCondition struct {
	LogicalOperator LogicalOperator
	Conditions      []Condition
}

func (c *CompositeCondition) build() string {
	var parts []string
	for _, condition := range c.Conditions {
		parts = append(parts, condition.build())
	}
	return fmt.Sprintf("(%s)", strings.Join(parts, fmt.Sprintf(" %s ", c.LogicalOperator)))
}

func NewCompositeCondition(logicalOperator LogicalOperator, conditions ...Condition) *CompositeCondition {
	return &CompositeCondition{
		LogicalOperator: logicalOperator,
		Conditions:      conditions,
	}
}
