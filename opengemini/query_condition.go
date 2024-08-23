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
