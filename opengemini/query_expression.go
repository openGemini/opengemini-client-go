package opengemini

import (
	"fmt"
	"strings"
)

type Expression interface {
	build() string
}

type ConstantExpression struct {
	Value interface{}
}

func (c *ConstantExpression) build() string {
	return fmt.Sprintf("%v", c.Value)
}

func NewConstantExpression(value interface{}) *ConstantExpression {
	return &ConstantExpression{
		Value: value,
	}
}

type StarExpression struct{}

type FieldExpression struct {
	Field string
}

func (f *FieldExpression) build() string {
	return `"` + f.Field + `"`
}

func NewFieldExpression(field string) *FieldExpression {
	return &FieldExpression{
		Field: field,
	}
}

type FunctionExpression struct {
	Function  FunctionEnum
	Arguments []Expression
}

func (f *FunctionExpression) build() string {
	var args []string
	for _, arg := range f.Arguments {
		args = append(args, arg.build())
	}
	return fmt.Sprintf("%s(%s)", f.Function, strings.Join(args, ", "))
}

func NewFunctionExpression(function FunctionEnum, arguments ...Expression) *FunctionExpression {
	return &FunctionExpression{
		Function:  function,
		Arguments: arguments,
	}
}

type ArithmeticExpression struct {
	Operator ArithmeticOperator
	Operands []Expression
}

func (a *ArithmeticExpression) build() string {
	var operandStrings []string
	for _, operand := range a.Operands {
		operandStrings = append(operandStrings, operand.build())
	}
	return fmt.Sprintf("(%s)", strings.Join(operandStrings, fmt.Sprintf(" %s ", a.Operator)))
}

func NewArithmeticExpression(operator ArithmeticOperator, operands ...Expression) *ArithmeticExpression {
	return &ArithmeticExpression{
		Operator: operator,
		Operands: operands,
	}
}
