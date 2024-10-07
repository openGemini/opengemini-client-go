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

type Expression interface {
	build() string
}

type AllowedConstantTypes interface {
	bool | int | int64 | float64 | string
}

type ConstantExpression[T AllowedConstantTypes] struct {
	Value T
}

func (c *ConstantExpression[T]) build() string {
	return fmt.Sprintf("%v", c.Value)
}

func NewConstantExpression[T AllowedConstantTypes](value T) *ConstantExpression[T] {
	return &ConstantExpression[T]{Value: value}
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

type AsExpression struct {
    Alias string
    OriginExpr Expression
}

func (a *AsExpression) build() string {
    return fmt.Sprintf("%s AS \"%s\"", a.OriginExpr.build(), a.Alias)
}

func NewAsExpression(alias string, expr Expression) *AsExpression {
    return &AsExpression{
        Alias : alias,
        OriginExpr: expr,
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
