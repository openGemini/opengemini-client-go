package opengemini

type Expression interface{}

type ConstantExpression struct {
	Value interface{}
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

func NewFieldExpression(field string) *FieldExpression {
	return &FieldExpression{
		Field: field,
	}
}

type FunctionExpression struct {
	Function  FunctionEnum
	Arguments []Expression
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

func NewArithmeticExpression(operator ArithmeticOperator, operands ...Expression) *ArithmeticExpression {
	return &ArithmeticExpression{
		Operator: operator,
		Operands: operands,
	}
}
