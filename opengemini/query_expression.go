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
	Left     Expression
	Operator ArithmeticOperator
	Right    Expression
}

func NewArithmeticExpression(left Expression, operator ArithmeticOperator, right Expression) *ArithmeticExpression {
	return &ArithmeticExpression{
		Left:     left,
		Operator: operator,
		Right:    right,
	}
}
