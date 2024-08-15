package opengemini

type ComparisonOperator string

const (
	Equals              ComparisonOperator = "EQUALS"
	NotEquals           ComparisonOperator = "NOT_EQUALS"
	GreaterThan         ComparisonOperator = "GREATER_THAN"
	LessThan            ComparisonOperator = "LESS_THAN"
	GreaterThanOrEquals ComparisonOperator = "GREATER_THAN_OR_EQUALS"
	LessThanOrEquals    ComparisonOperator = "LESS_THAN_OR_EQUALS"
)

type LogicalOperator string

const (
	And LogicalOperator = "AND"
	Or  LogicalOperator = "OR"
)

type ArithmeticOperator string

const (
	Add      ArithmeticOperator = "+"
	Subtract ArithmeticOperator = "-"
	Multiply ArithmeticOperator = "*"
	Divide   ArithmeticOperator = "/"
)
