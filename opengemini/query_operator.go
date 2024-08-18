package opengemini

type ComparisonOperator string

const (
	Equals              ComparisonOperator = "="
	NotEquals           ComparisonOperator = "<>"
	GreaterThan         ComparisonOperator = ">"
	LessThan            ComparisonOperator = "<"
	GreaterThanOrEquals ComparisonOperator = ">="
	LessThanOrEquals    ComparisonOperator = "<="
	Match               ComparisonOperator = "=~"
	NotMatch            ComparisonOperator = "!~"
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
