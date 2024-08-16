package opengemini

type Condition interface{}

type ComparisonCondition struct {
	Column   string
	Operator ComparisonOperator
	Value    interface{}
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

func NewCompositeCondition(logicalOperator LogicalOperator, conditions ...Condition) *CompositeCondition {
	return &CompositeCondition{
		LogicalOperator: logicalOperator,
		Conditions:      conditions,
	}
}
