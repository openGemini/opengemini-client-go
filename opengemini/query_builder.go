package opengemini

import (
	"fmt"
	"strings"
	"time"
)

type QueryBuilder struct {
	selectExprs []Expression
	from        []string
	where       Condition
	groupBy     []Expression
	order       SortOrder
	limit       int64
	offset      int64
	timezone    *time.Location
}

func CreateQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func (q *QueryBuilder) Select(selectExpressions ...Expression) *QueryBuilder {
	q.selectExprs = selectExpressions
	return q
}

func (q *QueryBuilder) From(tables ...string) *QueryBuilder {
	q.from = tables
	return q
}

func (q *QueryBuilder) Where(condition Condition) *QueryBuilder {
	q.where = condition
	return q
}

func (q *QueryBuilder) GroupBy(groupByExpressions ...Expression) *QueryBuilder {
	q.groupBy = groupByExpressions
	return q
}

func (q *QueryBuilder) OrderBy(order SortOrder) *QueryBuilder {
	q.order = order
	return q
}

func (q *QueryBuilder) Limit(limit int64) *QueryBuilder {
	q.limit = limit
	return q
}

func (q *QueryBuilder) Offset(offset int64) *QueryBuilder {
	q.offset = offset
	return q
}

func (q *QueryBuilder) Timezone(location *time.Location) *QueryBuilder {
	q.timezone = location
	return q
}

func (q *QueryBuilder) Build() *Query {
	var commandBuilder strings.Builder

	// Build the SELECT part
	if len(q.selectExprs) > 0 {
		commandBuilder.WriteString("SELECT ")
		for i, expr := range q.selectExprs {
			if i > 0 {
				commandBuilder.WriteString(", ")
			}
			commandBuilder.WriteString(q.buildExpression(expr))
		}
	} else {
		commandBuilder.WriteString("SELECT *")
	}

	// Build the FROM part
	if len(q.from) > 0 {
		commandBuilder.WriteString(" FROM ")
		quotedTables := make([]string, len(q.from))
		for i, table := range q.from {
			quotedTables[i] = `"` + table + `"`
		}
		commandBuilder.WriteString(strings.Join(quotedTables, ", "))
	}

	// Build the WHERE part
	if q.where != nil {
		commandBuilder.WriteString(" WHERE ")
		commandBuilder.WriteString(q.buildCondition(q.where))
	}

	// Build the GROUP BY part
	if len(q.groupBy) > 0 {
		commandBuilder.WriteString(" GROUP BY ")
		for i, expr := range q.groupBy {
			if i > 0 {
				commandBuilder.WriteString(", ")
			}
			commandBuilder.WriteString(q.buildExpression(expr))
		}
	}

	// Build the ORDER BY part
	if q.order != "" {
		commandBuilder.WriteString(" ORDER BY ")
		commandBuilder.WriteString(string(q.order))
	}

	// Build the LIMIT part
	if q.limit > 0 {
		commandBuilder.WriteString(fmt.Sprintf(" LIMIT %d", q.limit))
	}

	// Build the OFFSET part
	if q.offset > 0 {
		commandBuilder.WriteString(fmt.Sprintf(" OFFSET %d", q.offset))
	}

	// Build the TIMEZONE part
	if q.timezone != nil {
		commandBuilder.WriteString(fmt.Sprintf(" TZ('%s')", q.timezone.String()))
	}

	// Return the final query
	return &Query{
		Command: commandBuilder.String(),
	}
}

func (q *QueryBuilder) buildExpression(expr Expression) string {
	return expr.build()
}

func (q *QueryBuilder) buildCondition(cond Condition) string {
	switch c := cond.(type) {
	case *ComparisonCondition:
		return fmt.Sprintf(`"%s" %s '%v'`, c.Column, c.Operator, c.Value)
	case *CompositeCondition:
		var parts []string
		for _, condition := range c.Conditions {
			parts = append(parts, q.buildCondition(condition))
		}
		return fmt.Sprintf("(%s)", strings.Join(parts, fmt.Sprintf(" %s ", c.LogicalOperator)))
	default:
		return ""
	}
}
