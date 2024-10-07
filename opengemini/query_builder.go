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
			commandBuilder.WriteString(expr.build())
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
		commandBuilder.WriteString(q.where.build())
	}

	// Build the GROUP BY part
	if len(q.groupBy) > 0 {
		commandBuilder.WriteString(" GROUP BY ")
		for i, expr := range q.groupBy {
			if i > 0 {
				commandBuilder.WriteString(", ")
			}
			commandBuilder.WriteString(expr.build())
		}
	}

	// Build the ORDER BY part
	if q.order != "" {
		commandBuilder.WriteString(" ORDER BY time ")
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
