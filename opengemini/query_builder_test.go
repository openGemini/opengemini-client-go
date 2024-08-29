package opengemini

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestQueryBuilderSelectAllFromTable(t *testing.T) {
	query := CreateQueryBuilder().From("h2o_feet").Build()

	expectedQuery := `SELECT * FROM "h2o_feet"`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectTopFromTable(t *testing.T) {
	qb := CreateQueryBuilder()

	topFunction := NewFunctionExpression(FunctionTop, NewFieldExpression("water_level"), NewConstantExpression(5))

	query := qb.Select(topFunction).From("h2o_feet").Build()

	expectedQuery := `SELECT TOP("water_level", 5) FROM "h2o_feet"`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectLastAndTagFromTable(t *testing.T) {
	qb := CreateQueryBuilder()

	lastFunction := NewFunctionExpression(FunctionLast, NewFieldExpression("water_level"))

	query := qb.Select(lastFunction, NewFieldExpression("location")).From("h2o_feet").Build()

	expectedQuery := `SELECT LAST("water_level"), "location" FROM "h2o_feet"`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWithArithmetic(t *testing.T) {
	qb := CreateQueryBuilder()

	waterLevelField := NewFieldExpression("water_level")

	multipliedByFour := NewArithmeticExpression(Multiply, waterLevelField, NewConstantExpression(4))

	addTwo := NewArithmeticExpression(Add, multipliedByFour, NewConstantExpression(2))

	query := qb.Select(addTwo).From("h2o_feet").Limit(10).Build()

	expectedQuery := `SELECT (("water_level" * 4) + 2) FROM "h2o_feet" LIMIT 10`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWhereCondition(t *testing.T) {
	qb := CreateQueryBuilder()

	condition := NewComparisonCondition("water_level", GreaterThan, 8)

	query := qb.From("h2o_feet").Where(condition).Build()

	expectedQuery := `SELECT * FROM "h2o_feet" WHERE "water_level" > 8`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWithComplexWhereCondition(t *testing.T) {
	qb := CreateQueryBuilder()

	locationCondition := NewComparisonCondition("location", NotEquals, "santa_monica")
	lowerWaterLevelCondition := NewComparisonCondition("water_level", LessThan, -0.57)
	higherWaterLevelCondition := NewComparisonCondition("water_level", GreaterThan, 9.95)

	waterLevelCondition := NewCompositeCondition(Or, lowerWaterLevelCondition, higherWaterLevelCondition)

	finalCondition := NewCompositeCondition(And, locationCondition, waterLevelCondition)

	query := qb.Select(NewFieldExpression("water_level")).From("h2o_feet").Where(finalCondition).Build()

	expectedQuery := `SELECT "water_level" FROM "h2o_feet" WHERE ("location" <> 'santa_monica' AND ("water_level" < -0.57 OR "water_level" > 9.95))`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWithGroupBy(t *testing.T) {
	qb := CreateQueryBuilder()

	meanFunction := NewFunctionExpression(FunctionMean, NewFieldExpression("water_level"))

	query := qb.Select(meanFunction).From("h2o_feet").GroupBy(NewFieldExpression("location")).Build()

	expectedQuery := `SELECT MEAN("water_level") FROM "h2o_feet" GROUP BY "location"`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWithTimeRangeAndGroupByTime(t *testing.T) {
	qb := CreateQueryBuilder()

	countFunction := NewFunctionExpression(FunctionCount, NewFieldExpression("water_level"))

	startTimeCondition := NewComparisonCondition("time", GreaterThanOrEquals, "2019-08-18T00:00:00Z")
	endTimeCondition := NewComparisonCondition("time", LessThanOrEquals, "2019-08-18T00:30:00Z")

	timeRangeCondition := NewCompositeCondition(And, startTimeCondition, endTimeCondition)

	groupByTime := NewFunctionExpression(FunctionTime, NewConstantExpression("12m"))

	query := qb.Select(countFunction).From("h2o_feet").Where(timeRangeCondition).GroupBy(groupByTime).Build()

	expectedQuery := `SELECT COUNT("water_level") FROM "h2o_feet" WHERE ("time" >= '2019-08-18T00:00:00Z' AND "time" <= '2019-08-18T00:30:00Z') GROUP BY TIME(12m)`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWithTimeRangeAndOrderBy(t *testing.T) {
	qb := CreateQueryBuilder()

	waterLevelField := NewFieldExpression("water_level")
	locationField := NewFieldExpression("location")

	startTimeCondition := NewComparisonCondition("time", GreaterThanOrEquals, "2019-08-18T00:00:00Z")
	endTimeCondition := NewComparisonCondition("time", LessThanOrEquals, "2019-08-18T00:30:00Z")

	timeRangeCondition := NewCompositeCondition(And, startTimeCondition, endTimeCondition)

	query := qb.Select(waterLevelField, locationField).
		From("h2o_feet").
		Where(timeRangeCondition).
		OrderBy(Desc).Build()

	expectedQuery := `SELECT "water_level", "location" FROM "h2o_feet" WHERE ("time" >= '2019-08-18T00:00:00Z' AND "time" <= '2019-08-18T00:30:00Z') ORDER BY time DESC`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWithTimeRangeGroupByAndOrderBy(t *testing.T) {
	qb := CreateQueryBuilder()

	countFunction := NewFunctionExpression(FunctionCount, NewFieldExpression("water_level"))

	startTimeCondition := NewComparisonCondition("time", GreaterThanOrEquals, "2019-08-18T00:00:00Z")
	endTimeCondition := NewComparisonCondition("time", LessThanOrEquals, "2019-08-18T00:30:00Z")

	timeRangeCondition := NewCompositeCondition(And, startTimeCondition, endTimeCondition)

	groupByTime := NewFunctionExpression(FunctionTime, NewConstantExpression("12m"))

	query := qb.Select(countFunction).
		From("h2o_feet").
		Where(timeRangeCondition).
		GroupBy(groupByTime).
		OrderBy(Desc).Build()

	expectedQuery := `SELECT COUNT("water_level") FROM "h2o_feet" WHERE ("time" >= '2019-08-18T00:00:00Z' AND "time" <= '2019-08-18T00:30:00Z') GROUP BY TIME(12m) ORDER BY time DESC`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWithLimitAndOffset(t *testing.T) {
	qb := CreateQueryBuilder()

	// Create expressions for the fields "water_level" and "location"
	waterLevelField := NewFieldExpression("water_level")
	locationField := NewFieldExpression("location")

	// Build the query with LIMIT and OFFSET
	query := qb.Select(waterLevelField, locationField).
		From("h2o_feet").
		Limit(3).
		Offset(3).
		Build()

	// Expected SQL query string
	expectedQuery := `SELECT "water_level", "location" FROM "h2o_feet" LIMIT 3 OFFSET 3`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWithWhereAndTimezone(t *testing.T) {
	qb := CreateQueryBuilder()

	waterLevelField := NewFieldExpression("water_level")

	locationCondition := NewComparisonCondition("location", Equals, "santa_monica")
	startTimeCondition := NewComparisonCondition("time", GreaterThanOrEquals, "2019-08-18T00:00:00Z")
	endTimeCondition := NewComparisonCondition("time", LessThanOrEquals, "2019-08-18T00:18:00Z")

	finalCondition := NewCompositeCondition(And, locationCondition, startTimeCondition, endTimeCondition)

	location, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)

	query := qb.Select(waterLevelField).
		From("h2o_feet").
		Where(finalCondition).
		Timezone(location).
		Build()

	expectedQuery := `SELECT "water_level" FROM "h2o_feet" WHERE ("location" = 'santa_monica' AND "time" >= '2019-08-18T00:00:00Z' AND "time" <= '2019-08-18T00:18:00Z') TZ('America/Chicago')`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWithAsExpression(t *testing.T) {
	qb := CreateQueryBuilder()

	waterLevelField := NewFieldExpression("water_level")

	locationCondition := NewComparisonCondition("location", Equals, "santa_monica")
	startTimeCondition := NewComparisonCondition("time", GreaterThanOrEquals, "2019-08-18T00:00:00Z")
	endTimeCondition := NewComparisonCondition("time", LessThanOrEquals, "2019-08-18T00:18:00Z")

	finalCondition := NewCompositeCondition(And, locationCondition, startTimeCondition, endTimeCondition)

	location, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)

        asWL := NewAsExpression("WL", waterLevelField)

	query := qb.Select(asWL).
		From("h2o_feet").
		Where(finalCondition).
		Timezone(location).
		Build()

	expectedQuery := `SELECT "water_level" AS "WL" FROM "h2o_feet" WHERE ("location" = 'santa_monica' AND "time" >= '2019-08-18T00:00:00Z' AND "time" <= '2019-08-18T00:18:00Z') TZ('America/Chicago')`

	require.Equal(t, expectedQuery, query.Command)
}

func TestQueryBuilderSelectWithAggregate(t *testing.T) {
	qb := CreateQueryBuilder()

	waterLevelField := NewFieldExpression("water_level")
	countWaterLevelField := NewFunctionExpression(FunctionCount, waterLevelField)

	locationCondition := NewComparisonCondition("location", Equals, "santa_monica")
	startTimeCondition := NewComparisonCondition("time", GreaterThanOrEquals, "2019-08-18T00:00:00Z")
	endTimeCondition := NewComparisonCondition("time", LessThanOrEquals, "2019-08-18T00:18:00Z")

	finalCondition := NewCompositeCondition(And, locationCondition, startTimeCondition, endTimeCondition)

	location, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)

        asWL := NewAsExpression("WL", countWaterLevelField)

	query := qb.Select(asWL).
		From("h2o_feet").
		Where(finalCondition).
		Timezone(location).
		Build()

	expectedQuery := `SELECT COUNT("water_level") AS "WL" FROM "h2o_feet" WHERE ("location" = 'santa_monica' AND "time" >= '2019-08-18T00:00:00Z' AND "time" <= '2019-08-18T00:18:00Z') TZ('America/Chicago')`

	require.Equal(t, expectedQuery, query.Command)
}
