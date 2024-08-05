package opengemini

import (
	"errors"
	"fmt"
)

type ValuesResult struct {
	Measurement string
	Values      []interface{}
}

func (c *client) ShowTagKeys(database string, builder TagKeysBuilder) (map[string][]string, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}
	if builder == nil {
		return nil, errors.New("empty command")
	}
	tagKeyResult, err := c.showTagSeriesQuery(database, builder.Build())
	if err != nil {
		return nil, err
	}
	var data = make(map[string][]string)
	for _, result := range tagKeyResult {
		var tags []string
		for _, value := range result.Values {
			tags = append(tags, value.(string))
		}
		data[result.Measurement] = tags
	}
	return data, nil
}

func (c *client) ShowTagValues(database string, builder TagValuesBuilder) ([]string, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}
	if builder == nil {
		return nil, errors.New("empty command")
	}

	queryResult, err := c.Query(Query{Database: database, Command: builder.Build()})
	if err != nil {
		return nil, err
	}

	if queryResult.hasError() != nil {
		return nil, queryResult.hasError()
	}

	if len(queryResult.Results) == 0 {
		return []string{}, nil
	}

	var values []string
	querySeries := queryResult.Results[0].Series

	for _, series := range querySeries {
		if len(series.Values) != 2 {
			continue
		}
		for _, valRes := range series.Values {
			if len(valRes) != 2 {
				return []string{}, fmt.Errorf("invalid values: %s", valRes)
			}
			if strVal, ok := valRes[1].(string); ok {
				values = append(values, strVal)
			}
		}
	}

	return values, nil
}

func (c *client) ShowFieldKeys(database string, builder FieldKeysBuilder) (map[string]map[string]string, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}

	if builder == nil {
		return nil, errors.New("empty command")
	}

	queryResult, err := c.Query(Query{Database: database, Command: builder.Build()})
	if err != nil {
		return nil, err
	}

	if queryResult.hasError() != nil {
		return nil, queryResult.hasError()
	}

	if len(queryResult.Results) == 0 {
		return nil, nil
	}

	querySeries := queryResult.Results[0].Series
	var value = make(map[string]map[string]string, len(querySeries))

	for _, series := range querySeries {
		var kv = make(map[string]string, len(series.Values))
		for _, valRes := range series.Values {
			if len(valRes) != 2 {
				return nil, fmt.Errorf("invalid values: %s", valRes)
			}
			var k, v string
			if strVal, ok := valRes[0].(string); ok {
				k = strVal
			}
			if strVal, ok := valRes[1].(string); ok {
				v = strVal
			}
			kv[k] = v
		}
		value[series.Name] = kv
	}
	return value, nil
}

func (c *client) ShowSeries(database, command string) ([]string, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}
	seriesResult, err := c.showTagSeriesQuery(database, command)
	if err != nil {
		return nil, err
	}
	if len(seriesResult) == 0 {
		return []string{}, nil
	}
	var (
		values = seriesResult[0].Values
		series = make([]string, 0, len(values))
	)
	for _, v := range values {
		strV, ok := v.(string)
		if !ok {
			return series, nil
		}
		series = append(series, strV)
	}
	return series, nil
}
