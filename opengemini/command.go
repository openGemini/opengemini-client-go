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

import "fmt"

func (c *client) ShowTagKeys(builder ShowTagKeysBuilder) (map[string][]string, error) {
	command, err := builder.build()
	if err != nil {
		return nil, err
	}
	base := builder.getMeasurementBase()

	queryResult, err := c.queryPost(Query{
		Database:        base.database,
		RetentionPolicy: base.retentionPolicy,
		Command:         command,
	})

	if err != nil {
		return nil, err
	}

	err = queryResult.hasError()
	if err != nil {
		return nil, fmt.Errorf("show tag keys err: %s", err)
	}

	var data = make(map[string][]string)
	if len(queryResult.Results) == 0 {
		return data, nil
	}
	for _, series := range queryResult.Results[0].Series {
		var tags []string
		for _, values := range series.Values {
			for _, value := range values {
				strVal, ok := value.(string)
				if !ok {
					continue
				}
				tags = append(tags, strVal)
			}
		}
		data[series.Name] = tags
	}

	return data, nil
}

func (c *client) ShowTagValues(builder ShowTagValuesBuilder) ([]string, error) {
	command, err := builder.build()
	if err != nil {
		return nil, err
	}
	base := builder.getMeasurementBase()

	queryResult, err := c.queryPost(Query{
		Database:        base.database,
		RetentionPolicy: base.retentionPolicy,
		Command:         command,
	})

	if err != nil {
		return nil, err
	}

	err = queryResult.hasError()
	if err != nil {
		return nil, fmt.Errorf("show tag value err: %s", err)
	}

	var values []string
	if len(queryResult.Results) == 0 {
		return values, nil
	}

	querySeries := queryResult.Results[0].Series
	for _, series := range querySeries {
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

func (c *client) ShowFieldKeys(database string, measurements ...string) (map[string]map[string]string, error) {
	var measurement string
	if len(measurements) != 0 {
		measurement = measurements[0]
	}
	err := checkDatabaseName(database)
	if err != nil {
		return nil, err
	}

	var command = "SHOW FIELD KEYS"
	if measurement != "" {
		command += " FROM " + measurement
	}

	queryResult, err := c.Query(Query{Database: database, Command: command})
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

func (c *client) ShowSeries(builder ShowSeriesBuilder) ([]string, error) {
	command, err := builder.build()
	if err != nil {
		return nil, err
	}

	base := builder.getMeasurementBase()

	seriesResult, err := c.Query(Query{Database: base.database, RetentionPolicy: base.retentionPolicy, Command: command})
	if err != nil {
		return nil, err
	}

	err = seriesResult.hasError()
	if err != nil {
		return nil, fmt.Errorf("get series failed: %s", err)
	}

	var seriesValues = make([]string, 0, len(seriesResult.Results))
	if len(seriesResult.Results) == 0 {
		return seriesValues, nil
	}
	for _, series := range seriesResult.Results[0].Series {
		for _, values := range series.Values {
			for _, value := range values {
				strVal, ok := value.(string)
				if !ok {
					continue
				}
				seriesValues = append(seriesValues, strVal)
			}
		}
	}
	return seriesValues, nil
}
