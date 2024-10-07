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

import "errors"

const RpColumnLen = 8

// SeriesResult contains the results of a series query
type SeriesResult struct {
	Series []*Series `json:"series,omitempty"`
	Error  string    `json:"error,omitempty"`
}

// QueryResult is the top-level struct
type QueryResult struct {
	Results []*SeriesResult `json:"results,omitempty"`
	Error   string          `json:"error,omitempty"`
}

func (result *QueryResult) hasError() error {
	if len(result.Error) > 0 {
		return errors.New(result.Error)
	}
	for _, res := range result.Results {
		if len(res.Error) > 0 {
			return errors.New(res.Error)
		}
	}
	return nil
}

func (result *QueryResult) convertRetentionPolicyList() []RetentionPolicy {
	if len(result.Results) == 0 || len(result.Results[0].Series) == 0 {
		return []RetentionPolicy{}
	}
	var (
		seriesValues    = result.Results[0].Series[0].Values
		retentionPolicy = make([]RetentionPolicy, 0, len(seriesValues))
	)

	for _, v := range seriesValues {
		if len(v) < RpColumnLen {
			break
		}
		if rp := NewRetentionPolicy(v); rp != nil {
			retentionPolicy = append(retentionPolicy, *rp)
		}
	}
	return retentionPolicy
}

func (result *QueryResult) convertMeasurements() []string {
	if len(result.Results) == 0 || len(result.Results[0].Series) == 0 {
		return []string{}
	}
	var (
		seriesValues = result.Results[0].Series[0].Values
		measurements = make([]string, 0, len(seriesValues))
	)

	for _, v := range seriesValues {
		if measurementName, ok := v[0].(string); ok {
			measurements = append(measurements, measurementName)
		}
	}
	return measurements
}
