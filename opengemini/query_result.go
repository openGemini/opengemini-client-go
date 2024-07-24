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

func (result *QueryResult) convertRetentionPolicy() []RetentionPolicy {
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
