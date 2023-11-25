package opengemini

import "errors"

// SeriesResult contains the results of a series query
type SeriesResult struct {
	Series []Series `json:"series,omitempty"`
	Error  string   `json:"error,omitempty"`
}

// QueryResult is the top-level struct
type QueryResult struct {
	Results []SeriesResult `json:"results,omitempty"`
	Error   string         `json:"error,omitempty"`
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
