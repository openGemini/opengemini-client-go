package opengemini

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
