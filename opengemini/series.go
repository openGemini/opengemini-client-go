package opengemini

type SeriesValue []interface{}

type SeriesValues []SeriesValue

// Series defines the structure for series data
type Series struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  SeriesValues      `json:"values,omitempty"`
}
