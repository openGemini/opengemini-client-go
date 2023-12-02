package opengemini

import (
	"errors"
)

type ValuesResult struct {
	Measurement string
	Values      []interface{}
}

func (c *client) ShowTagKeys(database, command string) ([]ValuesResult, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}
	tagKeyResult, err := c.showTagSeriesQuery(database, command)
	if err != nil {
		return nil, err
	}
	return tagKeyResult, nil
}

func (c *client) ShowTagValues(database, command string) ([]ValuesResult, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}
	if len(command) == 0 {
		return nil, errors.New("empty command")
	}

	tagValueResult, err := c.showTagFieldQuery(database, command)
	if err != nil {
		return nil, err
	}
	return tagValueResult, nil
}

func (c *client) ShowFieldKeys(database, command string) ([]ValuesResult, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}

	if len(command) == 0 {
		return nil, errors.New("empty command")
	}

	tagKeyResult, err := c.showTagFieldQuery(database, command)
	if err != nil {
		return nil, err
	}
	return tagKeyResult, nil
}

func (c *client) ShowSeries(database, command string) ([]string, error) {
	var series = make([]string, 0)
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
	for _, v := range seriesResult[0].Values {
		if _, ok := v.(string); !ok {
			return series, nil
		}
		series = append(series, v.(string))
	}
	return series, nil
}
