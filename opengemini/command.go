package opengemini

func (c *client) ShowTagKeys(database, command string) ([]ValuesResult, error) {
	err := CheckDatabaseAndCommand(database, command)
	if err != nil {
		return nil, err
	}

	tagKeyResult, err := c.showTagSeriesQuery(database, command)
	if err != nil {
		return nil, err
	}
	return tagKeyResult, nil
}

func (c *client) ShowTagValues(database, command string) ([]ValuesResult, error) {
	err := CheckDatabaseAndCommand(database, command)
	if err != nil {
		return nil, err
	}

	tagValueResult, err := c.showTagFieldQuery(database, command)
	if err != nil {
		return nil, err
	}
	return tagValueResult, nil
}

func (c *client) ShowFieldKeys(database, command string) ([]ValuesResult, error) {
	err := CheckDatabaseAndCommand(database, command)
	if err != nil {
		return nil, err
	}

	tagKeyResult, err := c.showTagFieldQuery(database, command)
	if err != nil {
		return nil, err
	}
	return tagKeyResult, nil
}

func (c *client) ShowSeries(database, command string) ([]string, error) {
	err := CheckDatabaseAndCommand(database, command)
	if err != nil {
		return nil, err
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
