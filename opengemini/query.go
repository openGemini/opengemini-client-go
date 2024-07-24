package opengemini

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Query struct {
	Database        string
	Command         string
	RetentionPolicy string
	Precision       Precision
}

type keyValue struct {
	Name  string
	Value string
}

// Query sends a command to the server
func (c *client) Query(q Query) (*QueryResult, error) {
	req := requestDetails{
		queryValues: make(map[string][]string),
	}
	req.queryValues.Add("db", q.Database)
	req.queryValues.Add("q", q.Command)
	req.queryValues.Add("rp", q.RetentionPolicy)
	req.queryValues.Add("epoch", q.Precision.Epoch())

	// metric
	c.metrics.queryCounter.Add(1)
	c.metrics.queryDatabaseCounter.WithLabelValues(q.Database).Add(1)
	startAt := time.Now()

	resp, err := c.executeHttpGet(UrlQuery, req)

	cost := float64(time.Since(startAt).Milliseconds())
	c.metrics.queryLatency.Observe(cost)
	c.metrics.queryDatabaseLatency.WithLabelValues(q.Database).Observe(cost)

	if err != nil {
		return nil, errors.New("query request failed, error: " + err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("query resp read failed, error: " + err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("query error resp, code: " + resp.Status + "body: " + string(body))
	}
	var qr = new(QueryResult)
	err = json.Unmarshal(body, qr)
	if err != nil {
		return nil, errors.New("query unmarshal resp body failed, error: " + err.Error())
	}
	return qr, nil
}

func (c *client) queryPost(q Query) (*QueryResult, error) {
	req := requestDetails{
		queryValues: make(map[string][]string),
	}

	req.queryValues.Add("db", q.Database)
	req.queryValues.Add("q", q.Command)
	resp, err := c.executeHttpPost(UrlQuery, req)
	if err != nil {
		return nil, errors.New("request failed, error: " + err.Error())
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("read resp failed, error: " + err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("error resp, code: " + resp.Status + "body: " + string(body))
	}
	var qr = new(QueryResult)
	err = json.Unmarshal(body, qr)
	if err != nil {
		return nil, errors.New("unmarshal resp body failed, error: " + err.Error())
	}
	return qr, nil

}

func (c *client) showTagSeriesQuery(database, command string) ([]ValuesResult, error) {
	var tagSeries []ValuesResult
	tagSeriesResult, err := c.Query(Query{Database: database, Command: command})
	if err != nil {
		return tagSeries, err
	}

	err = tagSeriesResult.hasError()
	if err != nil {
		return tagSeries, fmt.Errorf("get tagSeriesResult failed, error: %s", err)
	}
	if len(tagSeriesResult.Results) == 0 {
		return tagSeries, nil
	}
	values := tagSeriesResult.Results[0].Series
	tagSeries = make([]ValuesResult, 0, len(values))
	for _, res := range values {
		tagSeriesRes := new(ValuesResult)
		tagSeriesRes.Measurement = res.Name
		for _, valRes := range res.Values {
			for _, value := range valRes {
				strVal, ok := value.(string)
				if !ok {
					return tagSeries, nil
				}
				tagSeriesRes.Values = append(tagSeriesRes.Values, strVal)
			}
		}
		tagSeries = append(tagSeries, *tagSeriesRes)
	}
	return tagSeries, nil
}

func (c *client) showTagFieldQuery(database, command string) ([]ValuesResult, error) {
	var tagValueResult []ValuesResult
	tagKeyResult, err := c.Query(Query{Database: database, Command: command})
	if err != nil {
		return tagValueResult, err
	}

	err = tagKeyResult.hasError()
	if err != nil {
		return tagValueResult, fmt.Errorf("get tagKeyResult failed, error: %s", err)
	}
	if len(tagKeyResult.Results) == 0 {
		return tagValueResult, nil
	}

	values := tagKeyResult.Results[0].Series
	tagValueResult = make([]ValuesResult, 0, len(values))
	for _, res := range values {
		tagValueRes := new(ValuesResult)
		for _, valRes := range res.Values {
			tagValue := new(keyValue)
			if len(valRes) < 2 {
				return []ValuesResult{}, fmt.Errorf("invalid values: %s", valRes)
			}
			if strVal, ok := valRes[0].(string); ok {
				tagValue.Name = strVal
			}
			if strVal, ok := valRes[1].(string); ok {
				tagValue.Value = strVal
			}
			tagValueRes.Values = append(tagValueRes.Values, *tagValue)
		}
		tagValueRes.Measurement = res.Name
		tagValueResult = append(tagValueResult, *tagValueRes)
	}
	return tagValueResult, nil

}
