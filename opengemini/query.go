package opengemini

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

type Query struct {
	Database        string
	Command         string
	RetentionPolicy string
	// Precision Timestamp reply format , without Precision server will reply with RFC3339 format
	Precision PrecisionType
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
	req.queryValues.Add("epoch", q.Precision.String())

	resp, err := c.executeHttpGet(UrlQuery, req)
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
	tagSeries := make([]ValuesResult, 0)
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

	for _, res := range tagSeriesResult.Results[0].Series {
		tagSeriesRes := new(ValuesResult)
		tagSeriesRes.Measurement = res.Name
		for _, valRes := range res.Values {
			for _, value := range valRes {
				if _, ok := value.(string); !ok {
					return tagSeries, nil
				}
				tagSeriesRes.Values = append(tagSeriesRes.Values, value.(string))
			}
		}
		tagSeries = append(tagSeries, *tagSeriesRes)
	}
	return tagSeries, nil
}

func (c *client) showTagFieldQuery(database, command string) ([]ValuesResult, error) {
	tagValueResult := make([]ValuesResult, 0)
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

	for _, res := range tagKeyResult.Results[0].Series {
		tagValueRes := new(ValuesResult)
		for _, valRes := range res.Values {
			tagValue := new(keyValue)
			if len(valRes) < 2 {
				return tagValueResult, nil
			}
			if _, ok := valRes[0].(string); ok {
				tagValue.Name = valRes[0].(string)
			}
			if _, ok := valRes[1].(string); ok {
				tagValue.Value = valRes[1].(string)
			}
			tagValueRes.Values = append(tagValueRes.Values, *tagValue)
		}
		tagValueRes.Measurement = res.Name
		tagValueResult = append(tagValueResult, *tagValueRes)
	}
	return tagValueResult, nil

}
