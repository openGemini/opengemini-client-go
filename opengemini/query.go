package opengemini

import (
	"encoding/json"
	"io"
)

type Query struct {
	Database        string
	Command         string
	RetentionPolicy string
}

// Query sends a command to the server
func (c *client) Query(q Query) (*QueryResult, error) {
	req := requestDetails{
		queryValues: make(map[string][]string),
	}
	req.queryValues.Add("db", q.Database)
	req.queryValues.Add("q", q.Command)
	resp, err := c.executeHttpGet(UrlQuery, req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var qr = new(QueryResult)
	err = json.Unmarshal(body, qr)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var qr = new(QueryResult)
	err = json.Unmarshal(body, qr)
	if err != nil {
		return nil, err
	}
	return qr, nil
}
