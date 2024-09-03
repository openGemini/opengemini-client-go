package opengemini

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type ValuesResult struct {
	Measurement string
	Values      []interface{}
}

func (c *client) ShowMeasurements(builder ShowMeasurementBuilder) ([]string, error) {
	base := builder.getMeasurementBase()
	err := checkDatabaseName(base.database)
	if err != nil {
		return nil, err
	}

	command, err := builder.build()
	if err != nil {
		return nil, err
	}

	queryResult, err := c.queryPost(Query{
		Database:        base.database,
		RetentionPolicy: base.retentionPolicy,
		Command:         command,
	})

	if err != nil {
		return nil, err
	}

	err = queryResult.hasError()
	if err != nil {
		return nil, fmt.Errorf("show measurements err: %s", err)
	}

	return queryResult.convertMeasurements(), nil
}

func (c *client) DropMeasurement(database, retentionPolicy, measurement string) error {
	err := checkDatabaseName(database)
	if err != nil {
		return err
	}
	if err = checkMeasurementName(measurement); err != nil {
		return err
	}

	req := requestDetails{
		queryValues: make(url.Values),
	}
	req.queryValues.Add("db", database)
	req.queryValues.Add("rp", retentionPolicy)
	req.queryValues.Add("q", `DROP MEASUREMENT "`+measurement+`"`)
	resp, err := c.executeHttpPost("/query", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.New("read resp failed, error: " + err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("error resp, code: " + resp.Status + "body: " + string(body))
	}
	return nil
}

func (c *client) CreateMeasurement(builder CreateMeasurementBuilder) error {
	base := builder.getMeasurementBase()
	err := checkDatabaseName(base.database)
	if err != nil {
		return err
	}

	command, err := builder.build()
	if err != nil {
		return err
	}
	req := requestDetails{
		queryValues: make(url.Values),
	}
	req.queryValues.Add("db", base.database)
	req.queryValues.Add("rp", base.retentionPolicy)
	req.queryValues.Add("q", command)
	resp, err := c.executeHttpPost("/query", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.New("read resp failed, error: " + err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("error resp, code: " + resp.Status + "body: " + string(body))
	}
	return nil
}
