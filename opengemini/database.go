package opengemini

import (
	"errors"
	"fmt"
	"strings"
)

func (c *client) CreateDatabase(database string) error {
	if len(database) == 0 {
		return errors.New("empty database name")
	}

	cmd := fmt.Sprintf("CREATE DATABASE \"%s\"", database)
	queryResult, err := c.queryPost(Query{Command: cmd})
	if err != nil {
		return err
	}

	err = queryResult.hasError()
	if err != nil {
		return fmt.Errorf("create database %w", err)
	}

	return nil
}

func (c *client) CreateDatabaseWithRp(database string, rpConfig RpConfig) error {
	if len(database) == 0 {
		return errors.New("empty database name")
	}

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("CREATE DATABASE \"%s\" WITH DURATION %s REPLICATION 1", database, rpConfig.Duration))
	if len(rpConfig.ShardGroupDuration) > 0 {
		buf.WriteString(fmt.Sprintf(" SHARD DURATION %s", rpConfig.ShardGroupDuration))
	}
	if len(rpConfig.IndexDuration) > 0 {
		buf.WriteString(fmt.Sprintf(" INDEX DURATION %s", rpConfig.IndexDuration))
	}
	buf.WriteString(fmt.Sprintf(" NAME %s", rpConfig.Name))
	queryResult, err := c.queryPost(Query{Command: buf.String()})
	if err != nil {
		return err
	}

	err = queryResult.hasError()
	if err != nil {
		return fmt.Errorf("create database with rentention policy err: %w", err)
	}

	return nil
}

func (c *client) ShowDatabases() ([]string, error) {
	var ShowDatabases = "SHOW DATABASES"
	queryResult, err := c.Query(Query{Command: ShowDatabases})
	if err != nil {
		return nil, err
	}
	if len(queryResult.Error) > 0 {
		return nil, fmt.Errorf("show datababse err: %s", queryResult.Error)
	}
	if len(queryResult.Results) == 0 || len(queryResult.Results[0].Series) == 0 {
		return []string{}, nil
	}
	var (
		values   = queryResult.Results[0].Series[0].Values
		dbResult = make([]string, 0, len(values))
	)

	for _, v := range values {
		if len(v) == 0 {
			continue
		}
		val, ok := v[0].(string)
		if !ok {
			continue
		}
		dbResult = append(dbResult, val)
	}
	return dbResult, nil
}

func (c *client) DropDatabase(database string) error {
	if len(database) == 0 {
		return errors.New("empty database name")
	}
	cmd := fmt.Sprintf("DROP DATABASE \"%s\"", database)
	queryResult, err := c.queryPost(Query{Command: cmd})
	if err != nil {
		return err
	}
	err = queryResult.hasError()
	if err != nil {
		return fmt.Errorf("drop database %w", err)
	}
	return nil
}
