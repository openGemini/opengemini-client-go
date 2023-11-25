package opengemini

import (
	"errors"
	"fmt"
	"strings"
)

// RetentionPolicy defines the structure for retention policy info
type RetentionPolicy struct {
	Name               string
	Duration           string
	ShardGroupDuration string
	HotDuration        string
	WarmDuration       string
	IndexDuration      string
	ReplicaNum         int64
	IsDefault          bool
}

// CreateRetentionPolicy Create retention policy
func (c *client) CreateRetentionPolicy(database string, rpConfig RpConfig, isDefault bool) error {
	if len(database) == 0 {
		return errors.New("empty database name")
	}
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("CREATE RETENTION POLICY %s ON %s DURATION %s REPLICATION 1", rpConfig.Name, database, rpConfig.Duration))
	if len(rpConfig.ShardGroupDuration) > 0 {
		buf.WriteString(fmt.Sprintf(" SHARD DURATION %s", rpConfig.ShardGroupDuration))
	}
	if len(rpConfig.IndexDuration) > 0 {
		buf.WriteString(fmt.Sprintf(" INDEX DURATION %s", rpConfig.IndexDuration))
	}
	if isDefault {
		buf.WriteString(" DEFAULT")
	}

	queryResult, err := c.queryPost(Query{Command: buf.String()})
	if err != nil {
		return err
	}

	err = queryResult.hasError()
	if err != nil {
		return fmt.Errorf("create retention policy err: %s", err)
	}

	return nil
}

// ShowRetentionPolicy Show retention policy
func (c *client) ShowRetentionPolicy(database string) ([]RetentionPolicy, error) {
	var (
		ShowRetentionPolicy = "SHOW RETENTION POLICIES"
		rpResult            = make([]RetentionPolicy, 0)
	)
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}

	queryResult, err := c.Query(Query{Database: database, Command: ShowRetentionPolicy})
	if err != nil {
		return nil, err
	}

	err = queryResult.hasError()
	if err != nil {
		return rpResult, fmt.Errorf("show retention policy err: %s", err)
	}

	if len(queryResult.Results) == 0 {
		return rpResult, nil
	}
	if len(queryResult.Results[0].Series) == 0 {
		return rpResult, nil
	}
	rpResult = convertRetentionPolicy(queryResult)
	return rpResult, nil
}

func convertRetentionPolicy(queryResult *QueryResult) []RetentionPolicy {
	var (
		retentionPolicy = make([]RetentionPolicy, 0)
		rpColumnLen     = 8
	)
	if len(queryResult.Results) == 0 || len(queryResult.Results[0].Series) == 0 {
		return retentionPolicy
	}

	for _, v := range queryResult.Results[0].Series[0].Values {
		if len(v) < rpColumnLen {
			break
		}
		var (
			ok         bool
			replicaNum float64
		)
		rp := new(RetentionPolicy)
		if rp.Name, ok = v[0].(string); !ok {
			break
		}

		if rp.Duration, ok = v[1].(string); !ok {
			break
		}
		if rp.ShardGroupDuration, ok = v[2].(string); !ok {
			break
		}
		if rp.HotDuration, ok = v[3].(string); !ok {
			break
		}
		if rp.WarmDuration, ok = v[4].(string); !ok {
			break
		}
		if rp.IndexDuration, ok = v[5].(string); !ok {
			break
		}
		if replicaNum, ok = v[6].(float64); !ok {
			break
		}
		rp.ReplicaNum = int64(replicaNum)
		if rp.IsDefault, ok = v[7].(bool); !ok {
			break
		}
		retentionPolicy = append(retentionPolicy, *rp)
	}
	return retentionPolicy
}

// DropRetentionPolicy Drop retention policy
func (c *client) DropRetentionPolicy(rp string, database string) error {
	if len(rp) == 0 {
		return errors.New("empty retention policy")
	}
	if len(database) == 0 {
		return errors.New("empty database name")
	}

	cmd := fmt.Sprintf("DROP RETENTION POLICY %s ON %s", rp, database)
	queryResult, err := c.queryPost(Query{Command: cmd})
	if err != nil {
		return err
	}
	err = queryResult.hasError()
	if err != nil {
		return fmt.Errorf("drop  retention policy err: %s", err)
	}
	return nil
}
