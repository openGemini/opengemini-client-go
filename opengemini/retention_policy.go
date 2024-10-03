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

func (rp *RetentionPolicy) SetName(value SeriesValue) error {
	name, ok := value[0].(string)
	if !ok {
		return fmt.Errorf("set RetentionPolicy name: name must be a string")
	}
	rp.Name = name
	return nil
}

func (rp *RetentionPolicy) SetDuration(value SeriesValue) error {
	duration, ok := value[1].(string)
	if !ok {
		return fmt.Errorf("set RetentionPolicy duration: duration must be a string")
	}
	rp.Duration = duration
	return nil
}

func (rp *RetentionPolicy) SetShardGroupDuration(value SeriesValue) error {
	sgDuration, ok := value[2].(string)
	if !ok {
		return fmt.Errorf("set RetentionPolicy shardGroupDuration: shardGroupDuration must be a string")
	}
	rp.ShardGroupDuration = sgDuration
	return nil
}

func (rp *RetentionPolicy) SetHotDuration(value SeriesValue) error {
	hDuration, ok := value[3].(string)
	if !ok {
		return fmt.Errorf("set RetentionPolicy hotDuration: hotDuration must be a string")
	}
	rp.HotDuration = hDuration
	return nil
}

func (rp *RetentionPolicy) SetWarmDuration(value SeriesValue) error {
	wDuration, ok := value[4].(string)
	if !ok {
		return fmt.Errorf("set RetentionPolicy warmDuration: warmDuration must be a string")
	}
	rp.WarmDuration = wDuration
	return nil
}

func (rp *RetentionPolicy) SetIndexDuration(value SeriesValue) error {
	iDuration, ok := value[5].(string)
	if !ok {
		return fmt.Errorf("set RetentionPolicy indexDuration: indexDuration must be a string")
	}
	rp.IndexDuration = iDuration
	return nil
}

func (rp *RetentionPolicy) SetReplicaNum(value SeriesValue) error {
	replicaNum, ok := value[6].(float64)
	if !ok {
		return fmt.Errorf("set RetentionPolicy replicaNum: replicaNum must be a float64")
	}
	rp.ReplicaNum = int64(replicaNum)
	return nil
}

func (rp *RetentionPolicy) SetDefault(value SeriesValue) error {
	isDefault, ok := value[7].(bool)
	if !ok {
		return fmt.Errorf("set RetentionPolicy isDefault: isDefault must be a bool")
	}
	rp.IsDefault = isDefault
	return nil
}

func NewRetentionPolicy(value SeriesValue) *RetentionPolicy {
	rp := &RetentionPolicy{}
	if !errors.Is(rp.SetName(value), nil) ||
		!errors.Is(rp.SetDuration(value), nil) ||
		!errors.Is(rp.SetShardGroupDuration(value), nil) ||
		!errors.Is(rp.SetHotDuration(value), nil) ||
		!errors.Is(rp.SetWarmDuration(value), nil) ||
		!errors.Is(rp.SetIndexDuration(value), nil) ||
		!errors.Is(rp.SetReplicaNum(value), nil) ||
		!errors.Is(rp.SetDefault(value), nil) {
		return nil
	}
	return rp
}

// CreateRetentionPolicy Create retention policy
func (c *client) CreateRetentionPolicy(database string, rpConfig RpConfig, isDefault bool) error {
	err := checkDatabaseAndPolicy(database, rpConfig.Name)
	if err != nil {
		return err
	}

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("CREATE RETENTION POLICY %s ON \"%s\" DURATION %s REPLICATION 1", rpConfig.Name, database, rpConfig.Duration))
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
		return fmt.Errorf("create retention policy %w", err)
	}

	return nil
}

func (c *client) UpdateRetentionPolicy(database string, rpConfig RpConfig, isDefault bool) error {
	err := checkDatabaseAndPolicy(database, rpConfig.Name)
	if err != nil {
		return err
	}

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("ALTER RETENTION POLICY %s ON \"%s\" ", rpConfig.Name, database))
	if len(rpConfig.Duration) > 0 {
		buf.WriteString(fmt.Sprintf(" DURATION %s", rpConfig.Duration))
	}
	if len(rpConfig.IndexDuration) > 0 {
		buf.WriteString(fmt.Sprintf(" INDEX DURATION %s", rpConfig.IndexDuration))
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
		return fmt.Errorf("update retention policy %w", err)
	}

	return nil
}

// ShowRetentionPolicies Show retention policy
func (c *client) ShowRetentionPolicies(database string) ([]RetentionPolicy, error) {
	err := checkDatabaseName(database)
	if err != nil {
		return nil, err
	}

	var (
		ShowRetentionPolicy = "SHOW RETENTION POLICIES"
		rpResult            []RetentionPolicy
	)

	queryResult, err := c.Query(Query{Database: database, Command: ShowRetentionPolicy})
	if err != nil {
		return nil, err
	}

	err = queryResult.hasError()
	if err != nil {
		return rpResult, fmt.Errorf("show retention policy err: %s", err)
	}

	rpResult = queryResult.convertRetentionPolicyList()
	return rpResult, nil
}

// DropRetentionPolicy Drop retention policy
func (c *client) DropRetentionPolicy(database, retentionPolicy string) error {
	err := checkDatabaseAndPolicy(database, retentionPolicy)
	if err != nil {
		return err
	}

	cmd := fmt.Sprintf("DROP RETENTION POLICY %s ON \"%s\"", retentionPolicy, database)
	queryResult, err := c.queryPost(Query{Command: cmd})
	if err != nil {
		return err
	}
	err = queryResult.hasError()
	if err != nil {
		return fmt.Errorf("drop  retention policy %w", err)
	}
	return nil
}
