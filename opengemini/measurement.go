package opengemini

type ValuesResult struct {
	Measurement string
	Values      []interface{}
}

func (c *client) DropMeasurement(database, retentionPolicy, measurement string) error {
	err := CheckDatabaseAndPolicyAndMeasurement(database, retentionPolicy, measurement)
	if err != nil {
		return err
	}
	panic("implement me")
}
