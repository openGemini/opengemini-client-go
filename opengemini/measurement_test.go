package opengemini

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestClientDropMeasurementExistSpecifyRp(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	retentionPolicy := randomRetentionPolicy()
	measurement := randomMeasurement()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	err = c.CreateRetentionPolicy(databaseName, RpConfig{Name: retentionPolicy, Duration: "3d"}, false)
	require.Nil(t, err)
	err = c.WriteBatchPointsWithRp(context.Background(), databaseName, retentionPolicy, []*Point{
		{
			Measurement: measurement,
			Precision:   0,
			Time:        time.Time{},
			Tags:        nil,
			Fields: map[string]interface{}{
				"value": 1,
			},
		},
	})
	require.Nil(t, err)
	time.Sleep(time.Second * 5)
	measurements, err := c.ShowMeasurements(databaseName, retentionPolicy)
	require.Nil(t, err)
	require.Contains(t, measurements, measurement)
	err = c.DropMeasurement(databaseName, retentionPolicy, measurement)
	require.Nil(t, err)
	measurements, err = c.ShowMeasurements(databaseName, retentionPolicy)
	require.Nil(t, err)
	require.NotContains(t, measurements, measurement)
	err = c.DropRetentionPolicy(databaseName, retentionPolicy)
	require.Nil(t, err)
}

func TestClientDropMeasurementNonExistent(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	retentionPolicy := randomRetentionPolicy()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	err = c.CreateRetentionPolicy(databaseName, RpConfig{Name: retentionPolicy, Duration: "3d"}, false)
	require.Nil(t, err)
	err = c.DropMeasurement(databaseName, retentionPolicy, "non_existent_measurement")
	require.Nil(t, err)
	err = c.DropRetentionPolicy(databaseName, retentionPolicy)
	require.Nil(t, err)
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientDropMeasurementEmptyMeasurementName(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	retentionPolicy := randomRetentionPolicy()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	err = c.CreateRetentionPolicy(databaseName, RpConfig{Name: retentionPolicy, Duration: "3d"}, false)
	require.Nil(t, err)
	err = c.DropMeasurement(databaseName, retentionPolicy, "")
	require.NotNil(t, err)
	err = c.DropRetentionPolicy(databaseName, retentionPolicy)
	require.Nil(t, err)
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientDropMeasurementEmptyRetentionPolicy(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	measurement := randomMeasurement()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	err = c.WriteBatchPoints(context.Background(), databaseName, []*Point{
		{
			Measurement: measurement,
			Precision:   0,
			Time:        time.Time{},
			Tags:        nil,
			Fields: map[string]interface{}{
				"value": 1,
			},
		},
	})
	require.Nil(t, err)
	time.Sleep(time.Second * 5)
	measurements, err := c.ShowMeasurements(databaseName, "")
	require.Nil(t, err)
	require.Contains(t, measurements, measurement)
	err = c.DropMeasurement(databaseName, "", measurement)
	require.Nil(t, err)
	measurements, err = c.ShowMeasurements(databaseName, "")
	require.Nil(t, err)
	require.NotContains(t, measurements, measurement)
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientDropMeasurementEmptyDatabaseName(t *testing.T) {
	c := testDefaultClient(t)
	retentionPolicy := randomRetentionPolicy()
	measurement := randomMeasurement()
	err := c.DropMeasurement("", retentionPolicy, measurement)
	require.NotNil(t, err)
}
