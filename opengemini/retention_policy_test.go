package opengemini

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClientCreateRetentionPolicy(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	retentionPolicyTest1 := randomRetentionPolicy()
	retentionPolicyTest2 := randomRetentionPolicy()
	retentionPolicyTest3 := randomRetentionPolicy()
	retentionPolicyTest4 := randomRetentionPolicy()
	err = c.CreateRetentionPolicy(databaseName, RpConfig{Name: retentionPolicyTest1, Duration: "3d"}, false)
	require.Nil(t, err)
	err = c.CreateRetentionPolicy(databaseName, RpConfig{Name: retentionPolicyTest2, Duration: "3d", ShardGroupDuration: "1h"}, false)
	require.Nil(t, err)
	err = c.CreateRetentionPolicy(databaseName, RpConfig{Name: retentionPolicyTest3, Duration: "3d", ShardGroupDuration: "1h", IndexDuration: "7h"}, false)
	require.Nil(t, err)
	err = c.CreateRetentionPolicy(databaseName, RpConfig{Name: retentionPolicyTest4, Duration: "3d"}, true)
	require.Nil(t, err)
	err = c.DropRetentionPolicy(databaseName, retentionPolicyTest1)
	require.Nil(t, err)
	err = c.DropRetentionPolicy(databaseName, retentionPolicyTest2)
	require.Nil(t, err)
	err = c.DropRetentionPolicy(databaseName, retentionPolicyTest3)
	require.Nil(t, err)
	err = c.DropRetentionPolicy(databaseName, retentionPolicyTest4)
	require.Nil(t, err)
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientCreateRetentionPolicyNotExistDatabase(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	retentionPolicy := randomRetentionPolicy()
	err := c.CreateRetentionPolicy(databaseName, RpConfig{Name: retentionPolicy, Duration: "3d"}, false)
	require.NotNil(t, err)
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientCreateRetentionPolicyEmptyDatabase(t *testing.T) {
	c := testDefaultClient(t)
	retentionPolicy := randomRetentionPolicy()
	err := c.CreateRetentionPolicy("", RpConfig{Name: retentionPolicy, Duration: "3d"}, false)
	require.NotNil(t, err)
}

func TestClientDropRetentionPolicy(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	retentionPolicy := randomRetentionPolicy()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	err = c.CreateRetentionPolicy(databaseName, RpConfig{Name: retentionPolicy, Duration: "3d"}, false)
	require.Nil(t, err)
	err = c.DropRetentionPolicy(databaseName, retentionPolicy)
	require.Nil(t, err)
}

func TestClientShowRetentionPolicy(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	rpResult, err := c.ShowRetentionPolicies(databaseName)
	require.Nil(t, err)
	require.NotEqual(t, len(rpResult), 0)
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}
