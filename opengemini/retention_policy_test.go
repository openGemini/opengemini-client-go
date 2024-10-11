// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opengemini

import (
	"testing"

	"github.com/stretchr/testify/require"
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

func TestClientUpdateRetentionPolicy(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)

	err = c.UpdateRetentionPolicy(databaseName, RpConfig{Name: "autogen", Duration: "300d"}, true)
	require.Nil(t, err)
	rpResult, err := c.ShowRetentionPolicies(databaseName)
	require.Nil(t, err)
	require.Equal(t, len(rpResult), 1)
	require.Equal(t, rpResult[0].Name, "autogen")
	require.Equal(t, rpResult[0].Duration, "7200h0m0s")

	err = c.UpdateRetentionPolicy(databaseName, RpConfig{Name: "autogen", Duration: "300d", ShardGroupDuration: "2h", IndexDuration: "4h"}, true)
	require.Nil(t, err)
	rpResult, err = c.ShowRetentionPolicies(databaseName)
	require.Nil(t, err)
	require.Equal(t, len(rpResult), 1)
	require.Equal(t, rpResult[0].Name, "autogen")
	require.Equal(t, rpResult[0].Duration, "7200h0m0s")
	require.Equal(t, rpResult[0].IndexDuration, "4h0m0s")
	require.Equal(t, rpResult[0].ShardGroupDuration, "2h0m0s")

	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}
