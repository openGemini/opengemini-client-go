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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClientCreateDatabaseSuccess(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientCreateDatabaseEmptyDatabase(t *testing.T) {
	c := testDefaultClient(t)
	err := c.CreateDatabase("")
	require.NotNil(t, err)
}

func TestClientCreateDatabaseWithRpSuccess(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabaseWithRp(databaseName, RpConfig{Name: "test4", Duration: "1d", ShardGroupDuration: "1h", IndexDuration: "7h"})
	require.Nil(t, err)
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientCreateDatabaseWithRpZeroSuccess(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabaseWithRp(databaseName, RpConfig{Name: "test4", Duration: "0", ShardGroupDuration: "1h", IndexDuration: "7h"})
	require.NotNil(t, err)
}

func TestClientCreateDatabaseWithRpInvalid(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabaseWithRp(databaseName, RpConfig{Name: "test4", Duration: "1", ShardGroupDuration: "1h", IndexDuration: "7h"})
	require.NotNil(t, err)
}

func TestClientCreateDatabaseWithRpEmptyDatabase(t *testing.T) {
	c := testDefaultClient(t)
	err := c.CreateDatabaseWithRp("", RpConfig{Name: "test4", Duration: "1h", ShardGroupDuration: "1h", IndexDuration: "7h"})
	require.NotNil(t, err)
}

func TestClientShowDatabases(t *testing.T) {
	c := testDefaultClient(t)
	_, err := c.ShowDatabases()
	require.Nil(t, err)
}

func TestClientDropDatabase(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClientDropDatabaseEmptyDatabase(t *testing.T) {
	c := testDefaultClient(t)
	err := c.DropDatabase("")
	require.NotNil(t, err)
}

func TestCreateAndDropDatabaseWithSpecificSymbol(t *testing.T) {
	c := testDefaultClient(t)
	err := c.CreateDatabase("Specific-Symbol")
	require.Nil(t, err)
	err = c.DropDatabase("Specific-Symbol")
	require.Nil(t, err)
}
