package opengemini

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClient_Write(t *testing.T) {
	c := testDefaultClient(t)

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	bp := &BatchPoints{}
	testMeasurement := randomMeasurement()
	// point1 will write success with four kinds variant type field
	point1 := &Point{}
	point1.SetMeasurement(testMeasurement)
	point1.AddTag("Tag", "Test1")
	point1.AddField("stringField", "test1")
	point1.AddField("intField", 897870)
	point1.AddField("doubleField", 834.5433)
	point1.AddField("boolField", true)
	bp.AddPoint(point1)

	// point2 will parse fail for having no field
	point2 := &Point{}
	point2.SetMeasurement(testMeasurement)
	point2.AddTag("Tag", "Test2")
	bp.AddPoint(point2)

	// point3 will write success with timestamp
	point3 := &Point{}
	point3.SetMeasurement(testMeasurement)
	point3.AddTag("Tag", "Test3")
	point3.AddField("stringField", "test3")
	point3.AddField("boolField", false)
	point3.Time = time.Now()
	bp.AddPoint(point3)

	err = c.WriteBatchPoints(database, bp)
	assert.Nil(t, err)

	// check whether write success
	q := Query{
		Database: database,
		Command:  "select * from " + testMeasurement,
	}
	time.Sleep(time.Second * 5)
	result, err := c.Query(q)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result.Results[0].Series[0].Values))
}
