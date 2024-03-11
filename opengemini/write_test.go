package opengemini

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClientWriteBatchPoints(t *testing.T) {
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

func TestClientWritePoint(t *testing.T) {
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

	callback := func(err error) {
		assert.Nil(t, err)
	}
	point := &Point{}
	point.Measurement = randomMeasurement()
	point.AddTag("tag", "test")
	point.AddField("field", "test")
	err = c.WritePoint(context.Background(), database, point, callback)
	assert.Nil(t, err)
}

func TestWriteAssignedIntegerField(t *testing.T) {
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

	callback := func(err error) {
		assert.Nil(t, err)
	}
	measurement := randomMeasurement()
	point := &Point{}
	point.Measurement = measurement
	point.AddTag("tag", "test")
	point.AddField("field", 123)
	err = c.WritePoint(context.Background(), database, point, callback)
	assert.Nil(t, err)

	time.Sleep(time.Second * 5)

	// check field's data type
	res, err := c.ShowFieldKeys(database, fmt.Sprintf("SHOW FIELD KEYS FROM %s", measurement))
	assert.Nil(t, err)
	if value, ok := res[0].Values[0].(keyValue); !ok {
		t.Fail()
	} else {
		assert.Equal(t, "integer", value.Value)
	}
}

func TestWriteWithBatchInterval(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "127.0.0.1",
			Port: 8086,
		}},
		BatchConfig: &BatchConfig{
			BatchSize:     5000,
			BatchInterval: time.Second * 5,
		},
	})

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	//TestBatchInterval
	point := &Point{}
	point.SetMeasurement("test")
	point.AddField("field", "interval")
	receiver := make(chan struct{})
	startTime := time.Now()
	err = c.WritePoint(context.Background(), database, point, func(err error) {
		receiver <- struct{}{}
	})
	assert.Nil(t, err)
	timer := time.NewTimer(10 * time.Second)
	for {
		select {
		case <-receiver:
			goto END
		case <-timer.C:
			goto END
		}
	}
END:
	duration := time.Since(startTime)
	assert.Equal(t, true, duration < 10*time.Second)
}

func TestWriteWithBatchSize(t *testing.T) {
	c := testNewClient(t, &Config{
		Addresses: []*Address{{
			Host: "127.0.0.1",
			Port: 8086,
		}},
		BatchConfig: &BatchConfig{
			BatchSize:     10,
			BatchInterval: time.Hour,
		},
	})

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()
	callbackCount := 0
	receiver := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		point := &Point{}
		point.SetMeasurement("test")
		point.AddField("field", "test")
		point.SetTime(time.Now())
		err := c.WritePoint(context.Background(), database, point, func(err error) {
			receiver <- struct{}{}
		})
		assert.Nil(t, err)
	}

	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()

	for callbackCount < 10 {
		select {
		case <-receiver:
			callbackCount++
		case <-timer.C:
			t.Fatalf("Test timed out")
		}
	}
}
