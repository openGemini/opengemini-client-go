package opengemini

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	testMeasurement := randomMeasurement()
	// point1 will write success with four kinds variant type field
	point1 := &Point{
		Measurement: testMeasurement,
		Tags:        map[string]string{"Tag": "Test1"},
		Fields: map[string]interface{}{
			"stringField": "test1",
			"intField":    897870,
			"doubleField": 834.5433,
			"boolField":   true,
		},
	}

	// point2 will parse fail for having no field
	point2 := &Point{
		Measurement: testMeasurement,
		Tags:        map[string]string{"Tag": "Test2"},
	}

	// point3 will write success with timestamp
	point3 := &Point{
		Measurement: testMeasurement,
		Time:        time.Now(),
		Tags:        map[string]string{"Tag": "Test3"},
		Fields: map[string]interface{}{
			"stringField": "test3",
			"boolField":   false,
		},
	}
	bp := []*Point{point1, point2, point3}

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

func TestClient_WriteBatchPointsWithRetentionPolicy(t *testing.T) {
	c := testDefaultClient(t)

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)
	err = c.CreateRetentionPolicy(database, RpConfig{Name: "testRp", Duration: "3d", ShardGroupDuration: "1h", IndexDuration: "7h"}, false)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	bp := make([]*Point, 3)
	testMeasurement := randomMeasurement()
	// point1 will write success with four kinds variant type field
	point1 := &Point{
		Measurement: testMeasurement,
		Tags:        map[string]string{"Tag": "Test1"},
		Fields: map[string]interface{}{
			"stringField": "test1",
			"intField":    897870,
			"doubleField": 834.5433,
			"boolField":   true,
		},
	}
	bp = append(bp, point1)

	// point2 will parse fail for having no field
	point2 := &Point{
		Measurement: testMeasurement,
		Tags:        map[string]string{"Tag": "Test2"},
	}
	bp = append(bp, point2)

	// point3 will write success with timestamp
	point3 := &Point{
		Measurement: testMeasurement,
		Time:        time.Now(),
		Tags:        map[string]string{"Tag": "Test3"},
		Fields: map[string]interface{}{
			"stringField": "test3",
			"boolField":   false,
		},
	}
	bp = append(bp, point3)

	err = c.WriteBatchPointsWithRp(database, "testRp", bp)
	assert.Nil(t, err)

	time.Sleep(time.Second * 5)
	// check whether write success
	res, err := c.Query(Query{
		Database: database,
		Command:  "select * from " + testMeasurement,
	})
	assert.Nil(t, err)
	assert.Contains(t, res.Results[0].Error, "measurement not found")

	res, err = c.Query(Query{
		Database:        database,
		Command:         "select * from " + testMeasurement,
		RetentionPolicy: "testRp",
	})
	assert.Nil(t, err)
	t.Logf("%#v", res.Results[0].Series[0].Values)
	assert.Equal(t, 2, len(res.Results[0].Series[0].Values))
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
	point := &Point{
		Measurement: randomMeasurement(),
		Tags:        map[string]string{"tag": "test"},
		Fields:      map[string]interface{}{"filed": "test"},
	}
	err = c.WritePoint(context.Background(), database, point, callback)
	assert.Nil(t, err)
}

func TestClientWritePointWithRetentionPolicy(t *testing.T) {
	c := testDefaultClient(t)

	// create a test database with rand suffix
	database := randomDatabaseName()
	err := c.CreateDatabase(database)
	assert.Nil(t, err)
	err = c.CreateRetentionPolicy(database, RpConfig{Name: "testRp", Duration: "3d", ShardGroupDuration: "1h", IndexDuration: "7h"}, false)
	assert.Nil(t, err)

	// delete test database before exit test case
	defer func() {
		err := c.DropDatabase(database)
		assert.Nil(t, err)
	}()

	callback := func(err error) {
		assert.Nil(t, err)
	}
	point := &Point{
		Measurement: randomMeasurement(),
		Tags:        map[string]string{"tag": "test"},
		Fields:      map[string]interface{}{"field": "test"},
	}
	err = c.WritePointWithRp(context.Background(), database, "testRp", point, callback)
	assert.Nil(t, err)
	time.Sleep(time.Second * 3)
	res, err := c.Query(Query{
		Database: database,
		Command:  "select * from " + point.Measurement,
	})
	assert.Nil(t, err)
	assert.Contains(t, res.Results[0].Error, "measurement not found")

	res, err = c.Query(Query{
		Database:        database,
		Command:         "select * from " + point.Measurement,
		RetentionPolicy: "testRp",
	})
	assert.NotNil(t, res.Results[0].Series[0].Values)
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
	point := &Point{
		Measurement: randomMeasurement(),
		Tags:        map[string]string{"tag": "test"},
		Fields:      map[string]interface{}{"field": 123},
	}
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

	// TestBatchInterval
	point := &Point{
		Measurement: "test",
		Fields:      map[string]interface{}{"field": "interval"},
	}
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
		point := &Point{
			Measurement: "test",
			Time:        time.Now(),
			Fields:      map[string]interface{}{"field": "size"},
		}
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
