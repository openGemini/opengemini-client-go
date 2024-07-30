package opengemini

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestQueryWithEpoch(t *testing.T) {
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
	p := &Point{}
	p.Measurement = testMeasurement
	p.AddField("TestField", 123)
	p.Time = time.Now()

	err = c.WritePoint(database, p, func(err error) {
		assert.Nil(t, err)
	})
	assert.Nil(t, err)

	time.Sleep(time.Second * 5)

	PrecisionTimestampLength := make(map[Precision]int64)
	PrecisionTimestampLength[PrecisionNanosecond] = 19
	PrecisionTimestampLength[PrecisionMicrosecond] = 16
	PrecisionTimestampLength[PrecisionMillisecond] = 13
	PrecisionTimestampLength[PrecisionSecond] = 10
	PrecisionTimestampLength[PrecisionMinute] = 8
	PrecisionTimestampLength[PrecisionHour] = 6

	// check whether write success
	for precision, length := range PrecisionTimestampLength {
		q := Query{
			Database:  database,
			Command:   "select * from " + testMeasurement,
			Precision: precision,
		}
		result, err := c.Query(q)
		assert.Nil(t, err)
		v := int64(result.Results[0].Series[0].Values[0][0].(float64))
		assert.Equal(t, length, getTimestampLength(v))
	}
}

func getTimestampLength(timestamp int64) int64 {
	var length int64 = 0
	for ; timestamp != 0; length++ {
		timestamp /= 10
	}
	return length
}
