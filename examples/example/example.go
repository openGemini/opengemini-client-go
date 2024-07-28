package main

/*
	The example code use the dot import, but the user should choose the package import method according to their own needs
*/

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	. "github.com/openGemini/opengemini-client-go/opengemini"
)

func main() {
	// create an openGemini client
	config := &Config{
		Addresses: []*Address{{
			Host: "127.0.0.1",
			Port: 8086,
		}},
	}
	client, err := NewClient(config)
	if err != nil {
		fmt.Println(err)
		return
	}

	// create a database
	exampleDatabase := "ExampleDatabase"
	err = client.CreateDatabase(exampleDatabase)
	if err != nil {
		fmt.Println(err)
		return
	}

	exampleMeasurement := "ExampleMeasurement"

	// use point write method
	point := &Point{
		Measurement: exampleMeasurement,
		Tags:        map[string]string{"Weather": "foggy"},
		Fields: map[string]interface{}{
			"Humidity":    87,
			"Temperature": 25,
		},
	}
	err = client.WritePoint(context.Background(), exampleDatabase, point, func(err error) {
		if err != nil {
			fmt.Printf("write point failed for %s", err)
		}
	})
	if err != nil {
		fmt.Println(err)
	}

	// use write batch points method
	pointList := make([]*Point, 0, 10)
	tagList := []string{"sunny", "rainy", "windy"}
	for i := 0; i < 10; i++ {
		p := &Point{
			Measurement: exampleMeasurement,
			Time:        time.Now(),
			Tags:        map[string]string{"Weather": tagList[rand.Int31n(3)]},
			Fields: map[string]interface{}{
				"Humidity":    rand.Int31n(100),
				"Temperature": rand.Int31n(40),
			},
		}
		pointList = append(pointList, p)
		time.Sleep(time.Nanosecond)
	}
	err = client.WriteBatchPoints(exampleDatabase, pointList)
	if err != nil {
		fmt.Println(err)
	}

	time.Sleep(time.Second * 5)

	// do a query
	q := Query{
		Database: exampleDatabase,
		Command:  "select * from " + exampleMeasurement,
	}
	res, err := client.Query(q)
	if err != nil {
		fmt.Println(err)
	}
	for _, r := range res.Results {
		for _, s := range r.Series {
			for _, v := range s.Values {
				for _, i := range v {
					fmt.Print(i)
					fmt.Print(" | ")
				}
				fmt.Println()
			}
		}
	}
}
