package main

/*
	The example code use the dot import, but the user should choose the package import method according to their own needs
*/

import (
	"context"
	"fmt"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"math/rand"
	"time"
)

func main() {
	// create an openGemini client
	config := &opengemini.Config{
		Addresses: []*opengemini.Address{{
			Host: "127.0.0.1",
			Port: 8086,
		}},
	}
	client, err := opengemini.NewClient(config)
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
	point := &opengemini.Point{}
	point.Measurement = exampleMeasurement
	point.AddTag("Weather", "foggy")
	point.AddField("Humidity", 87)
	point.AddField("Temperature", 25)
	err = client.WritePoint(exampleDatabase, point, func(err error) {
		if err != nil {
			fmt.Printf("write point failed for %s", err)
		}
	})
	if err != nil {
		fmt.Println(err)
	}

	// use write batch points method
	var pointList []*opengemini.Point
	var tagList []string
	tagList = append(tagList, "sunny", "rainy", "windy")
	for i := 0; i < 10; i++ {
		p := &opengemini.Point{}
		p.Measurement = exampleMeasurement
		p.AddTag("Weather", tagList[rand.Int31n(3)])
		p.AddField("Humidity", rand.Int31n(100))
		p.AddField("Temperature", rand.Int31n(40))
		p.Time = time.Now()
		pointList = append(pointList, p)
		time.Sleep(time.Nanosecond)
	}
	err = client.WriteBatchPoints(context.Background(), exampleDatabase, pointList)
	if err != nil {
		fmt.Println(err)
	}

	time.Sleep(time.Second * 5)

	// do a query
	q := opengemini.Query{
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
