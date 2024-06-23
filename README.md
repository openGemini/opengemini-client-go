# opengemini-client-go

opengemini-client-go is a Go client for OpenGemini. 

English | [简体中文](README_CN.md) 

OpenGemini is an open-source time series database, find more about OpenGemini at https://github.com/openGemini/openGemini

## Requirements

- Go 1.19

## Usage

Import the Client Library:

<i><font color=gray>The example code use the dot import, but the user should choose the package import method according to their own needs</font></i>

```go
import . "github.com/openGemini/opengemini-client-go/opengemini"
```

Create a Client:

```go
config := &Config{
	Addresses: []*Address{
		{
			Host: "127.0.0.1",
			Port: 8086,
		},
	},
}
client, err := NewClient(config)
if err != nil {
	fmt.Println(err)
}
```

Create a Database:

```go
exampleDatabase := "ExampleDatabase"
err = client.CreateDatabase(exampleDatabase)
if err != nil {
	fmt.Println(err)
	return
}
```

Write single point:

```go
exampleMeasurement := "ExampleMeasurement"
point := &Point{}
point.SetMeasurement(exampleMeasurement)
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
```

Write batch points:

```go
exampleMeasurement := "ExampleMeasurement"
var pointList []*Point
var tagList []string
tagList = append(tagList, "sunny", "rainy", "windy")
for i := 0; i < 10; i++ {
	p := &Point{}
	p.SetMeasurement(exampleMeasurement)
	p.AddTag("Weather", tagList[rand.Int31n(3)])
	p.AddField("Humidity", rand.Int31n(100))
	p.AddField("Temperature", rand.Int31n(40))
	p.SetTime(time.Now())
        pointList = append(pointList,p)
	time.Sleep(time.Nanosecond)
}
err = client.WriteBatchPoints(exampleDatabase, pointList)
if err != nil {
	fmt.Println(err)
}
```

Do a query:

```go
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
```
