# opengemini-client-go

![License](https://img.shields.io/badge/license-Apache2.0-green) ![Language](https://img.shields.io/badge/Language-Go-blue.svg) [![version](https://img.shields.io/github/v/tag/opengemini/opengemini-client-go?label=release&color=blue)](https://github.com/opengemini/opengemini-client-go/releases) [![Godoc](http://img.shields.io/badge/docs-go.dev-blue.svg?style=flat-square)](https://pkg.go.dev/github.com/openGemini/opengemini-client-go)

[English](README.md) | 简体中文 

`opengemini-client-go` 是一个用 Go 语言编写的 OpenGemini 客户端。

## 设计文档

[OpenGemini Client 设计文档](https://github.com/openGemini/openGemini.github.io/blob/main/src/zh/guide/develop/client_design.md)

## 关于 OpenGemini

OpenGemini 是一款云原生分布式时序数据库。获取更多信息，请点击[这里](https://github.com/openGemini/openGemini)。

## 要求

- Go 1.19+

## 用法

引入客户端库：

<i><font color=gray>示例使用点引用法，用户可结合具体需要选择适合的引用方式。</font></i>

```go
import . "github.com/openGemini/opengemini-client-go/opengemini"
```

创建客户端：

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

创建数据库：

```go
exampleDatabase := "ExampleDatabase"
err = client.CreateDatabase(exampleDatabase)
if err != nil {
	fmt.Println(err)
	return
}
```

写入单个点：

```go
exampleMeasurement := "ExampleMeasurement"
point := &Point{}
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
```

批量写入点：

```go
exampleMeasurement := "ExampleMeasurement"
var pointList []*Point
var tagList []string
tagList = append(tagList, "sunny", "rainy", "windy")
for i := 0; i < 10; i++ {
	p := &Point{}
	p.Measurement = exampleMeasurement
	p.AddTag("Weather", tagList[rand.Int31n(3)])
	p.AddField("Humidity", rand.Int31n(100))
	p.AddField("Temperature", rand.Int31n(40))
	p.Time = time.Now()
        pointList = append(pointList,p)
	time.Sleep(time.Nanosecond)
}
err = client.WriteBatchPoints(exampleDatabase, pointList)
if err != nil {
	fmt.Println(err)
}
```

执行查询：

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
