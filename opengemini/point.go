package opengemini

import "time"

type Point struct {
	Measurement string
	Precision   string
	Time        time.Time
	Tags        map[string]string
	Fields      map[string]interface{}
}

func (p *Point) AddTag(key string, value string) {
	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}
	p.Tags[key] = value
}

func (p *Point) AddField(key string, value interface{}) {
	if p.Fields == nil {
		p.Fields = make(map[string]interface{})
	}
	p.Fields[key] = value
}

func (p *Point) SetTime(t time.Time) {
	p.Time = t
}

func (p *Point) SetPrecision(precision string) {
	p.Precision = precision
}

func (p *Point) SetMeasurement(name string) {
	p.Measurement = name
}

type BatchPoints struct {
	Points []*Point
}

func (bp *BatchPoints) AddPoint(p *Point) {
	bp.Points = append(bp.Points, p)
}
