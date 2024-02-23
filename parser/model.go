package parser

import "encoding/json"

type Dataset struct {
	name    string
	fields  map[string]Field
	dialect string
}

func NewDataset(name string, fields map[string]Field, dialect string) *Dataset {
	return &Dataset{
		name:    name,
		fields:  fields,
		dialect: dialect,
	}
}

type Field struct {
	Key  string `json:"key"`
	Fid  string `json:"fid"`
	Type string `json:"type"`
}

const (
	STRING   = "string"
	NUMBER   = "number"
	DATETIME = "datetime"
	BOOLEAN  = "bool"
)

func (d *Dataset) generateMetaStr() string {
	var values = make(map[string]interface{})
	fields := make([]Field, 0, len(d.fields))
	for _, v := range d.fields {
		fields = append(fields, v)
	}
	values[d.name] = fields
	str, _ := json.Marshal(values)
	return string(str)
}
