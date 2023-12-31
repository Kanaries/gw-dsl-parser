package parser

import "github.com/kanaries/gw-dsl-parser/common"

type RangeFilterRule struct {
	Type  string
	Value [2]float64
}

type DatasetType string
type Dataset struct {
	Type   DatasetType `json:"type"`
	Source string      `json:"source"`
}

func (r RangeFilterRule) Range() (min, max float64, ok bool) {
	if r.Type != "range" {
		return 0, 0, false
	}
	return r.Value[0], r.Value[1], true
}

type TemporalRangeFilterRule struct {
	Type  string
	Value [2]float64
}

func (tr TemporalRangeFilterRule) TemporalRange() (start, end float64, ok bool) {
	if tr.Type != "temporal range" {
		return 0, 0, false
	}
	return tr.Value[0], tr.Value[1], true
}

type OneOfFilterRule struct {
	Type  string
	Value []interface{}
}

func (oo OneOfFilterRule) OneOf() (values []interface{}, ok bool) {
	if oo.Type != "one of" {
		return nil, false
	}
	return oo.Value, true
}

type IExpParameter struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type FieldExpParameter struct {
	Type  string
	Value string
}

type ValueExpParameter struct {
	Type  string
	Value interface{}
}

type ExpressionExpParameter struct {
	Type  string
	Value IExpression
}

type ConstantExpParameter struct {
	Type  string
	Value interface{}
}

type GraphicWalkerDSL struct {
	Workflow []IBaseQuery `json:"workflow"`
	Limit    int          `json:"limit"`
	Offset   int          `json:"offset"`
}

type IBaseQuery struct {
	Type      string      `json:"type"`
	Query     []Query     `json:"query"`
	Filters   []Filter    `json:"filters"`
	Transform []Transform `json:"transform"`
	Sort      string      `json:"sort"` //'ascending' | 'descending'
	By        []string    `json:"by"`
}

func (query IBaseQuery) Descending() *bool {
	if len(query.Sort) == 0 {
		return nil
	}
	if query.Sort == SortDescending {
		return common.BoolPtr(true)
	}
	return common.BoolPtr(false)
}

const (
	SortAscending  = "ascending"
	SortDescending = "descending"
)

type Query struct {
	Op      string   `json:"op"`
	GroupBy []string `json:"groupBy"`
	//Agg     map[string]IAggregator `json:"agg"`
	Measures []Measure `json:"measures"`
	Fields   []string  `json:"fields"`
}

type Measure struct {
	Field      string      `json:"field"`
	Agg        IAggregator `json:"agg"`
	AsFieldKey string      `json:"asFieldKey"`
}

type Filter struct {
	Key  string      `json:"key"`
	Fid  string      `json:"fid"`
	Rule IFilterRule `json:"rule"`
}

type IAggregator string

const (
	Sum      IAggregator = "sum"
	Count                = "count"
	Max                  = "max"
	Min                  = "min"
	Mean                 = "mean"
	Median               = "median"
	Variance             = "variance"
	Stdev                = "stdev"
)

type IFilterRule struct {
	Type IFilter `json:"type"`
	//todo interface
	Value []interface{} `json:"value"`
}

type IFilter string

const (
	RANGE          IFilter = "range"
	ONE_OF                 = "one of"
	TEMPORAL_RANGE         = "temporal range"
)

type Transform struct {
	Expression IExpression `json:"expression"`
}

type IExpression struct {
	Op     string          `json:"op"`
	Params []IExpParameter `json:"params"`
	As     string          `json:"as"`
	Num    int64           `json:"num"`
}
