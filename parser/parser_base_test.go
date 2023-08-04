package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/foghorn-tech/kanaries-dsl/common"
	"strings"
	"testing"
)

func TestRaw(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "raw",
			  "fields": [
				"col_2"
			  ]
			}
		  ]
		}
	  ]
	}
	`
	sql := "SELECT col_2 FROM table1"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestRange(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "filter",
		  "filters": [
			{
			  "fid": "col_1",
			  "rule": {
				"type": "range",
				"value": [
				  4.1386666666666665,
				  12
				]
			  }
			}
		  ]
		},
		{
		  "type": "view",
		  "query": [
			{
			  "op": "raw",
			  "fields": [
				"col_1",
				"col_1"
			  ]
			}
		  ]
		}
	  ]
	}`
	sql := "SELECT col_1, col_1 FROM table1 WHERE col_1 BETWEEN 4.1387 AND 12.0000"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestIn(t *testing.T) {
	query := `
		{"datasetId":"cn68h527dse8","workflow":[{"type":"filter","filters":[{"key":"col_10","rule":{"type":"one of","value":["charge"]}}]},{"type":"transform","transform":[{"key":"gw_count_fid","expression":{"op":"one","params":[],"as":"gw_count_fid"}}]},{"type":"view","query":[{"op":"aggregate","groupBy":["col_14","col_10"],"measures":[{"field":"gw_count_fid","agg":"sum","asFieldKey":"gw_count_fid_sum"}]}]}]}
	`
	sql := "SELECT col_14, col_10, sum(1) AS gw_count_fid_sum FROM table1 WHERE col_10 IN ('charge', 'charge') GROUP BY col_14, col_10"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}

func TestFilter(t *testing.T) {
	query := `
	{
		  "workflow": [
				{
					  "type": "filter",
					  "filters": [
							{
								  "key": "col_4",
								  "rule": {
										"type": "one of",
										"value": [
											  "0.1.6.1",
											  "0.1.7",
											  "0.1.6.0",
											  "0.1.6.2-alpha.0",
											  "0.1.7a1",
											  "0.1.7-alpha.0",
											  "0.1.7a4",
											  "0.1.7a5",
											  "0.1.9.1",
											  "0.1.6.2",
											  "0.1.6.1a6",
											  "0.1.8",
											  "0.1.7a3",
											  "0.1.9",
											  "0.1.10",
											  "0.1.11",
											  "0.1.8.dev.1",
											  "0.1.8.dev.0",
											  "0.2.0a1"
										]
								  }
							}
					  ]
				},
				{
					  "type": "transform",
					  "transform": [
							{
								  "key": "gw_count_fid",
								  "expression": {
										"op": "one",
										"params": [],
										"as": "gw_count_fid"
								  }
							}
					  ]
				},
				{
					  "type": "view",
					  "query": [
							{
								  "op": "aggregate",
								  "groupBy": [
										"col_14",
										"col_4",
										"col_4"
								  ],
								  "measures": [
										{
											  "field": "gw_count_fid",
											  "agg": "sum",
											  "asFieldKey": "gw_count_fid_sum"
										}
								  ]
							}
					  ]
				}
		  ]
	}
	`
	sql := "SELECT col_14, col_4, col_4, sum(1) AS gw_count_fid_sum FROM table1 WHERE col_4 IN ('0.1.6.1', '0.1.7', '0.1.6.0', '0.1.6.2-alpha.0', '0.1.7a1', '0.1.7-alpha.0', '0.1.7a4', '0.1.7a5', '0.1.9.1', '0.1.6.2', '0.1.6.1a6', '0.1.8', '0.1.7a3', '0.1.9', '0.1.10', '0.1.11', '0.1.8.dev.1', '0.1.8.dev.0', '0.2.0a1') GROUP BY col_14, col_4, col_4"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestSpecial(t *testing.T) {
	query := `
{"workflow": [{"type": "view", "query": [{"op": "aggregate", "groupBy": ["c2Vhc29uXzI="], "measures": []}]}]}
	`
	sql := "SELECT c2Vhc29uXzI= FROM table1 GROUP BY \"c2Vhc29uXzI=\""
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestAggBinCount(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "transform",
		  "transform": [
			{
			  "fid": "gw_ZM8H",
			  "expression": {
				"op": "binCount",
				"as": "gw_ZM8H",
				"params": [
				  {
					"type": "field",
					"value": "col_3"
				  }
				]
			  }
			}
		  ]
		},
		{
		  "type": "view",
		  "query": [
			{
			  "op": "raw",
			  "fields": [
				"col_0",
				"gw_ZM8H"
			  ]
			}
		  ]
		}
	  ]
	}
	`
	sql := "SELECT col_0, least((col_3 - \"min_gw_ZM8H\") / ((\"max_gw_ZM8H\" - \"min_gw_ZM8H\") / 10), 9) + 1 AS \"gw_ZM8H\" FROM (SELECT *, max(col_3) OVER () AS \"max_gw_ZM8H\", min(col_3) OVER () AS \"min_gw_ZM8H\" FROM table1) AS kanaries_sub_query"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestSumLog2(t *testing.T) {
	query := `
	{
		"workflow": [{
			"type": "transform",
			"transform": [{
				"key": "gw_62jy",
				"expression": {
					"op": "bin",
					"as": "gw_62jy",
					"params": [{
						"type": "field",
						"value": "col_29"
					}]
				}
			}, {
				"key": "gw_MMjF",
				"expression": {
					"op": "log2",
					"as": "gw_MMjF",
					"params": [{
						"type": "field",
						"value": "col_21"
					}]
				}
			}]
		}, {
			"type": "view",
			"query": [{
				"op": "aggregate",
				"groupBy": ["gw_62jy"],
				"measures": [{
					"field": "gw_MMjF",
					"agg": "sum",
					"asFieldKey": "gw_MMjF_sum"
				}]
			}]
		}]
	}`
	sql := "SELECT min_gw_62jy + (least(floor((col_29 - min_gw_62jy) / ((max_gw_62jy - min_gw_62jy) / 10.0)), 9) * ((max_gw_62jy - min_gw_62jy) / 10.0)) AS gw_62jy, sum(log(col_21) / log(2)) AS \"gw_MMjF_sum\" FROM (SELECT *, max(col_29) OVER () AS max_gw_62jy, min(col_29) OVER () AS min_gw_62jy FROM table1) AS kanaries_sub_query GROUP BY gw_62jy"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestAlias(t *testing.T) {
	query := `
	{
		"workflow": [{
			"type": "transform",
			"transform": [{
				"key": "gw_W4Tf",
				"expression": {
					"op": "bin",
					"as": "gw_W4Tf",
					"params": [{
						"type": "field",
						"value": "col_6"
					}]
				}
			}, {
				"key": "gw_vH37",
				"expression": {
					"op": "log2",
					"as": "gw_vH37",
					"params": [{
						"type": "field",
						"value": "col_11"
					}]
				}
			}]
		}, {
			"type": "view",
			"query": [{
				"op": "aggregate",
				"groupBy": ["gw_W4Tf"],
				"measures": [{
					"field": "gw_vH37",
					"agg": "sum",
					"asFieldKey": "gw_vH37_sum"
				}]
			}]
		}]
	}
	`
	sql := "SELECT \"min_gw_W4Tf\" + (least(floor((col_6 - \"min_gw_W4Tf\") / ((\"max_gw_W4Tf\" - \"min_gw_W4Tf\") / 10.0)), 9) * ((\"max_gw_W4Tf\" - \"min_gw_W4Tf\") / 10.0)) AS \"gw_W4Tf\", sum(log(col_11) / log(2)) AS \"gw_vH37_sum\" FROM (SELECT *, max(col_6) OVER () AS \"max_gw_W4Tf\", min(col_6) OVER () AS \"min_gw_W4Tf\" FROM table1) AS kanaries_sub_query GROUP BY \"gw_W4Tf\""
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestBin(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "transform",
		  "transform": [
			{
			  "fid": "gw_Kr7j",
			  "expression": {
				"op": "bin",
				"as": "gw_Kr7j",
				"params": [
				  {
					"type": "field",
					"value": "col_1"
				  }
				]
			  }
			}
		  ]
		},
		{
		  "type": "view",
		  "query": [
			{
			  "op": "raw",
			  "fields": [
				"gw_Kr7j",
				"col_13"
			  ]
			}
		  ]
		}
	  ]
	}
	`
	sql := "SELECT \"min_gw_Kr7j\" + (least(floor((col_1 - \"min_gw_Kr7j\") / ((\"max_gw_Kr7j\" - \"min_gw_Kr7j\") / 10.0)), 9) * ((\"max_gw_Kr7j\" - \"min_gw_Kr7j\") / 10.0)) AS \"gw_Kr7j\", col_13 FROM (SELECT *, max(col_1) OVER () AS \"max_gw_Kr7j\", min(col_1) OVER () AS \"min_gw_Kr7j\" FROM table1) AS kanaries_sub_query"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}

func TestBinCount(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "transform",
		  "transform": [
			{
			  "fid": "gw_ZM8H",
			  "expression": {
				"op": "binCount",
				"as": "gw_ZM8H",
				"params": [
				  {
					"type": "field",
					"value": "col_3"
				  }
				]
			  }
			}
		  ]
		},
		{
		  "type": "view",
		  "query": [
			{
			  "op": "raw",
			  "fields": [
				"gw_ZM8H"
			  ]
			}
		  ]
		}
	  ]
	}
	`
	sql := "SELECT least((col_3 - \"min_gw_ZM8H\") / ((\"max_gw_ZM8H\" - \"min_gw_ZM8H\") / 10), 9) + 1 AS \"gw_ZM8H\" FROM (SELECT *, max(col_3) OVER () AS \"max_gw_ZM8H\", min(col_3) OVER () AS \"min_gw_ZM8H\" FROM table1) AS kanaries_sub_query"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestLog10(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "transform",
		  "transform": [
			{
			  "fid": "gw_oZBh",
			  "expression": {
				"op": "log10",
				"as": "gw_oZBh",
				"params": [
				  {
					"type": "field",
					"value": "col_1"
				  }
				]
			  }
			}
		  ]
		},
		{
		  "type": "view",
		  "query": [
			{
			  "op": "raw",
			  "fields": [
				"gw_oZBh",
				"col_1"
			  ]
			}
		  ]
		}
	  ]
	}`
	sql := "SELECT log10(col_1) AS \"gw_oZBh\", col_1 FROM table1"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}

func TestLog2(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "transform",
		  "transform": [
			{
			  "fid": "gw_f23i",
			  "expression": {
				"op": "log2",
				"as": "gw_f23i",
				"params": [
				  {
					"type": "field",
					"value": "col_1"
				  }
				]
			  }
			}
		  ]
		},
		{
		  "type": "view",
		  "query": [
			{
			  "op": "raw",
			  "fields": [
				"gw_f23i",
				"col_1"
			  ]
			}
		  ]
		}
	  ]
	}`
	sql := "SELECT log(col_1) / log(2) AS gw_f23i, col_1 FROM table1"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}

func TestStdev(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "aggregate",
			  "groupBy": [
				"col_0"
			  ],
			  "measures": [
				{
				  "field": "col_3",
				  "agg": "stdev",
				  "asFieldKey": "col_3_stdev"
				}
			  ]
			}
		  ]
		}
	  ]
	}
	`
	sql := "SELECT col_0, stddev(col_3) AS col_3_stdev FROM table1 GROUP BY col_0"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestVariance(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "aggregate",
			  "groupBy": [
				"col_0"
			  ],
			  "measures": [
				{
				  "field": "col_3",
				  "agg": "variance",
				  "asFieldKey": "col_3_variance"
				}
			  ]
			}
		  ]
		}
	  ]
	}`
	sql := "SELECT col_0, variance(col_3) AS col_3_variance FROM table1 GROUP BY col_0"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestMax(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "aggregate",
			  "groupBy": [
				"col_0"
			  ],
			  "measures": [
				{
				  "field": "col_3",
				  "agg": "max",
				  "asFieldKey": "col_3_max"
				}
			  ]
			}
		  ]
		}
	  ]
	}`
	sql := "SELECT col_0, max(col_3) AS col_3_max FROM table1 GROUP BY col_0"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestMin(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "aggregate",
			  "groupBy": [
				"col_0"
			  ],
			  "measures": [
				{
				  "field": "col_3",
				  "agg": "min",
				  "asFieldKey": "col_3_min"
				}
			  ]
			}
		  ]
		}
	  ]
	}`
	sql := "SELECT col_0, min(col_3) AS col_3_min FROM table1 GROUP BY col_0"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func TestCount(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "aggregate",
			  "groupBy": [
				"col_0"
			  ],
			  "measures": [
				{
				  "field": "col_3",
				  "agg": "count",
				  "asFieldKey": "col_3_count"
				}
			  ]
			}
		  ]
		}
	  ]
	}`
	sql := "SELECT col_0, count(col_3) AS col_3_count FROM table1 GROUP BY col_0"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}

func TestMedian(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "aggregate",
			  "groupBy": [
				"col_0"
			  ],
			  "measures": [
				{
				  "field": "col_3",
				  "agg": "median",
				  "asFieldKey": "col_3_median"
				}
			  ]
			}
		  ]
		}
	  ]
	}`
	sql := "SELECT col_0, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col_3) AS col_3_median FROM table1 GROUP BY col_0"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}

func TestFindAll(t *testing.T) {
	query := `{
	  "workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "aggregate",
			  "groupBy": [],
			  "measures": [
				{
				  "field": "*",
				  "agg": "count",
				  "asFieldKey": "count"
				}
			  ]
			}
		  ]
		}
	  ]
	}`
	sql := "SELECT count(*) AS count FROM table1"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}

func TestSumFuc(t *testing.T) {
	query := `{
	"workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "aggregate",
			  "groupBy": [
				"col_0"
			  ],
			  "measures": [
				{
				  "field": "col_3",
				  "agg": "sum",
				  "asFieldKey": "col_3_sum"
				}
			  ]
			}
		  ]
		}
	  ]
	}`
	sql := "SELECT col_0, sum(col_3) AS col_3_sum FROM table1 GROUP BY col_0"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}

func TestMean(t *testing.T) {
	query := `
	{
	  "workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "aggregate",
			  "groupBy": [
				"col_0"
			  ],
			  "measures": [
				{
				  "field": "col_3",
				  "agg": "mean",
				  "asFieldKey": "col_3_mean"
				}
			  ]
			}
		  ]
		}
	  ]
	}
	`
	sql := "SELECT col_0, avg(col_3) AS col_3_mean FROM table1 GROUP BY col_0"
	dataset := Dataset{
		Source: "table1",
		Type:   common.DatasetTypeTable,
	}
	err := testParser(query, sql, dataset, t)
	if err != nil {
		t.Error(err)
	}
}
func testParser(query, sql string, dataset Dataset, t *testing.T) error {
	baseParser := BaseParser{}
	var payload GraphicWalkerDSL
	err := json.Unmarshal([]byte(query), &payload)
	if err != nil {
		t.Error(fmt.Sprintf("QueryDataset error: %v", err))
		payload.Workflow = make([]IBaseQuery, 0)
	}
	res, _ := baseParser.Parse(dataset, payload)
	if strings.TrimSpace(res) != strings.TrimSpace(sql) {
		return errors.New(fmt.Sprintf("got %s \n"+
			"expect %s \n", res, sql))
	}
	return nil
}
