package parser

import (
	"testing"
)

func TestWasmRuntime(t *testing.T) {
	err := InitDuckDBParser()
	if err != nil {
		return
	}
	parser := NewDuckDBParser()
	query := `
	{
        "workflow": [{
			"type": "transform",
			"transform": [{
				"key": "gw_MRzB",
				"expression": {
					"op": "dateTimeFeature",
					"as": "gw_MRzB",
					"params": [{
						"type": "field",
						"value": "c_0"
					}, {
						"type": "value",
						"value": "week"
					},{
						"type": "format",
						"value": "timestamp"
					}]
				}
			}]
    		}, {
    			"type": "view",
    			"query": [{
    				"op": "aggregate",
    				"groupBy": ["gw_MRzB"],
    				"measures": [{
    					"field": "c_11",
    					"agg": "sum",
    					"asFieldKey": "c_11_sum"
    				}]
    			}]
    		}]
    	}`

	res, err := parser.Parse(Dataset{
		Type:   "table",
		Source: "t_1222",
	}, []byte(query), "{\"t_1222\":[{\"key\":\"c_0\",\"type\":\"datetime\",\"name\":\"c_0\",\"fid\":\"c_0\"}]}")
	if err != nil {
		return
	}
	println(res)
}
