#  Graphic Walker DSL Parser 

> The project is currently in testing & validation. Welcome the community to raise issues and contribute code.


## Introduction

![./LICENSE](https://img.shields.io/github/license/kanaries/gw-dsl-parser?style=flat-square)
[![](https://img.shields.io/badge/twitter-kanaries_data-03A9F4?style=flat-square&logo=twitter)](https://twitter.com/kanaries_data)
[![](https://img.shields.io/discord/987366424634884096?color=%237289da&label=Discord&logo=discord&logoColor=white&style=flat-square)](https://discord.gg/WWHraZ8SeV)
[![](https://img.shields.io/badge/Slack-green?style=flat-square&logo=slack&logoColor=white)](https://kanaries-community.slack.com/join/shared_invite/zt-20hho6t45-_OSDdTQamnrSnOW6C2PTgg)

This project converts Graphic Walker DSL into SQL, which is needed for connecting Graphic Walker with databases/OLAP/data services. By integrating with Graphic Walker's server-side mode, it pushes down computations to the query engine for improved performance.

* Leverage the query engine for faster processing instead of pulling all data to the application layer
* Reduce data transfer between database and application server
* Allow users to analyze data using their own databases


## Quick Start

Regarding the definition of the Graphic Walker DSL and how to integrate it, 
please refer to the Graphic Walker documentation. Here we will focus on describing how to use the SDK.


Before integrating, we need to understand two parameters: `Dataset` and `GraphicWalkerDSL`. 

- `GraphicWalkerDSLDSL`: `GraphicWalkerDSL` is the serialized DSL obtained by passing GraphicWalker directly to the backend.
- `Dataset` is your abstraction of the data source.  

For example: 
- If you want to query a Mysql table called ***"test_table"***, you need to define the `Dataset` as follows:
```go
package main

import (
	"github.com/kanaries/gw-dsl-parser/parser"
)

func main() {
	// refer to this doc on how to get the API Key: ：https://github.com/Kanaries/pygwalker/wiki/How-to-get-api-key-of-kanaries%3F
	client := parser.NewClient("ak")
	
	// define the fields of the dataset
	fields := make(map[string]parser.Field)
	fields["col_1"] = parser.Field{
		Key:  "col_1",
		Fid:  "col_1",
		Type: parser.STRING,
	}
	// construct the dataset, name it "test_table"
	dataset := parser.NewDataset("test_table", fields, "mysql")

	// query from graphic walker request
	query := `
	{
	  "workflow": [
		{
		  "type": "view",
		  "query": [
			{
			  "op": "raw",
			  "fields": [
				"col_1"
			  ]
			}
		  ]
		}
	  ]
	}
	`
	sql, err := client.Parse(dataset, query)
	if err != nil {
		println(err)
	}
	println(sql)

}

```

## Supported databases

- [x] Postgresql
- [x] DuckDB
- [x] Snowflake
- [x] MySQL
- [x] BigQuery
- [x] ClickHouse

    

## How to run in other languages

- python: https://github.com/Kanaries/gw-dsl-parser-py
- js: https://www.npmjs.com/package/@kanaries/gw-dsl-parser
## Features

- More database support ( Snowflake, ClickHouse, etc.)
- SQL syntax compatibility test


## LICENSE
Please refer to [LICENSE](https://github.com/Kanaries/gw-dsl-parser/blob/main/LICENSE).
