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
- If you want to query a PG table called ***"student"***, you need to construct ***"dataset"***: ` type = 'table', source = 'student' `. 
- If source is sub query like `select * from student limit 10`, then you can do:  ` type = 'sub_query', source = 'select * from student limit 10' `.

```go
package main

import (
	"encoding/json"
	dsl "github.com/kanaries/gw-dsl-parser/parser"
)

func main() {
	// Based on the database type you need to connect to,
	// construct the corresponding parser: DuckDB & postgresql & CubeJS
	parser := dsl.NewPgParser()

	// Construct the dataset
	dataset := dsl.Dataset{
		Type:   "table",
		Source: "student",
	}

	// Construct the GraphicWalkerDSL
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
	var graphicWalkerDSL dsl.GraphicWalkerDSL
	_ = json.Unmarshal([]byte(query), &graphicWalkerDSL)
	res, _ := parser.Parse(dataset, graphicWalkerDSL)
	println(res) // SELECT col_2 FROM student
}

```

## Supported databases

- [x] Postgresql
- [x] DuckDB
- [x] CubeJS
    

## How to run in other languages
We provide a WebAssembly compiled version that you can execute with the following commands:
    
```bash
GOOS=js GOARCH=wasm go build -o main.wasm wasm_main.go
```

Or you can use our precompiled wasm build artifact

This loads the WebAssembly module, instantiates it, and calls the 'main' export to execute it.
me

## Features

- More database support ( Snowflake, ClickHouse, etc.)
- SQL syntax compatibility test


## LICENSE
Please refer to [LICENSE](https://github.com/Kanaries/gw-dsl-parser/blob/main/LICENSE).
