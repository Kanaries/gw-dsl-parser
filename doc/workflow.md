# Workflow

GraphicWalker will apply an asynchronous workflow to compute the data to the view data which is directly used by the renderer.
The computation workflow is formed by a series of data queries, which describe how to compute the view data from the raw data.
The computation workflow is not only used in the rendering phase, but also used in cases such as preview table, and filter editor when GraphicWalker needs to get the necessary statistics from the raw data.

## Computation Workflow

The overall workflow process is similar to MapReduce, where each flow involves processing the results of the previous workflow node.
The difference is that you must use 'view' or 'sort' as the ending of the workflow

### 1. View Query (required)

The view query is used to shape the data into a view data. It contains a list of view queries, each of which contains a view-level operation. A workflow must contain at least one view-level operation, which describes the structure of the view data.

At the moment, there are 2 view-level operations: aggregate, and raw.

#### 1.1 Raw Query

Use the raw operation in the view query when you want the data not to be aggregated. The raw operation contains a list of fields to be included in the view data.The schema of the view query is

```json
{
  "workflow": [
    {
      "type": "view",
      "query": [
        {
          "op": "raw",
          "fields": ["col_2"]
        }
      ]
    }
  ]
}
```

    SQL: SELECT "col_2" FROM "table"

#### 1.2 Aggregate Query

Use the aggregate operation in the view query when you want the data to be aggregated. The aggregate operation contains a list of measures to be aggregated by a specified aggregation function with the group-by fields.

The currently supported aggregation values include:

- `sum`
- `min`
- `max`
- `count`
- `median`
- `variance`
- `stdev`
- `mean`
- `distinctCount`

The schema of the view query is

```json
{
  "workflow": [
    {
      "type": "view",
      "query": [
        {
          "op": "aggregate",
          "groupBy": ["col_0"],
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
```

    SQL: SELECT "col_0", stddev("col_3") AS "col_3_stdev" FROM "table" GROUP BY "col_0"

### Filter Query (optional)

The filter query is used to filter the raw data. It contains a list of filter fields, each of which contains a filter rule.

The currently supported filter rule include:

- `range`
- `one of`
- `temporal range`

The schema of the filter query is

```json
{
  "workflow": [
    {
      "type": "filter",
      "filters": [
        {
          "fid": "col_1",
          "rule": {
            "type": "range",
            "value": [4.1387, 12.1]
          }
        }
      ]
    },
    {
      "type": "view",
      "query": [
        {
          "op": "raw",
          "fields": ["col_1"]
        }
      ]
    }
  ]
}
```

### Transform Query\*\* (optional)

> Details about transform query in [Transform](./transform_workflow.md)

The transform query is used to resolve the field calculations. It contains a list of transform fields, each of which contains an expression.

The currently supported transform expression op include:

- `one`
- `bin`
- `binCount`
- `log2`
- `log10`
- `log`
- `dateTimeDrill`
- `dateTimeFeature`
- `expr(beta)`
- `panit`
- `cast`

  The schema of the transform query is

```json
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
          "fields": ["gw_f23i", "col_1"]
        }
      ]
    }
  ]
}
```

`SQL: SELECT log("col_1") / log(2) AS "gw_f23i", "col_1" FROM "table"`

## DataView Workflow

DataView is used when data needs to be processed, for example, when there is a raw table named 'default,'
and we want to perform operations on it using SQL to create a new view.
Then, based on this view, we can perform analysis.
We can define the workflow as follows

```json
{
  "workflow": [
    {
      "type": "view",
      "query": [
        {
          "op": "aggregate",
          "groupBy": ["id"]
        }
      ]
    }
  ],
  "dataview": [
    {
      "type": "sql",
      "query": [
        {
          "sql": "select id, name1, name2 from (select 1 as id, * from default)",
          "fidMap": {
            "name1": "timestamp_s_col",
            "name2": "bigint_col"
          }
        }
      ]
    }
  ]
}
```

`SQL: SELECT "id" FROM (SELECT id, name1, name2 FROM (SELECT 1 AS id, * FROM (SELECT "timestamp_s_col" AS "name1", "bigint_col" AS "name2" FROM "table_1") AS "k_gw_write_view") AS "k_gw_review_default") AS "view_0" GROUP BY "id"`
