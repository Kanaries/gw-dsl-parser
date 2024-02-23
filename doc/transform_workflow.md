# Transform

## Cast

For most data types, there's usually no need to be aware of the data type, such as converting a number to a string or a string to a number, these conversions are implicitly handled by most databases.

However, when dealing with time data, additional attention is required. We may encounter cases where we need to convert a number (milliseconds timestamp) to a string, a string (formatted like yyyy-mm-dd) to a string.

> See details about date type in [Data Type](./data_type.md)

```json
{
  "workflow": [
    {
      "type": "transform",
      "transform": [
        {
          "expression": {
            "op": "cast",
            "as": "gw_MRzB",
            "params": [
              {
                "type": "field",
                "value": "gw_MRzB"
              },
              {
                "type": "type",
                "value": "datetime" //datetime  string number
              }
            ]
          }
        }
      ]
    }
  ]
}
```
