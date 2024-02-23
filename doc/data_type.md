# Data Type

## Number

## String

## DateTime

In order to avoid time zone issues and differences in time types across various databases (such as DuckDB having timestamp_s, timestamp with timezone, etc.), all datetime types parsed in the current version will be converted to Unix millisecond timestamps, masking the concept of time zones.

If the underlying data includes time zone information, such as in the case of timestamp with timezone, it will be converted to Coordinated Universal Time (UTC). If it does not include time zone information, it will be treated directly as UTC time.
