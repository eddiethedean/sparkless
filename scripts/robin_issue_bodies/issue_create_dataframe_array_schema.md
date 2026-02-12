## Summary

PySpark accepts DataFrame creation with list/array column data (e.g. `spark.createDataFrame([{"name": "a", "vals": [1,2,3]}])`). Robin's `create_dataframe_from_rows` rejects schema dtype `'list'` or `'array'` for a column, raising `RuntimeError: create_dataframe_from_rows: unsupported type 'list' for column 'vals'` (or `'array'`). This blocks posexplode/explode and any API that needs array columns.

## Current behavior (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")

data = [{"name": "a", "vals": [1, 2, 3]}]
# RuntimeError: create_dataframe_from_rows: unsupported type 'list' for column 'vals'
# (same with ("vals", "array"))
df = create_df(data, [("name", "string"), ("vals", "list")])
df.collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"name": "a", "vals": [1, 2, 3]}])
out = df.collect()  # succeeds; vals is array column
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/08_create_dataframe_array_column.py
```

Robin fails with `RuntimeError: create_dataframe_from_rows: unsupported type 'list' for column 'vals'` (and similarly for `'array'`). PySpark succeeds.

## Context

- Sparkless posexplode/explode tests fail with "invalid series dtype: expected `List`, got `str`" because array columns cannot be created with list/array schema in Robin; they fall back to string.
- Robin version: 0.8.0+ (sparkless pyproject.toml).
