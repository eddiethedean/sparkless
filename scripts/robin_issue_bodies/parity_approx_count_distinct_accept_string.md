# [Parity] approx_count_distinct() should accept column name (string) as well as Column

## Summary

PySpark's `F.approx_count_distinct()` accepts either a column name (string) or a Column: `F.approx_count_distinct("value")` and `F.approx_count_distinct(F.col("value"))` both work. Robin's `approx_count_distinct()` raises `TypeError: argument 'col': 'str' object cannot be converted to 'Column'` when given a string column name; it currently requires a Column only.

## Robin reproduction (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
data = [
    {"k": "A", "value": 1},
    {"k": "A", "value": 10},
    {"k": "A", "value": 1},
    {"k": "B", "value": 5},
    {"k": "B", "value": 5},
]
df = create_df(data, [("k", "string"), ("value", "int")])

# Fails: TypeError: argument 'col': 'str' object cannot be converted to 'Column'
result = df.agg(rs.approx_count_distinct("value"))
# result = df.agg(rs.approx_count_distinct("value", rsd=0.01))  # same error with rsd

# Workaround: must pass Column
result = df.agg(rs.approx_count_distinct(rs.col("value")))  # OK
```

## PySpark equivalent (works with string)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
df = spark.createDataFrame(
    [("A", 1), ("A", 10), ("A", 1), ("B", 5), ("B", 5)], ["k", "value"]
)

# PySpark accepts string column name
result = df.agg(F.approx_count_distinct("value"))
result.collect()  # [Row(approx_count_distinct(value)=3)]

# Also works with rsd
result = df.agg(F.approx_count_distinct("value", rsd=0.01))
result.collect()  # [Row(approx_count_distinct(value)=3)]
```

## Expected

`approx_count_distinct(col, rsd=None)` should accept `col` as either:

- a **string** (column name), e.g. `approx_count_distinct("value")`, or  
- a **Column**, e.g. `approx_count_distinct(rs.col("value"))`.

For parity with PySpark, when `col` is a string, the implementation can treat it as `col(col)` internally.

## Actual

Robin raises: `TypeError: argument 'col': 'str' object cannot be converted to 'Column'` when the first argument is a string. Callers must use `rs.col("value")` instead of `"value"`.

## Context

- Robin-sparkless now exposes `approx_count_distinct` (see #297); this issue is about the **argument type** only.
- Sparkless v4 uses Robin as the execution engine and currently works around this by wrapping `approx_count_distinct` to convert string to `F.col(name)` before calling Robin. Upstream support would remove the need for that shim and improve drop-in PySpark compatibility.
