## Summary

PySpark's `F.posexplode()` accepts either a column name (string) or a Column: `F.posexplode("Values")` and `F.posexplode(F.col("Values"))` both work. Robin's `posexplode()` raises `TypeError: argument 'col': 'str' object cannot be converted to 'Column'` when given a string column name; it appears to require a Column only.

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
data = [{"Name": "Alice", "Values": [10, 20]}, {"Name": "Bob", "Values": [30, 40]}]
df = create_df(data, [("Name", "string"), ("Values", "array")])
# Fails: TypeError: argument 'col': 'str' object cannot be converted to 'Column'
out = df.select("Name", rs.posexplode("Values").alias("pos", "val")).collect()
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
df = spark.createDataFrame(
    [{"Name": "Alice", "Values": [10, 20]}, {"Name": "Bob", "Values": [30, 40]}]
)
# PySpark accepts string column name
out = df.select("Name", F.posexplode("Values").alias("pos", "val")).collect()
# Succeeds: 4 rows (pos, val for each element)
```

## Expected

`posexplode(column_name: str)` should be accepted for PySpark parity, in addition to `posexplode(column: Column)`.

## Actual

Robin raises: `TypeError: argument 'col': 'str' object cannot be converted to 'Column'`.

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo (with robin-sparkless and pyspark installed):

```bash
python scripts/robin_parity_repros/11_posexplode_array.py
```

Robin fails, PySpark OK (exit code 1 indicates parity gap).

## Context

Sparkless v4 uses Robin as the execution engine. This API parity gap was identified when running the repro script; related upstream issue for posexplode: #264. See [docs/robin_sparkless_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/robin_sparkless_issues.md).
