# [Parity] to_date(col [, format]) missing

## Summary

PySpark provides `F.to_date(col, format=None)` to parse string to date. Robin-sparkless does not expose `to_date` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"s": "2024-02-15"}], [("s", "string")])
df.select(rs.to_date(rs.col("s")).alias("d")).collect()  # AttributeError: no to_date
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("2024-02-15",)], ["s"])
df.select(F.to_date("s").alias("d")).collect()
```

## Expected

Module exposes `to_date(col, format=None)`.

## Actual

`robin_sparkless` has no `to_date` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/57_to_date.py`
