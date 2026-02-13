# [Parity] hour(col) missing

## Summary

PySpark provides `F.hour(col)` to extract hour from timestamp. Robin-sparkless does not expose `hour` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"ts": "2024-02-01 14:30:00"}], [("ts", "string")])
df.select(rs.hour(rs.col("ts")).alias("h")).collect()  # AttributeError: no hour
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("2024-02-01 14:30:00",)], ["ts"])
df.select(F.hour(F.to_timestamp("ts")).alias("h")).collect()  # 14
```

## Expected

Module exposes `hour(col)`.

## Actual

`robin_sparkless` has no `hour` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/48_hour.py`
