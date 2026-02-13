# [Parity] last_day(col) missing

## Summary

PySpark provides `F.last_day(col)` to get the last day of the month. Robin-sparkless does not expose `last_day` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"dt": "2024-02-15"}], [("dt", "string")])
df.select(rs.last_day(rs.col("dt")).alias("ld")).collect()  # AttributeError: no last_day
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("2024-02-15",)], ["dt"])
df.select(F.last_day(F.to_date("dt")).alias("ld")).collect()  # 2024-02-29
```

## Expected

Module exposes `last_day(col)`.

## Actual

`robin_sparkless` has no `last_day` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/49_last_day.py`
