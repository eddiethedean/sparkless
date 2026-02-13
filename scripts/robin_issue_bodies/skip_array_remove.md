# [Parity] array_remove(col, element) missing

## Summary

PySpark provides `F.array_remove(col, element)` to remove all elements equal to element from array. Robin-sparkless does not expose `array_remove` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"arr": [1, 2, 2, 3]}], [("arr", "array")])
df.select(rs.array_remove(rs.col("arr"), 2).alias("out")).collect()  # AttributeError: no array_remove
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([([1, 2, 2, 3],)], "arr array<int>")
df.select(F.array_remove("arr", 2).alias("out")).collect()  # [1, 3]
```

## Expected

Module exposes `array_remove(col, element)`.

## Actual

`robin_sparkless` has no `array_remove` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/51_array_remove.py`
