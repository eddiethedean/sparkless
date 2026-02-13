# [Parity] explode_outer() wrong behavior — lengths don't match when input has NULL/empty

## Summary

PySpark's `F.explode_outer(col)` produces one row with NULLs for the exploded column when the input is NULL or empty (OUTER semantics). Robin-sparkless appears to have `explode_outer` but returns incorrect results: RuntimeError "lengths don't match: Series length 2 doesn't match the DataFrame height of 3" when one row has array [1,2] and another has NULL.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
data = [{"name": "a", "arr": [1, 2]}, {"name": "b", "arr": None}]
df = create_df(data, [("name", "string"), ("arr", "array")])
out = df.select("name", rs.explode_outer(rs.col("arr")).alias("v"))
out.collect()  # RuntimeError: lengths don't match
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", [1, 2]), ("b", None)], "name string, arr array<int>")
out = df.select("name", F.explode_outer("arr").alias("v"))
out.collect()  # [Row(name='a', v=1), Row(name='a', v=2), Row(name='b', v=None)]
```

## Expected

- explode_outer produces one row per element; for NULL or empty array, one row with name and v=NULL.
- No length mismatch; row count = sum of (len(arr) if arr else 1) per input row.

## Actual

- RuntimeError: lengths don't match: Series length 2 doesn't match the DataFrame height of 3.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/37_explode_outer.py`
