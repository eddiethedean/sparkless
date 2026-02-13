# [Parity] flatten(col) missing or createDataFrame array of arrays

## Summary

PySpark provides `F.flatten(col)` to flatten an array of arrays into a single array. Robin-sparkless fails to create a DataFrame with nested array (array of arrays): "json_value_to_series: unsupported bigint for Array". So flatten() cannot be tested until array-of-array data is supported.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
data = [{"nested": [[1, 2], [3, 4]]}]
df = create_df(data, [("nested", "array")])  # Fails: unsupported bigint for Array
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([([[1, 2], [3, 4]],)], "nested array<array<int>>")
df.select(F.flatten("nested").alias("flat")).collect()  # [1, 2, 3, 4]
```

## Expected

create_dataframe_from_rows accepts array of arrays; or flatten(col) exists and works on such columns.

## Actual

create_df fails for nested array data; flatten() not tested.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/52_flatten.py`
