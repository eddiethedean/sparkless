# [Parity] inline(array of structs) and createDataFrame with array of structs

## Summary

PySpark supports (1) creating a DataFrame with a column that is an array of structs, and (2) `F.inline(col)` to explode that array into a table (one row per struct). Robin-sparkless fails to create such a DataFrame: "json_value_to_series: unsupported bigint for Object" when passing data with array of structs. So inline() cannot be tested until create_dataframe_from_rows accepts array-of-struct schema/data.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
data = [{"id": 1, "items": [{"x": 10, "y": "a"}, {"x": 20, "y": "b"}]}]
df = create_df(data, [("id", "int"), ("items", "array")])  # Fails: unsupported bigint for Object
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
spark = SparkSession.builder.master("local[1]").getOrCreate()
schema = StructType([
    StructField("id", IntegerType()),
    StructField("items", ArrayType(StructType([
        StructField("x", IntegerType()), StructField("y", StringType()),
    ]))),
])
df = spark.createDataFrame([(1, [{"x": 10, "y": "a"}, {"x": 20, "y": "b"}])], schema)
df.select(F.inline("items")).collect()  # [Row(x=10, y='a'), Row(x=20, y='b')]
```

## Expected

- create_dataframe_from_rows accepts schema with array of structs and data with list of dicts per row.
- inline(col) explodes array of structs into rows with struct fields as columns.

## Actual

- create_df fails: json_value_to_series: unsupported bigint for Object {"x": Number(10), "y": String("a")}.
- inline() not tested because DataFrame cannot be created.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/38_inline.py`
