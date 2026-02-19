## Summary

When creating a DataFrame from rows that include **array columns** (e.g. Python lists), the robin-sparkless crate can fail with **array column value must be null or array**. PySpark `createDataFrame` accepts Python lists for array-typed columns. The crate may expect a different representation (e.g. JSON array string or a specific serialization) or reject valid list payloads.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only, using the robin-sparkless crate via PyO3):

```python
from sparkless import SparkSession
from sparkless.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

schema = StructType([
    StructField("id", StringType()),
    StructField("arr", ArrayType(IntegerType())),
])
# PySpark accepts list as array column value
data = [
    {"id": "x", "arr": [1, 2, 3]},
    {"id": "y", "arr": [4, 5]},
]

df = spark.createDataFrame(data, schema)
print(df.collect())
```

**Observed:** `ValueError: create_dataframe_from_rows failed: array column value must be null or array`

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

schema = StructType([
    StructField("id", StringType()),
    StructField("arr", ArrayType(IntegerType())),
])
data = [
    {"id": "x", "arr": [1, 2, 3]},
    {"id": "y", "arr": [4, 5]},
]

df = spark.createDataFrame(data, schema)
rows = df.collect()
assert rows[0]["id"] == "x" and rows[0]["arr"] == [1, 2, 3]
assert rows[1]["id"] == "y" and rows[1]["arr"] == [4, 5]
print(rows)
```

**Expected:** DataFrame is created; `arr` is an array column; `collect()` returns rows with Python lists for `arr`. No error.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.0** via PyO3 extension.
- PySpark 3.x.

## Request

Accept Python list (or the JSON/serialized form Sparkless sends for array columns) as valid array column values in `create_dataframe_from_rows`, matching PySpark semantics for array types.
