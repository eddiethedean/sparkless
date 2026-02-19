# [PySpark parity] create_dataframe_from_rows: unsupported type map<string,string>

## Summary

When creating a DataFrame from rows that include **map columns** (e.g. Python dicts), the robin-sparkless crate fails with **create_dataframe_from_rows: unsupported type 'map<string,string>' for column '...'**. PySpark `createDataFrame` accepts dict values for map-typed columns. The crate does not yet support map type in `create_dataframe_from_rows`.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only, using the robin-sparkless crate via PyO3):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F
from sparkless.sql.types import StructType, StructField, StringType, IntegerType, MapType

spark = SparkSession.builder.appName("repro").getOrCreate()

# Map column: keys and values as strings (PySpark accepts dict)
schema = StructType([
    StructField("id", IntegerType()),
    StructField("m", MapType(StringType(), StringType())),
])
data = [
    {"id": 1, "m": {"a": "x", "b": "y"}},
    {"id": 2, "m": {"c": "z"}},
]

df = spark.createDataFrame(data, schema)
print(df.collect())
```

**Observed:** `ValueError: create_dataframe_from_rows failed: create_dataframe_from_rows: unsupported type 'map<string,string>' for column 'm'`

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType

spark = SparkSession.builder.appName("repro").getOrCreate()

schema = StructType([
    StructField("id", IntegerType()),
    StructField("m", MapType(StringType(), StringType())),
])
data = [
    {"id": 1, "m": {"a": "x", "b": "y"}},
    {"id": 2, "m": {"c": "z"}},
]

df = spark.createDataFrame(data, schema)
rows = df.collect()
assert rows[0]["id"] == 1 and rows[0]["m"] == {"a": "x", "b": "y"}
assert rows[1]["id"] == 2 and rows[1]["m"] == {"c": "z"}
print(rows)
```

**Expected:** DataFrame is created; `m` is a map column; `collect()` returns rows with Python dicts for `m`. No error.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.0** via PyO3 extension.
- PySpark 3.x.

## Request

Support map type in `create_dataframe_from_rows` (e.g. accept dict or equivalent serialization for map columns), matching PySpark semantics for map types.
