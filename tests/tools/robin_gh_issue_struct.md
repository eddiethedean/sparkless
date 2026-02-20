# [PySpark parity] create_dataframe_from_rows: struct value must be object or array

## Summary

When creating a DataFrame from rows that include **struct columns** (e.g. tuple or dict for nested fields), the robin-sparkless crate can fail with **struct value must be object (by field name) or array (by position)**. PySpark `createDataFrame` accepts both dict (by field name) and tuple/list (by position) for struct-typed columns. The crate may not accept the same serialized form that Sparkless sends for positional struct values.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only, using the robin-sparkless crate via PyO3):

```python
from sparkless import SparkSession
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

schema = StructType([
    StructField("id", StringType()),
    StructField("nested", StructType([
        StructField("a", IntegerType()),
        StructField("b", StringType()),
    ])),
])
# Struct as tuple (positional) - PySpark accepts this
data = [{"id": "x", "nested": (1, "y")}]
df = spark.createDataFrame(data, schema)
print(df.collect())
```

**Observed:** `ValueError: create_dataframe_from_rows failed: struct value must be object (by field name) or array (by position) ...`

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

schema = StructType([
    StructField("id", StringType()),
    StructField("nested", StructType([
        StructField("a", IntegerType()),
        StructField("b", StringType()),
    ])),
])
# Both dict and tuple are accepted
df1 = spark.createDataFrame([{"id": "x", "nested": (1, "y")}], schema)
df2 = spark.createDataFrame([{"id": "x", "nested": {"a": 1, "b": "y"}}], schema)
df1.collect()  # OK
df2.collect()  # OK
```

**Expected:** DataFrame is created; struct column accepts both dict (by name) and tuple/list (by position). No error.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.2** via PyO3 extension.
- PySpark 3.x.

## Request

Accept struct values in the same formats PySpark does (object by field name, array by position) in `create_dataframe_from_rows`, or document the exact JSON/serialized shape expected so that Sparkless (and other callers) can send structs correctly.
