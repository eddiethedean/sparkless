## Summary

When creating a DataFrame from non-empty rows, the robin-sparkless crate returns **schema must not be empty when rows are not empty**. In PySpark, `createDataFrame(data, schema)` accepts non-empty data with an explicit schema; if the schema is provided (even as a list of column names), the call succeeds. The crate appears to reject the case where the schema passed from the Sparkless PyO3 layer is empty while the row data is not.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only, using the robin-sparkless crate via PyO3):

```python
from sparkless import SparkSession
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

# Explicit schema, non-empty rows – PySpark accepts this
schema = StructType([
    StructField("name", StringType()),
    StructField("value", IntegerType()),
])
data = [{"name": "a", "value": 1}, {"name": "b", "value": 2}]

df = spark.createDataFrame(data, schema)
print(df.collect())
```

**Observed:** `ValueError: create_dataframe_from_rows failed: create_dataframe_from_rows: schema must not be empty when rows are not empty`

(This can occur when the Sparkless layer serializes the schema to the crate in a form the crate treats as empty, or when a code path sends rows with an empty schema list.)

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

schema = StructType([
    StructField("name", StringType()),
    StructField("value", IntegerType()),
])
data = [{"name": "a", "value": 1}, {"name": "b", "value": 2}]

df = spark.createDataFrame(data, schema)
rows = df.collect()
assert len(rows) == 2
assert rows[0]["name"] == "a" and rows[0]["value"] == 1
print(rows)
```

**Expected:** DataFrame is created and `collect()` returns the two rows. No error when schema is non-empty and data is non-empty.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.0** via PyO3 extension.
- PySpark 3.x.
