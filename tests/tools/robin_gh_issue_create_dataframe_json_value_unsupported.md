# [PySpark parity] create_dataframe_from_rows: json_value_to_series unsupported types

## Summary

When creating a DataFrame from rows that include **array columns** with element types such as **boolean**, **long**, **int**, or certain **string** formats, the robin-sparkless crate fails with **json_value_to_series: unsupported boolean/long/int/string for ...** or **create_dataframe_from_rows: array element ...**. PySpark createDataFrame accepts lists of these types for array columns. This affects tests such as `test_explode_with_booleans`, `test_array_distinct_with_nulls_in_array`, `test_drop_duplicates_with_nulls_in_array`, `test_posexplode_empty_array`, `test_array_type_elementtype_with_date_type`.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

# Array of booleans
schema = StructType([
    StructField("id", StringType()),
    StructField("flags", ArrayType(BooleanType())),
])
data = [{"id": "x", "flags": [True, False]}, {"id": "y", "flags": [False]}]
df = spark.createDataFrame(data, schema)
df.collect()
```

**Observed:** `ValueError: create_dataframe_from_rows failed: json_value_to_series: unsupported boolean ...` (or unsupported long/int for other schemas).

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType

spark = SparkSession.builder.appName("repro").getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("flags", ArrayType(BooleanType())),
])
data = [{"id": "x", "flags": [True, False]}, {"id": "y", "flags": [False]}]
df = spark.createDataFrame(data, schema)
df.collect()  # OK
```

**Expected:** createDataFrame accepts array columns with boolean, long, int, and supported string element types.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.2** via PyO3 extension.
- PySpark 3.x.

## Request

Support in create_dataframe_from_rows (and json_value_to_series) for array element types: boolean, long, int, and string formats that PySpark accepts.
