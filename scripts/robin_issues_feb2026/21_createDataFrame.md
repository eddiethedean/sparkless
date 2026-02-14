## Summary

PySpark exposes `spark.createDataFrame(data, schema=None)` as the standard way to build a DataFrame from local data. Robin-sparkless currently exposes `create_dataframe_from_rows` or `_create_dataframe_from_rows` with a different signature (e.g. schema as list of `(name, dtype_str)`). Compatibility layers (e.g. Sparkless) have to wrap this to offer PySpark-style `createDataFrame`.

**Request:** Add a public `createDataFrame(data, schema=None)` (or equivalent camelCase) on SparkSession that accepts PySpark-style arguments for parity:

- `data`: list of dicts, list of tuples/rows, or RDD-like
- `schema`: optional; `None` (infer), list of column names, or StructType / list of StructField / single DataType

This would allow downstream projects to call `spark.createDataFrame(...)` directly without a compat shim.

---

## Current Robin-sparkless (workaround)

Sparkless uses the internal/undocumented API:

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows", None)
# Schema must be list of (name, dtype_str), not PySpark StructType
df = create_df(
    [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}],
    [("name", "string"), ("age", "int")],
)
df.collect()
```

There is no `spark.createDataFrame(...)`; callers must know the internal method and schema format.

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[1]").getOrCreate()

# List of dicts, schema inferred
df1 = spark.createDataFrame([{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}])

# List of tuples with column names
df2 = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])

# With StructType
schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
df3 = spark.createDataFrame([("Alice", 25), ("Bob", 30)], schema)

spark.stop()
```

PySpark’s `createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)` is the standard entry point. Adding an equivalent on Robin’s SparkSession would improve parity and simplify wrappers.
