# Robin-sparkless Issues Identified from Sparkless Test Failures

This document reports Robin behavioral/API issues identified when running Sparkless tests with `SPARKLESS_TEST_BACKEND=robin`. Each issue includes minimal Robin reproduction code and equivalent PySpark code for comparison.

**Robin version tested:** 0.8.4  
**Date:** 2025-02-12

**GitHub issues filed:**
- Issue 1: [#272](https://github.com/eddiethedean/robin-sparkless/issues/272) round() on string
- Issue 2: [#273](https://github.com/eddiethedean/robin-sparkless/issues/273) to_timestamp() on string
- Issue 3: [#274](https://github.com/eddiethedean/robin-sparkless/issues/274) Join key type coercion
- Issue 4: [#275](https://github.com/eddiethedean/robin-sparkless/issues/275) create_map() empty
- Issue 5: [#276](https://github.com/eddiethedean/robin-sparkless/issues/276) between() string vs numeric

**Standalone repro scripts** (run from sparkless repo root):
- Issue 1: `python scripts/repro_robin_limitations/13_round_string_returns_none.py`
- Issue 2: `python scripts/repro_robin_limitations/14_to_timestamp_string.py`
- Issue 3: `python scripts/repro_robin_limitations/15_join_key_type_coercion.py`
- Issue 4: `python scripts/repro_robin_limitations/16_create_map_empty.py`
- Issue 5: `python scripts/repro_robin_limitations/17_between_string_numeric.py`
- Issue 6: `python scripts/repro_robin_limitations/18_to_timestamp_timestamp_col.py` (OK in 0.8.4)

---

## Issue 1: `round()` on string column returns None (PySpark strips and casts)

**PySpark behavior:** `F.round(" 10.6 ")` strips whitespace, casts to numeric, and returns `11.0`.

**Robin behavior:** Returns `None` for string columns.

### Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
data = [{"val": "  10.6  "}, {"val": "\t20.7"}]
df = create_df(data, [("val", "str")])
df = df.with_column("rounded", rs.round(rs.col("val")))
rows = df.collect()
# Actual: [{'val': '  10.6  ', 'rounded': None}, {'val': '\t20.7', 'rounded': None}]
# Expected: rounded = 11.0, 21.0
```

### PySpark equivalent

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([("  10.6  ",), ("\t20.7\n",)], ["val"])
df = df.withColumn("rounded", F.round("val"))
rows = df.collect()
# PySpark: rounded = 11.0, 21.0
```

---

## Issue 2: `to_timestamp()` on string column fails or returns None

**PySpark behavior:** `F.to_timestamp(col("ts_str"))` parses common formats like `"2024-01-01 10:00:00"` without explicit format. `F.to_timestamp(col("ts_str"), "yyyy-MM-dd'T'HH:mm:ss")` parses with format.

**Robin behavior:** 
- Without format: `RuntimeError: conversion from str to datetime[μs] failed`
- With format: Returns `None`

### Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
data = [{"ts_str": "2024-01-01 10:00:00"}]
df = create_df(data, [("ts_str", "str")])
# Fails: RuntimeError: conversion from `str` to `datetime[μs]` failed
df = df.with_column("ts", rs.to_timestamp(rs.col("ts_str")))

# With format - returns None:
data2 = [{"ts_str": "2024-01-01T10:00:00"}]
df2 = create_df(data2, [("ts_str", "str")])
df2 = df2.with_column("ts", rs.to_timestamp(rs.col("ts_str"), "yyyy-MM-dd'T'HH:mm:ss"))
rows = df2.collect()  # ts is None
```

### PySpark equivalent

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([("2024-01-01 10:00:00",)], ["ts_str"])
df = df.withColumn("ts", F.to_timestamp(F.col("ts_str")))
# PySpark: ts = datetime(2024, 1, 1, 10, 0, 0)
```

---

## Issue 3: Join requires exact key type match (PySpark coerces)

**PySpark behavior:** Join on columns with different types (e.g. str vs int) coerces to common type.

**Robin behavior:** `RuntimeError: datatypes of join keys don't match - id: str on left does not match id: i64 on right`

### Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
df1 = create_df([{"id": "1", "label": "a"}], [("id", "str"), ("label", "str")])
df2 = create_df([{"id": 1, "x": 10}], [("id", "int"), ("x", "int")])
# Fails: RuntimeError: datatypes of join keys don't match
joined = df1.join(df2, on=["id"], how="inner")
```

### PySpark equivalent

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df1 = spark.createDataFrame([("1", "a")], ["id", "label"])
df2 = spark.createDataFrame([(1, 10)], ["id", "x"])
# PySpark: succeeds, coerces str "1" and int 1 for join
joined = df1.join(df2, on="id", how="inner")
```

---

## Issue 4: `create_map()` requires at least one argument (PySpark supports empty)

**PySpark behavior:** `F.create_map()` and `F.create_map([])` return empty map column.

**Robin behavior:** `TypeError: py_create_map() missing 1 required positional argument: 'cols'`

### Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
data = [{"id": 1}]
df = create_df(data, [("id", "int")])
# Fails: TypeError: py_create_map() missing 1 required positional argument: 'cols'
df = df.with_column("m", rs.create_map())
```

### PySpark equivalent

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([(1,)], ["id"])
df = df.withColumn("m", F.create_map())
# PySpark: m = {} for each row
```

---

## Issue 5: `between()` with string column and numeric bounds fails (PySpark coerces)

**PySpark behavior:** `col("col").between(1, 20)` when `col` is string coerces for comparison.

**Robin behavior:** `RuntimeError: cannot compare string with numeric type (i32)`

### Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
data = [{"col": "5"}, {"col": "10"}, {"col": "15"}]
df = create_df(data, [("col", "str")])
# Fails: RuntimeError: cannot compare string with numeric type (i32)
df = df.with_column("between", rs.col("col").between(1, 20))
```

### PySpark equivalent

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([("5",), ("10",), ("15",)], ["col"])
df = df.withColumn("between", F.col("col").between(1, 20))
# PySpark: coerces and evaluates
```

---

## Issue 6: `to_timestamp()` on TimestampType column (pass-through)

**PySpark behavior:** `F.to_timestamp(col("ts"))` when `ts` is already TimestampType passes through unchanged.

**Robin behavior (older):** `RuntimeError: to_timestamp: invalid series dtype: expected String, got datetime[μs]`

**Note:** Robin 0.8.4 appears to handle this correctly now (pass-through works).

### Robin reproduction

```python
import robin_sparkless as rs
from datetime import datetime

spark = rs.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
# Create df with datetime column, then to_timestamp on it
data = [{"ts_str": "2024-01-01T10:00:00"}]
df = create_df(data, [("ts_str", "str")])
# First convert to datetime (if Robin had working to_timestamp)
# Then: to_timestamp on already-datetime column should pass through
# Robin fails when column is datetime: expected String, got datetime[μs]
```

### PySpark equivalent

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([("2024-01-01T10:00:00",)], ["ts_str"])
df = df.withColumn("ts", F.to_timestamp(F.col("ts_str"), "yyyy-MM-dd'T'HH:mm:ss"))
# Pass-through: to_timestamp on ts (already TimestampType) returns ts unchanged
df = df.withColumn("ts2", F.to_timestamp(F.col("ts"), "yyyy-MM-dd'T'HH:mm:ss"))
# PySpark: ts == ts2
```

---

## Summary

| # | Issue | Robin 0.8.4 | PySpark |
|---|-------|-------------|---------|
| 1 | round(string with whitespace) | Returns None | Strips, casts, returns value |
| 2 | to_timestamp(string) | Fails or returns None | Parses common formats |
| 3 | Join str vs i64 keys | Raises | Coerces |
| 4 | create_map() empty | Raises | Returns empty map |
| 5 | between(string, num, num) | Raises | Coerces |
| 6 | to_timestamp(datetime) pass-through | OK in 0.8.4 (may have been fixed) | Passes through |

---

## Note on Sparkless translation gaps

Some test failures are **Sparkless translation gaps**, not Robin bugs (e.g. Sparkless may not translate `F.expr("LTRIM(s)")` to Robin's `trim`, or `F.array()` to Robin's `array`). Robin's direct API works for array(), trim(), regexp_replace() when called correctly.
