# [PySpark parity] Cast/conversion semantics (str to datetime/i32, f64 to i32, string to boolean)

## Summary

When **casting** or converting columns (e.g. string to timestamp, string to int, double to int, string to boolean), the robin-sparkless crate can fail with errors such as: **conversion from `str` to `datetime[μs]` failed**, **conversion from `str` to `i32` failed**, **casting from f64 to i32 not supported**, **casting from string to boolean failed for value ''**. PySpark allows many of these with null/error handling or truncation. This affects tests like `test_cast_string_to_timestamp_still_works`, `test_astype_invalid_string_to_int`, `test_astype_double_to_int`, `test_astype_string_to_boolean`, `test_exact_scenario_from_issue_432`.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
# String column with numeric-looking values
df = spark.createDataFrame([{"text": "123"}, {"text": "abc"}])
# Cast string to int (invalid "abc" -> PySpark often null or error mode)
result = df.withColumn("num", F.col("text").cast("int"))
result.collect()
```

**Observed:** `ValueError: collect failed: conversion from str to i32 failed ...` or similar for other cast directions.

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([{"text": "123"}, {"text": "abc"}])
result = df.withColumn("num", F.col("text").cast("int"))
result.collect()  # 123 -> 123, abc -> null (or configurable error)
```

**Expected:** Cast semantics align with PySpark: either produce null for invalid values or document the behavior (e.g. strict vs lenient).

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.2** via PyO3 extension.
- PySpark 3.x.

## Request

Align cast/conversion behavior with PySpark for str→datetime, str→i32, f64→i32, string→boolean (null on invalid or documented semantics).
