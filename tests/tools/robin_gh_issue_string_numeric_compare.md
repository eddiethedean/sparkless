# [PySpark parity] collect failed: cannot compare string with numeric type

## Summary

When a **string column** is compared with numeric values (e.g. in `between(lower, upper)`, filter, or select expression), the robin-sparkless crate fails with **collect failed: cannot compare string with numeric type (i32)** (or similar). In PySpark, string-vs-numeric comparison is allowed in many contexts: the string is coerced (e.g. "5" compared to 1 and 10 in `between(1, 10)` yields true when the string parses to a number within range), or the comparison follows PySpark’s coercion rules.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()

# String column "val" with numeric-looking values
df = spark.createDataFrame(
    [
        {"id": 1, "val": "5"},
        {"id": 2, "val": "15"},
    ]
)
# between(1, 10) with string column: PySpark coerces and 5 is in [1,10], 15 is not
result = df.select(
    F.col("id"),
    F.col("val"),
    F.col("val").between(1, 10).alias("in_range"),
)
rows = result.collect()
```

**Observed:** `ValueError: collect failed: cannot compare string with numeric type (i32)` (or similar).

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()

df = spark.createDataFrame(
    [
        {"id": 1, "val": "5"},
        {"id": 2, "val": "15"},
    ]
)
result = df.select(
    F.col("id"),
    F.col("val"),
    F.col("val").between(1, 10).alias("in_range"),
)
rows = result.collect()
assert rows[0]["in_range"] is True   # "5" -> 5 in [1, 10]
assert rows[1]["in_range"] is False   # "15" -> 15 not in [1, 10]
print(rows)
```

**Expected:** No error; string column is coerced for comparison where applicable; `in_range` is True for "5" and False for "15".

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.0** via PyO3 extension.
- PySpark 3.x.

## Request

Allow string-vs-numeric comparison in filter/select/between (with coercion or documented semantics) to match PySpark behavior, or document the current restriction.
