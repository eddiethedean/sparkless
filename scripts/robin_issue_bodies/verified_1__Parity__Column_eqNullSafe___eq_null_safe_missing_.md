## Summary

PySpark's `Column` has `eqNullSafe(other)` (null-safe equality: NULL <=> NULL is true). robin-sparkless Column does not expose `eq_null_safe` or `eqNullSafe`, so filter expressions using null-safe equality cannot be expressed when using Robin directly.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"a": 1}, {"a": None}, {"a": 3}], [("a", "int")])

# AttributeError or similar: eq_null_safe / eqNullSafe not found
df.filter(F.col("a").eq_null_safe(F.lit(None))).collect()
```

Robin Column has no `eq_null_safe` or `eqNullSafe` method.

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"a": 1}, {"a": None}, {"a": 3}])
out = df.filter(F.col("a").eqNullSafe(F.lit(None))).collect()  # 1 row (the None row)
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo (with robin-sparkless and optionally pyspark installed):

```bash
python scripts/robin_parity_repros/04_filter_eqNullSafe.py
```

Robin reports: `eq_null_safe / eqNullSafe not found`. PySpark path succeeds.

## Context

- Sparkless uses robin-sparkless as execution backend. Many Sparkless tests fail with "Operation 'Operations: filter' is not supported" when the filter uses eqNullSafe; the Sparkless Robin materializer cannot translate it because Robin Column lacks this method.
- Representative Sparkless test: `tests/test_issue_260_eq_null_safe.py`
