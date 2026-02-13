## Summary

PySpark supports `eqNullSafe` with type coercion (e.g. string column vs int literal). Robin raises `RuntimeError: cannot compare string with numeric type (i32)`.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df(
    [{"str_col": "123", "other": 1}, {"str_col": "456", "other": 2}],
    [("str_col", "string"), ("other", "int")],
)

# RuntimeError: cannot compare string with numeric type (i32)
df.select(F.col("str_col").eq_null_safe(F.lit(123)).alias("eq")).collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"str_col": "123"}, {"str_col": "456"}])
out = df.select(F.col("str_col").eqNullSafe(F.lit(123)).alias("eq")).collect()
# [Row(eq=True), Row(eq=False)] (PySpark coerces string to int for comparison)
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/14_eqnullsafe_type_coercion.py
```

Robin fails with `RuntimeError: cannot compare string with numeric type (i32)`. PySpark succeeds.

## Context

- Robin version: 0.8.3 (sparkless pyproject.toml).
- Sparkless tests: `tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_type_coercion`.
- Robin has eq_null_safe for same-type comparisons (fixed in 0.8); the gap is type coercion.
