## Summary

When calling Robin aggregate functions (e.g. `F.sum(column)` in a `df.agg(...)` or `df.groupBy(...).agg(...)`), Sparkless parity tests sometimes fail with "No active SparkSession found. This operation requires an active SparkSession." PySpark keeps an "active" session (e.g. from `getOrCreate()`) so that F.sum and similar can resolve it; Robin may not register the session the same way.

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
df = create_df([{"x": 1}, {"x": 2}], [("x", "int")])
# May raise: RuntimeError: No active SparkSession found
out = df.agg(rs.sum(rs.col("x")))
out.collect()
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,), (2,)], ["x"])
df.agg(F.sum("x")).collect()  # Succeeds; session is active
```

## Expected

After creating a session and a DataFrame with it, aggregate functions (sum, avg, count, etc.) should work without requiring a separate "active session" registration step. Alternatively, Robin could expose `SparkSession.getActiveSession()` (or equivalent) so that the Python layer can set/retrieve the active session for F.* calls.

## Actual

RuntimeError: "Cannot perform sum aggregate function: No active SparkSession found. This operation requires an active SparkSession, similar to PySpark. Create a SparkSession first: spark = SparkSession('app_name')".

## How to reproduce

```bash
python scripts/robin_parity_repros/27_get_active_session_aggregate.py
```

## Context

Sparkless v4 skip list; parity aggregation tests skipped. [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md).
