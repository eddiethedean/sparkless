# [PySpark parity] select expects Column or str

## Summary

In some code paths, **select()** with column expressions (e.g. aggregations, window expressions, or join-result columns) is rejected with **select expects Column or str** (or "payload must be array of column names or ..."). PySpark accepts `df.select(expr1, expr2, ...)` with arbitrary column expressions. This affects 105+ tests (e.g. `test_inner_join_then_groupby`, `test_sum_over_window`, `test_join_then_aggregate_with_join_keys`).

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "b", "v": 30}])
# Select after groupBy: expressions like sum("v"), count("v")
agg_df = df.groupBy("k").agg(F.sum("v").alias("total"), F.count("v").alias("cnt"))
result = agg_df.select("k", "total", "cnt")
result.collect()
```

**Observed:** `ValueError: ... select expects Column or str` when select receives expression-backed columns (e.g. from agg or join).

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "b", "v": 30}])
agg_df = df.groupBy("k").agg(F.sum("v").alias("total"), F.count("v").alias("cnt"))
result = agg_df.select("k", "total", "cnt")
result.collect()  # OK
```

**Expected:** select() accepts column names and expression-derived columns.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.15.0** via PyO3 extension.
- PySpark 3.x.

## Request

Accept column expressions (and names) in select payload so plans with agg/join + select succeed.
