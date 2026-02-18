# [PySpark parity] Window expressions in select (expression must have col/lit/op/fn)

## Summary

When executing logical plans via the robin-sparkless crate (e.g. through Sparkless v4), **select** steps that include window function expressions (e.g. `row_number().over(window) + 10`) fail with **expression must have 'col', 'lit', 'op', or 'fn'**. The plan sends a `type: "window"` node in the expression tree; the crate does not accept that shape. PySpark supports window functions in select, including arithmetic on their results.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless import functions as F
from sparkless.window import Window

spark = SparkSession.builder.appName("repro").getOrCreate()

df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
w = Window.orderBy("val")
result = df.select(
    F.col("val"),
    (F.row_number().over(w) + 10).alias("row_plus_10"),
).collect()
# Expected: (1,11), (2,12), (3,13)
```

**Observed:** `ValueError: Robin execute_plan failed: expression: expression must have 'col', 'lit', 'op', or 'fn'`.

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("repro").getOrCreate()

df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
w = Window.orderBy("val")
result = df.select(
    F.col("val"),
    (F.row_number().over(w) + 10).alias("row_plus_10"),
).collect()
assert [row["row_plus_10"] for row in result] == [11, 12, 13]
```

**Expected:** Three rows with `row_plus_10` 11, 12, 13.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.11.4 via PyO3 extension.
- PySpark 3.x.

## References

- Sparkless doc: `docs/robin_unsupported_ops.md` (Window expressions in select).
