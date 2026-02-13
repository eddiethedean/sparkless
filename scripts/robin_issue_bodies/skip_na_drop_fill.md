## Summary

PySpark's `DataFrame.na` exposes `.drop(subset=..., how=..., thresh=...)` and `.fill(value, subset=...)` for null handling. Robin may not expose `na` or may have different signatures (e.g. no `subset`).

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
df = create_df(
    [{"x": 1, "y": None}, {"x": None, "y": 2}, {"x": 3, "y": 3}],
    [("x", "int"), ("y", "int")],
)
# df.na may be missing or .drop(subset=...) may raise TypeError
out = df.na.drop(subset=["x"])
out = df.na.fill(0, subset=["y"])
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, None), (None, 2), (3, 3)], ["x", "y"])
df.na.drop(subset=["x"]).collect()
df.na.fill(0, subset=["y"]).collect()
```

## Expected

`DataFrame.na` should expose `.drop(subset=..., how=..., thresh=...)` and `.fill(value, subset=...)` with PySpark-compatible semantics.

## Actual

DataFrame has no attribute `na`, or `na.drop`/`na.fill` lack `subset` (or other params), leading to TypeError.

## How to reproduce

```bash
python scripts/robin_parity_repros/21_na_drop_fill.py
```

## Context

Sparkless v4 skip list; test_issue_359_na_drop, test_na_fill_robust skipped. [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md).
