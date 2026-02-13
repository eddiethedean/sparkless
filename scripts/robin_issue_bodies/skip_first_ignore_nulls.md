## Summary

PySpark's `F.first(col, ignorenulls=True)` (or `first("col", ignorenulls=True)`) returns the first value in a group, optionally ignoring nulls. Robin may not expose `first()` or the `ignorenulls` parameter, causing Sparkless test_first_ignorenulls and test_first_method to be skipped.

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
df = create_df(
    [{"k": "a", "v": 1}, {"k": "a", "v": None}, {"k": "a", "v": 3}],
    [("k", "string"), ("v", "int")],
)
gd = df.group_by("k")
# first(column, ignorenulls=True) may be missing or raise TypeError
out = gd.agg(rs.first(rs.col("v"), ignorenulls=True).alias("first_v"))
out.collect()
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 1), ("a", None), ("a", 3)], ["k", "v"])
df.groupBy("k").agg(F.first("v", ignorenulls=True).alias("first_v")).collect()
# first_v = 1 (first non-null)
```

## Expected

Aggregate function `first(column, ignorenulls=True)` (or equivalent) for use in groupBy.agg().

## Actual

AttributeError: module has no 'first', or TypeError on ignorenulls.

## How to reproduce

```bash
python scripts/robin_parity_repros/26_first_ignore_nulls.py
```

## Context

Sparkless v4 skip list; test_first_ignorenulls, test_first_method skipped. [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md).
