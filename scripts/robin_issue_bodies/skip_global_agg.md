## Summary

PySpark's `DataFrame.agg(*exprs)` performs global aggregation (no groupBy) and returns a single-row DataFrame. Robin's DataFrame may not have an `agg()` method, causing "'DataFrame' object has no attribute 'agg'".

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
df = create_df([{"x": 1, "y": 10}, {"x": 2, "y": 20}], [("x", "int"), ("y", "int")])
# AttributeError: 'DataFrame' object has no attribute 'agg'
out = df.agg(rs.sum(rs.col("x")), rs.avg(rs.col("y")))
out.collect()
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, 10), (2, 20)], ["x", "y"])
df.agg(F.sum("x"), F.avg("y")).collect()  # [Row(sum(x)=3, avg(y)=15.0)]
```

## Expected

`DataFrame.agg(*exprs)` should accept one or more aggregate expressions and return a single-row DataFrame.

## Actual

DataFrame has no attribute `agg`, or agg fails.

## How to reproduce

```bash
python scripts/robin_parity_repros/25_global_agg.py
```

## Context

Sparkless v4 skip list; parity test_global_aggregation skipped. [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md).
