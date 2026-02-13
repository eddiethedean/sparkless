# [Parity] approx_count_distinct(column, rsd=...) missing

## Summary

PySpark provides `F.approx_count_distinct(column, rsd=...)` for approximate distinct count with configurable relative standard deviation. Robin-sparkless does not expose `approx_count_distinct` in the Python API, so code using it fails when run with Robin.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
data = [{"k": "A", "value": 1}, {"k": "A", "value": 10}, {"k": "A", "value": 1}, {"k": "B", "value": 5}, {"k": "B", "value": 5}]
df = create_df(data, [("k", "string"), ("value", "int")])
# Robin has no approx_count_distinct
out = df.agg(rs.approx_count_distinct(rs.col("value"), rsd=0.01))  # AttributeError / missing
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("A",1),("A",10),("A",1),("B",5),("B",5)], ["k","value"])
out = df.agg(F.approx_count_distinct("value", rsd=0.01))
out.collect()  # [(3,)]
```

## Expected

- Module (or session) exposes `approx_count_distinct(column, rsd=None)`.
- `rsd` (relative standard deviation) controls approximation accuracy; PySpark default is 0.05 when not specified.
- Result is approximate count of distinct values in the column.

## Actual

- `robin_sparkless` has no `approx_count_distinct` symbol; `getattr(rs, "approx_count_distinct", None)` is `None`.
- Aggregations using approx_count_distinct cannot be expressed.

## How to reproduce

Run from Sparkless repo root:
`python scripts/robin_parity_repros/28_approx_count_distinct_rsd.py`

## Context

Sparkless v4 skip list: `tests/unit/functions/test_approx_count_distinct_rsd.py` skipped when backend is Robin. [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md) — "Check; file if missing."
