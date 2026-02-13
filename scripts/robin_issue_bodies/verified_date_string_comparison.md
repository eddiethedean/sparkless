## Summary

PySpark supports comparing date/datetime columns to string literals (implicit cast). Robin raises `RuntimeError: cannot compare 'date/datetime/time' to a string value`.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"dt": "2025-01-01"}, {"dt": "2025-01-02"}], [("dt", "date")])

# RuntimeError: cannot compare 'date/datetime/time' to a string value
df.filter(F.col("dt") == "2025-01-01").collect()
```

## Expected behavior (PySpark)

```python
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"dt": date(2025, 1, 1)}, {"dt": date(2025, 1, 2)}])
out = df.filter(F.col("dt") == "2025-01-01").collect()  # 1 row (PySpark implicit cast)
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/13_date_string_comparison.py
```

Robin fails with `RuntimeError: cannot compare 'date/datetime/time' to a string value`. PySpark succeeds.

## Context

- Robin version: 0.8.3 (sparkless pyproject.toml).
- Sparkless tests: `tests/test_issue_261_between.py`, `tests/test_issue_259_datetime_string_comparison.py`.
