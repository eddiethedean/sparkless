## Summary

PySpark supports `F.round()` on string columns that contain numeric values (implicit cast). Robin raises `RuntimeError: round can only be used on numeric types`.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"val": "10.4"}, {"val": "9.6"}], [("val", "string")])

# RuntimeError: round can only be used on numeric types
df.with_column("rounded", F.round(F.col("val"))).collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"val": "10.4"}, {"val": "9.6"}])
out = df.withColumn("rounded", F.round("val")).collect()  # [10.0, 10.0]
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/09_round_string_column.py
```

Robin fails with `RuntimeError: round can only be used on numeric types`. PySpark succeeds.

## Context

- Robin version: 0.8.3 (sparkless pyproject.toml).
- Sparkless tests: `tests/test_issue_373_round_string.py`.
