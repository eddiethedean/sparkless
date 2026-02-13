## Summary

PySpark supports `F.array()` with no arguments, returning an empty array column. Robin raises `RuntimeError: array requires at least one column`.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"Name": "Alice"}, {"Name": "Bob"}], [("Name", "string")])

# RuntimeError: array requires at least one column
df.with_column("NewArray", F.array()).collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
out = df.withColumn("NewArray", F.array()).collect()  # [Row(Name='Alice', NewArray=[]), ...]
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/10_array_empty.py
```

Robin fails with `RuntimeError: array requires at least one column`. PySpark succeeds.

## Context

- Robin version: 0.8.3 (sparkless pyproject.toml).
- Sparkless tests: `tests/test_issue_367_array_empty.py`.
