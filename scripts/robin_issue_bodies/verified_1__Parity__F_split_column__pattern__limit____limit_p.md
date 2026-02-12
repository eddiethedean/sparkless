## Summary

PySpark supports `F.split(column, pattern, limit)` with an optional third argument to limit the number of splits. Robin's `split` accepts only 2 arguments (column, pattern); passing a limit raises `TypeError: py_split() takes 2 positional arguments but 3 were given`.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"s": "a,b,c"}], [("s", "string")])

# TypeError: py_split() takes 2 positional arguments but 3 were given
df.select(F.split(F.col("s"), ",", 2)).collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"s": "a,b,c"}])
out = df.select(F.split(F.col("s"), ",", 2)).collect()  # [Row(split(s, ,, 2)=['a', 'b,c'])]
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/07_split_limit.py
```

Robin fails with `TypeError: py_split() takes 2 positional arguments but 3 were given`. PySpark succeeds.

## Context

- Sparkless parity tests for split with limit fail when backend is Robin (e.g. `tests/parity/functions/test_split_limit_parity.py`, `tests/test_issue_328_split_limit.py`). Robin reports "not found: split(Value, ,, -1)" or similar when Sparkless translates the expression.
- Robin version: 0.8.0+ (sparkless pyproject.toml).
