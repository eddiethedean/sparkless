## Summary

PySpark has `F.posexplode()` to explode an array column into position + value columns. Robin module has no `posexplode` attribute: `AttributeError: module 'robin_sparkless' has no attribute 'posexplode'`.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
# AttributeError: module 'robin_sparkless' has no attribute 'posexplode'
F.posexplode  # does not exist
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame(
    [{"Name": "Alice", "Values": [10, 20]}, {"Name": "Bob", "Values": [30, 40]}]
)
out = df.select("Name", F.posexplode("Values").alias("pos", "val")).collect()
# 4 rows (2 per array)
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/11_posexplode_array.py
```

Robin fails with `AttributeError: module 'robin_sparkless' has no attribute 'posexplode'`. PySpark succeeds.

## Context

- Robin version: 0.8.3 (sparkless pyproject.toml).
- Sparkless tests: `tests/test_issue_366_alias_posexplode.py`.
- PySpark also has `F.explode()`; Robin may or may not have explode.
