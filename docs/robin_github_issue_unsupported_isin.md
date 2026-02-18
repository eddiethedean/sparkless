# [PySpark parity] Unsupported function / op: isin

## Summary

When executing logical plans via the robin-sparkless crate (e.g. through Sparkless v4), the **isin** operation is not supported. The plan adapter emits `isin` as an op with left/right (column and list of values). The crate reports `unsupported function: isin` or `unsupported expression op: isin`. PySpark supports `col.isin(*values)`.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()

df = spark.createDataFrame([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}])
result = df.filter(F.col("id").isin(1, 3)).collect()
# Expected: 2 rows (id 1 and 3)
```

**Observed:** `ValueError: Robin execute_plan failed: expression: unsupported function: isin` (or unsupported expression op: isin).

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()

df = spark.createDataFrame([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}])
result = df.filter(F.col("id").isin(1, 3)).collect()
assert len(result) == 2
assert {row["id"] for row in result} == {1, 3}
```

**Expected:** Two rows with `id` 1 and 3.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.11.4 via PyO3 extension.
- PySpark 3.x.

## References

- Sparkless doc: `docs/robin_unsupported_ops.md`.
