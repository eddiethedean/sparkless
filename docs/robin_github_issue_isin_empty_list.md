# [PySpark parity] isin with empty list semantics

## Summary

In PySpark, **`col.isin([])`** (column is-in empty list) evaluates to **false for every row**. When Sparkless sends this to the robin-sparkless crate, it may emit `isin` with two arguments (e.g. column and `lit(None)`) to satisfy a minimum-argument requirement. If the crate interprets that as "value is in {null}" or otherwise returns true/null for some rows, behavior diverges from PySpark. PySpark semantics: empty list → always false.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()

df = spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
# PySpark: no row satisfies "id in []" -> 0 rows
result = df.filter(F.col("id").isin([])).collect()
# Expected: 0 rows
```

**Observed:** If Robin returns any rows (or different semantics), this is a parity gap.

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()

df = spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
result = df.filter(F.col("id").isin([])).collect()
assert len(result) == 0, "col.isin([]) must be false for all rows; expected 0 rows"
```

**Expected:** Zero rows. `col.isin([])` is false for every row in PySpark.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.11.5 via PyO3 extension.
- PySpark 3.x.

## References

- Sparkless plan adapter sends at least 2 args for isin when right is empty (e.g. `[left_expr, {"lit": None}]`) to satisfy crate requirement; semantics should match PySpark (always false).
