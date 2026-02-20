# [PySpark parity] isin: cannot check for List(Int64) values in String data

## Summary

When using **col.isin(*values)** where the column type and the literal list type differ (e.g. **String** column with **List(Int64)** values, or mixed types), the robin-sparkless crate fails with **'is_in' cannot check for List(Int64) values in String data** (or similar). PySpark often coerces or allows such comparisons. This affects tests like `test_isin_with_mixed_types`.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
# String column "id"
df = spark.createDataFrame([{"id": "1", "name": "a"}, {"id": "2", "name": "b"}, {"id": "3", "name": "c"}])
# isin with int literals (PySpark may coerce "1","3" to match 1,3 or vice versa)
result = df.filter(F.col("id").isin(1, 3))
result.collect()
```

**Observed:** `ValueError: collect failed: 'is_in' cannot check for List(Int64) values in String data` (or type mismatch).

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([{"id": "1", "name": "a"}, {"id": "2", "name": "b"}, {"id": "3", "name": "c"}])
result = df.filter(F.col("id").isin(1, 3))
result.collect()  # PySpark may coerce; 0 or 2 rows depending on semantics
```

**Expected:** Either support mixed-type isin with coercion, or document the restriction clearly.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.2** via PyO3 extension.
- PySpark 3.x.

## Request

Support isin when column and literal list types differ (with coercion) to match PySpark, or document supported type combinations.
