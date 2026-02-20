# [PySpark parity] filter predicate must be of type Boolean, got String

## Summary

When **filter()** is used with a predicate that evaluates to or is interpreted as a **String** (e.g. string comparison, `contains`, or literal string in condition), the robin-sparkless crate fails with **filter predicate must be of type `Boolean`, got `String`**. PySpark allows string-involving predicates and returns a boolean column. This affects 39+ tests (e.g. `test_filter_with_string_equals`, `test_filter_and_literal_contains_and`).

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])
# Predicate involving string (e.g. contains or equals)
result = df.filter(F.col("name").contains("lic"))
result.collect()
```

**Observed:** `ValueError: collect failed: filter predicate must be of type Boolean, got String` (or similar).

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])
result = df.filter(F.col("name").contains("lic"))
result.collect()  # 1 row: alice
```

**Expected:** String-involving predicates produce a boolean and filter succeeds.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.2** via PyO3 extension.
- PySpark 3.x.

## Request

Ensure filter predicates that use string operations (contains, like, eq, etc.) are evaluated as Boolean in the engine.
