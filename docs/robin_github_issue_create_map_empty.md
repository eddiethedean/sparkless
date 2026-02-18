# [PySpark parity] create_map() with zero arguments fails (requires at least 1 argument)

## Summary

When executing a plan that uses `create_map()` with **no arguments** (empty map), the robin-sparkless crate fails with: **expression: fn 'create_map' requires at least 1 argument(s)**. In PySpark, `create_map()` with zero arguments is valid and returns an empty map `{}` for each row.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only execution):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([(1,)], ["id"])

# F.create_map() with no args: PySpark returns column of empty maps {}
result = df.withColumn("m", F.create_map()).collect()
# Expected: [Row(id=1, m={})]
# Observed: ValueError: Robin execute_plan failed: expression: fn 'create_map' requires at least 1 argument(s)
```

**Observed:** `ValueError: Robin execute_plan failed: expression: fn 'create_map' requires at least 1 argument(s)`.

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([(1,)], ["id"])

result = df.withColumn("m", F.create_map()).collect()
assert result[0]["m"] == {}, "create_map() with no args should return empty dict"
print(result)  # [Row(id=1, m={})]
```

**Expected:** No error; `result[0]["m"]` is `{}`.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.11.4 via PyO3 extension.
- PySpark 3.x.

**Fixed in robin-sparkless 0.11.5.**

## References

- Sparkless sends `{"fn": "create_map", "args": []}` for empty create_map; the crate rejects 0 arguments.
