# [PySpark parity] Unsupported expression ops: create_map, getItem, regexp_extract, startswith, is_null / is_not_null

## Summary

When executing logical plans via the robin-sparkless crate (e.g. through Sparkless v4), the following expression operations are not supported and produce errors. PySpark supports all of them.

| Op | Crate error / behavior |
|----|-------------------------|
| `create_map` | `unsupported expression op: create_map` |
| `getItem` | `unsupported expression op: getItem` |
| `regexp_extract` | `unsupported expression op: regexp_extract` |
| `startswith` | `unsupported expression op: startswith` |
| `is_null` / `is_not_null` | `unsupported expression op: is_null` or `unsupported function: is_null` |

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only; execution via crate through PyO3):

```python
from sparkless import SparkSession
from sparkless import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()

# --- create_map: fails with "unsupported expression op: create_map"
df = spark.createDataFrame([{"a": 1, "b": 2}])
df.select(F.create_map(F.lit("k"), F.col("a")).alias("m")).collect()

# --- getItem: fails with "unsupported expression op: getItem"
df = spark.createDataFrame([{"arr": [10, 20, 30]}])
df.select(F.col("arr").getItem(1).alias("x")).collect()

# --- regexp_extract: fails with "unsupported expression op: regexp_extract"
df = spark.createDataFrame([{"s": "hello world"}])
df.select(F.regexp_extract(F.col("s"), r"(\w+)", 1).alias("first")).collect()

# --- startswith: fails with "unsupported expression op: startswith"
df = spark.createDataFrame([{"name": "Alice"}])
df.filter(F.col("name").startswith("A")).collect()

# --- is_null: fails with "unsupported expression op: is_null"
df = spark.createDataFrame([{"x": 1}, {"x": None}])
df.filter(F.col("x").isNull()).collect()
```

**Observed:** `ValueError: Robin execute_plan failed: expression: unsupported expression op: <op>` (or similar) when the plan is executed by the crate.

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()

# create_map
df = spark.createDataFrame([{"a": 1, "b": 2}])
assert df.select(F.create_map(F.lit("k"), F.col("a")).alias("m")).collect()[0]["m"] == {"k": 1}

# getItem
df = spark.createDataFrame([{"arr": [10, 20, 30]}])
assert df.select(F.col("arr").getItem(1).alias("x")).collect()[0]["x"] == 20

# regexp_extract
df = spark.createDataFrame([{"s": "hello world"}])
assert df.select(F.regexp_extract(F.col("s"), r"(\w+)", 1).alias("first")).collect()[0]["first"] == "hello"

# startswith
df = spark.createDataFrame([{"name": "Alice"}])
assert len(df.filter(F.col("name").startswith("A")).collect()) == 1

# is_null
df = spark.createDataFrame([{"x": 1}, {"x": None}])
assert len(df.filter(F.col("x").isNull()).collect()) == 1
```

**Expected:** All of the above run and return the stated results.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.11.4 via PyO3 extension.
- PySpark 3.x.

## References

- Sparkless doc: `docs/robin_unsupported_ops.md`.
