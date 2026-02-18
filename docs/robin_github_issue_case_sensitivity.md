# [PySpark parity] Case sensitivity and column-not-found (not found: ID / Column 'value')

## Summary

When executing logical plans via the robin-sparkless crate (e.g. through Sparkless v4), **column-not-found** errors can occur when the plan or a later step references a column name that does not match the schema exactly (e.g. case difference: `ID` vs `id`). Errors observed: `not found: Column 'value'`, `not found: ID`, `Available columns: [id, name, value]`. Sparkless preserves the exact names from the logical plan and schema and does not change case when adapting. PySpark typically allows case-insensitive column resolution when `spark.sql.caseSensitive` is false.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()

# Schema has lowercase "id", "name", "value"
df = spark.createDataFrame(
    [(1, "a", 100), (2, "b", 200)],
    ["id", "name", "value"],
)
# Refer to column with different case (e.g. "ID" or "Value")
result = df.select(F.col("ID")).collect()  # or F.col("Value")
# PySpark (case-insensitive) often resolves this; Robin may fail
```

**Observed:** `ValueError: Robin execute_plan failed: session/df: not found: Column 'ID' not found. Available columns: [id, name, value]. Check spelling and case sensitivity (spark.sql.caseSensitive).` (or similar for `Value` / `value`).

After joins or unionByName, similar errors can appear for columns that exist in the schema but are referenced with different case or with a name that the crate does not recognize.

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
# Default: case insensitive
df = spark.createDataFrame(
    [(1, "a", 100), (2, "b", 200)],
    ["id", "name", "value"],
)
result = df.select(F.col("ID")).collect()  # resolves to "id"
assert len(result) == 2
assert result[0]["id"] == 1
```

**Expected:** Column resolution succeeds when names match case-insensitively (default).

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.11.4 via PyO3 extension.
- PySpark 3.x (case-insensitive by default).

## References

- Sparkless doc: `docs/robin_unsupported_ops.md` (Case sensitivity and column-not-found).
