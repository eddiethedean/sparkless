# [Enhancement] Support select() with column expressions and add regexp_extract_all for PySpark compatibility

<!-- This file is used as the body for the robin-sparkless GitHub issue. Create with: python scripts/create_robin_issue_regexp_extract_all.py -->

## Summary

PySpark supports `DataFrame.select()` with column **expressions** (e.g. `F.regexp_extract_all(col, pattern, group_idx).alias("m")`), not just column names. robin-sparkless currently:

1. **`select()`** accepts only a list of column **names** (strings), not Column/expression objects.
2. **No `regexp_extract_all`** (or equivalent) in the functions namespace, so "extract all regex matches as an array" cannot be expressed.

This blocks PySpark-style code and integrations (e.g. [Sparkless](https://github.com/eddiethedean/sparkless) Robin backend): tests that use `df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m"))` either fail with an unsupported-operation error or, in parallel test runs, can stall when the Robin path is first used.

## PySpark reference behavior

```python
# PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([
    {"s": "a1 b22 c333"},
    {"s": "no-digits"},
    {"s": None},
])

# Select with expression: regexp_extract_all returns array of all matches
result = df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m")).collect()
# Row(m=['1', '22', '333'])
# Row(m=[])
# Row(m=None)
```

## Current robin-sparkless behavior (reproduced)

### 1. Direct API

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()

data = [{"s": "a1 b22 c333"}, {"s": "no-digits"}, {"s": None}]
schema = [("s", "string")]
df = spark.create_dataframe_from_rows(data, schema)

# This works: select by column name only
df.select(["s"]).collect()  # OK

# PySpark-style: select with expression
# - F.regexp_extract_all does not exist
# - select() only accepts list of strings (column names), not Column expressions
# So this pattern cannot be expressed at all.
```

**Observed:** `robin_sparkless` has no `F.regexp_extract_all` (or similar). `select()` only accepts a list of column names (strings), not Column/expression arguments.

### 2. Sparkless in Robin mode (impact)

When [Sparkless](https://github.com/eddiethedean/sparkless) runs with backend=robin and hits the same pattern:

```
SparkUnsupportedOperationError: Operation 'Operations: select' is not supported
Reason: Backend 'robin' does not support these operations
```

The Sparkless Robin materializer only passes "select with list of string column names" to robin-sparkless; select-with-expressions is not supported, so the test is skipped in Robin mode. In parallel test runs (e.g. pytest-xdist), the same scenario can also lead to a **stall** (e.g. around first-time Robin session or materializer use on a worker), which is why this is reported as a real-world issue.

## Expected behavior (PySpark-compatible)

1. **`select()`** should accept Column/expression arguments, not only column names:
   - `df.select([col1, col2])` where `col1`/`col2` can be Column expressions (e.g. from `F.regexp_extract_all(...).alias("m")`).
   - Continuing to support `df.select(["a", "b"])` for name-only projection.

2. **`regexp_extract_all(column, pattern, group_index)`** (or equivalent) in the functions namespace:
   - **Semantics:** For each string in the column, find all non-overlapping matches of `pattern` and return the specified group (0 = full match) as an array. Null in → null out.
   - **Signature (PySpark-style):** `regexp_extract_all(col, pattern, idx=0)` → Column of array of string.

## Reproduction script

The Sparkless repo includes a script that reproduces the gap using robin-sparkless directly and (optionally) Sparkless in Robin mode:

```bash
# From sparkless repo root
pip install robin-sparkless   # if not already
python scripts/reproduce_robin_regexp_extract_all_issue.py

# With Sparkless in Robin mode (to see SparkUnsupportedOperationError)
SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python scripts/reproduce_robin_regexp_extract_all_issue.py
```

## Suggested implementation direction

- **select():** Extend the Python bindings so `select()` accepts:
  - A list of strings (column names) – current behavior.
  - A list of Column/expression objects (e.g. from `F.regexp_extract_all(...).alias("m")`), and evaluate them to produce the result DataFrame.
- **regexp_extract_all:** Add a function in the exposed functions namespace that:
  - Takes (column, pattern, group_index) and returns a Column representing an array of strings (all matches for that group). Pattern can be a string or Column; group_index an int (0 = full match).

## Context

- **Source:** Sparkless project (Robin backend integration). Test: `tests/unit/functions/test_issue_189_string_functions_robust.py::TestIssue189StringFunctionsRobust::test_regexp_extract_all_multiple_matches_and_nulls`.
- **Sparkless behavior:** This test is skipped when `SPARKLESS_TEST_BACKEND=robin` because the operation is unsupported; in parallel runs, hitting this path can also cause a stall.
- **Workaround (Sparkless):** Skip the test in Robin mode until robin-sparkless supports select-with-expressions and regexp_extract_all.
