# [Enhancement] Support with_column(name, expression) with Column expressions (PySpark parity)

<!-- This file is used as the body for the robin-sparkless GitHub issue. Create with: python scripts/create_robin_issue_with_column_expression.py -->

## Summary

PySpark's `DataFrame.withColumn(colName, col)` accepts a **Column expression** as the second argument (e.g. `F.col("a") * 2`, `F.lit(2) + F.col("x")`). robin-sparkless's `with_column(name, expr)` appears to treat the second argument as a **column name to resolve** rather than an expression to evaluate. When a Column expression object is passed, the engine raises `RuntimeError: not found: <...>`, where the message looks like a stringified form of the expression or an alias (e.g. `doubled`, `(2 + x)`, `(3 * x)`).

This blocks PySpark-style code and integrations (e.g. [Sparkless](https://github.com/eddiethedean/sparkless) Robin backend): any `df.withColumn("name", expr)` where `expr` is a non-trivial Column (arithmetic, literal + column, aliased expression) fails in Robin mode.

## PySpark reference behavior

```python
# PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# withColumn with expression: new column = existing * 2
df = spark.createDataFrame([{"a": 1}, {"a": 2}, {"a": 3}])
result = df.withColumn("doubled", (F.col("a") * 2).alias("doubled")).collect()
# Row(a=1, doubled=2), Row(a=2, doubled=4), Row(a=3, doubled=6)

# Literal on left (commutative)
df2 = spark.createDataFrame([{"x": 10}, {"x": 20}])
result2 = df2.withColumn("plus_two", F.lit(2) + F.col("x")).collect()
# Row(x=10, plus_two=12), Row(x=20, plus_two=22)
```

## Current robin-sparkless behavior (reproduced)

When Sparkless uses the Robin backend and executes the same patterns, materialization calls `df.with_column(col_name, robin_expr)` where `robin_expr` is a robin_sparkless Column built from the expression (e.g. `F.col("a") * F.lit(2)` or `F.lit(2) + F.col("x")`). The engine then raises:

```
RuntimeError: not found: doubled

Resolved plan until failure:
	---> FAILED HERE RESOLVING 'with_columns' <---
	DF ["a"]; PROJECT */1 COLUMNS; SELECTION: None
```

or

```
RuntimeError: not found: (2 + x)
RuntimeError: not found: (3 * x)
```

This suggests the second argument to `with_column` is being interpreted as a **column name** (or stringified and used as a name) instead of as an expression to evaluate against the current DataFrame.

## Minimal reproduction (robin-sparkless direct)

```python
import robin_sparkless as rs
F = rs

spark = F.SparkSession.builder().app_name("test").get_or_create()
data = [{"a": 1}, {"a": 2}, {"a": 3}]
schema = [("a", "int")]
df = spark.create_dataframe_from_rows(data, schema)

# Expected (PySpark): add column "doubled" = a * 2
# If with_column expects (name, expression):
expr = F.col("a") * F.lit(2)
df.with_column("doubled", expr).collect()   # -> RuntimeError: not found: ...
```

## Expected behavior (PySpark-compatible)

`with_column(col_name: str, col: Column)` should:

1. **Accept a Column expression** as the second argument (e.g. from `F.col("a") * F.lit(2)`, `F.lit(2) + F.col("x")`).
2. **Evaluate that expression** in the context of the current DataFrame and add the result as a new column (or replace existing) under `col_name`.
3. Continue to support simple column references if that is currently supported (e.g. `with_column("copy", F.col("a"))`).

## Impact on Sparkless

- **Tests:** Three tests in `tests/unit/backend/test_robin_materializer.py` are marked **xfail** when run with `SPARKLESS_TEST_BACKEND=robin` because of this limitation:
  - `test_with_column_alias_expression_robin` – `(F.col("a") * 2).alias("doubled")`
  - `test_with_column_literal_plus_column_robin` – `F.lit(2) + F.col("x")`
  - `test_with_column_literal_times_column_robin` – `F.lit(3) * F.col("x")`
- **Workaround:** Sparkless translates expressions to robin_sparkless Column objects and unwraps alias before calling `with_column`; the failure is inside robin-sparkless when it receives the expression Column.

## Suggested implementation direction

- In the Rust/Python bindings for `with_column(name, expr)`:
  - If `expr` is a Column **expression** (e.g. binary op, literal, alias), **evaluate** it in the plan to produce a new column and add it under `name`.
  - Do not treat the second argument as a column name to resolve (string lookup) when it is an expression object.

## Context

- **Source:** Sparkless project (Robin backend). Tests: `tests/unit/backend/test_robin_materializer.py::TestRobinMaterializerExpressionTranslation` (3 tests, xfail in Robin mode).
- **Sparkless materializer:** `sparkless/backend/robin/materializer.py` – builds robin Column via `_expression_to_robin()` and calls `df.with_column(col_name, robin_expr)`.
