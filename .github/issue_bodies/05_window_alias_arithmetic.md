## Summary

Window expressions combined with an alias or with arithmetic (e.g. `(row_number().over(...)).alias("row_plus_10")` or `row_number().over(...) + 10`) fail with `RuntimeError: not found: Column 'row_plus_10'` (or `'percentile'`, etc.). Robin resolves by name; the alias or the arithmetic result must be a single Robin Column expression, not a name lookup.

## Failure (current behavior)

```python
from sparkless.sql import SparkSession
from sparkless.functions import F
from sparkless.window import Window

spark = SparkSession.builder().master("local").get_or_create()
df = spark.createDataFrame([("a", 10), ("a", 20), ("b", 5)], ["dept", "salary"])

w = Window.partitionBy("dept").orderBy(F.col("salary").desc())

# FAILS: not found: Column 'row_plus_10' not found (or similar alias)
result = df.withColumn("row_plus_10", F.row_number().over(w) + 10)
result.collect()
```

```python
# FAILS: not found: Column 'percentile' not found
result = df.withColumn("percentile", F.ntile(4).over(w) * 25)
result.collect()
```

```python
# Alias only: FAILS if alias is sent as name
result = df.withColumn("rn", F.row_number().over(w))
result.collect()
```

## Expected behavior

- `F.row_number().over(w) + 10` should be translated to a single Robin expression (window column plus literal 10), and the result column should be named by the withColumn name or alias.
- `(row_number().over(w)).alias("rn")` should produce one column named `rn` with the window function result.
- Robin should receive one Column per select/withColumn item (expression + optional alias), not a column name string.

## Fix (Sparkless-side)

In `_expression_to_robin`:
- When the expression is a **ColumnOperation** with op `+`, `-`, `*`, `/` and one side is a WindowFunction (or an expression that translates to a window), recurse on both sides, translate each to Robin, and combine with the operator (e.g. `left_robin + right_robin`). Then apply alias if present.
- Ensure alias is always the outermost so the final Robin Column has the correct output name (e.g. `(base.row_number().over(...) + 10).alias("row_plus_10")`).

Ref: sparkless/backend/robin/materializer.py `_expression_to_robin` (WindowFunction branch and arithmetic branches).
