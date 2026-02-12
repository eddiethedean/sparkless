## Summary

Many filters fail with `SparkUnsupportedOperationError: Operation 'Operations: filter' is not supported` because `_simple_filter_to_robin` does not support `like`, `isnull`, or `isnotnull`. Extending it would fix these cases. (Note: `isin([])` should remain unsupported and documented as Robin #244.)

## Failure (current behavior)

```python
from sparkless.sql import SparkSession
from sparkless.functions import F

spark = SparkSession.builder().master("local").get_or_create()
df = spark.createDataFrame([("alice",), ("bob",), ("alex",)], ["name"])

# FAILS: SparkUnsupportedOperationError: Operation 'Operations: filter' is not supported
result = df.filter(F.col("name").like("al%"))
result.collect()
```

```python
df = spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "value"])
# FAILS: SparkUnsupportedOperationError
result = df.filter(F.col("value").isNull())
result.collect()
```

```python
# FAILS: SparkUnsupportedOperationError
result = df.filter(F.col("value").isNotNull())
result.collect()
```

## Expected behavior

- `df.filter(F.col("name").like("al%"))` should return rows where `name` matches the pattern (e.g. "alice", "alex").
- `df.filter(F.col("value").isNull())` should return rows where `value` is null.
- `df.filter(F.col("value").isNotNull())` should return rows where `value` is not null.

## Fix (Sparkless-side)

In `sparkless/backend/robin/materializer.py`, in `_simple_filter_to_robin`:
- Add support for `ColumnOperation(like, column, pattern)` when `pattern` is a string: translate to Robin’s like if available (e.g. `inner.like(pattern)` or `F.like(inner, pattern)`).
- Add support for unary `isnull` and `isnotnull`: translate to Robin’s `inner.isnull()` / `inner.isnotnull()` or equivalent.
- Do **not** add support for `isin([])` (empty list); document that filter with `isin([])` is unsupported (Robin #244).

Ref: `_simple_filter_to_robin` in materializer.py; docs/robin_limitations_sparkless_fixes.md.
