## Summary

Some tests fail with `SparkUnsupportedOperationError: Operation 'Operations: join' is not supported` or with join-related errors. The materializer already has a join branch (same-name on); the plan path does not support join (robin_plan raises). We should ensure the op path handles all join types and same-name keys used by tests; document or extend different-name join keys if needed.

## Failure (current behavior)

```python
from sparkless.sql import SparkSession
from sparkless.functions import F

spark = SparkSession.builder().master("local").get_or_create()
left = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
right = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "value"])

# May FAIL: Operation 'Operations: join' is not supported (e.g. if plan path is tried first)
# Or may fail if join condition uses different column names
result = left.join(right, on="id", how="inner")
result.collect()
```

```python
# Different-name join keys - may not be supported
right2 = spark.createDataFrame([(1, "x"), (2, "y")], ["id_right", "value"])
result = left.join(right2, left["id"] == right2["id_right"], how="inner")
```

## Expected behavior

- `left.join(right, on="id", how="inner")` should run on the op path and return joined rows when backend is Robin.
- Same-name join keys should work (materializer already supports this).
- Different-name keys (left.col == right.col) may be documented as a limitation or extended if Robin supports left_on/right_on.

## Fix (Sparkless-side)

- Keep join on the op path only; ensure `can_handle_join` and the materializer join branch handle inner, left, right, outer, full, and same-name on.
- Verify `_join_on_to_column_names` returns a list for conditions the tests use (e.g. single column name, list of names, ColumnOperation(==, col, col) with same name).
- If tests use different-name join keys, either add support (if Robin API allows) or document and skip/adapt those tests.

Ref: sparkless/backend/robin/materializer.py (join branch, _join_on_to_column_names).
