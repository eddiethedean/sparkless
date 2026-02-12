## Summary

Some failures show `not found: Column 'concat(first_name,  , last_name)'`. Concat-with-literal-separator → concat_ws is already implemented in the materializer; we should verify the pattern is detected for all AST shapes (e.g. concat as three-arg form) and add a test that runs with Robin.

## Failure (current behavior)

```python
from sparkless.sql import SparkSession
from sparkless.functions import F

spark = SparkSession.builder().master("local").get_or_create()
df = spark.createDataFrame([("alice", "x"), ("bob", "y")], ["first_name", "last_name"])

# FAILS: not found: Column 'concat(first_name,  , last_name)' (if concat_ws branch is not hit)
result = df.select(F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("full_name"))
result.collect()
```

## Expected behavior

- `F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))` should be translated to Robin’s `concat_ws(" ", col("first_name"), col("last_name"))` so that the result is one column with space-separated names.
- No column named literally `concat(first_name,  , last_name)` should be looked up; Robin should receive a single expression.

## Fix (Sparkless-side)

- In `sparkless/backend/robin/materializer.py`: verify the concat branch (lines ~597–613) detects the case where there is exactly one string Literal among the concat parts and the rest are columns; if the Sparkless AST uses a different structure (e.g. concat as a single call with three args in one wrapper), add a branch to normalize to (col, lit(sep), col) and then translate to concat_ws.
- Add a unit or parity test that runs with Robin: `df.select(F.concat(F.col("a"), F.lit(" "), F.col("b")).alias("full"))` and assert one column "full" with space-separated values.

Ref: sparkless/backend/robin/materializer.py (concat / concat_ws).
