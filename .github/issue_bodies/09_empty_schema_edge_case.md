## Summary

`ValueError: RobinMaterializer requires a non-empty schema` is raised when the materializer is called with a schema that has no fields. This can occur in edge cases (e.g. empty DataFrame or certain operations). We should handle empty schema deterministically: either return data as-is for no-op plans, or raise a clearer error, or fix callers to avoid passing empty schema.

## Failure (current behavior)

```python
from sparkless.sql import SparkSession
from sparkless.spark_types import StructType

spark = SparkSession.builder().master("local").get_or_create()
# Edge case: empty schema (e.g. after a bug or specific op sequence)
empty_schema = StructType([])
data = []
# When materializer is invoked with empty schema:
# ValueError: RobinMaterializer requires a non-empty schema
```

(Exact repro may depend on how the lazy engine builds the schema for a given plan.)

## Expected behavior

- Either: (1) allow empty schema and return `data` as-is when operations are effectively no-op, or (2) raise a clear error like "Robin backend does not support empty schema; ensure at least one column" and document, or (3) ensure callers never call the materializer with empty schema (e.g. short-circuit in lazy engine).
- Behavior should be deterministic and documented.

## Fix (Sparkless-side)

In `sparkless/backend/robin/materializer.py` `materialize()`:
- When `schema.fields` is empty, choose one: (1) if `not operations` or operations are no-ops, return `data` as list of rows; (2) else raise a clear `ValueError` with a message that empty schema is not supported.
- Alternatively, handle empty schema in the caller (e.g. LazyEvaluationEngine) so the materializer is never called with empty schema.

Ref: sparkless/backend/robin/materializer.py (start of materialize()).
