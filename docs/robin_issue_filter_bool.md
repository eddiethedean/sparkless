## Summary

When a Python `bool` (e.g. `True` or `False`) is passed to `DataFrame.filter(condition)`, robin-sparkless raises:

```
TypeError: argument 'condition': 'bool' object cannot be converted to 'Column'
```

For PySpark parity and easier integration, it would help to either (a) document that `filter()` expects a `Column` only, or (b) accept literal `True`/`False` and treat them as "no filter" / "filter none" (or equivalent).

## Context

Integrations like [Sparkless](https://github.com/eddiethedean/sparkless) can sometimes evaluate a constant condition to a Python bool (e.g. when optimizing or when the condition is literally `lit(True)`). Passing that bool to Robin's `filter()` triggers the TypeError. Sparkless can work around by normalizing to a Column or skipping the filter; the request here is for robin-sparkless to make this case well-defined (documentation or accepted behavior).

## Suggested behavior (optional)

- **Option A:** Document that `condition` must be a `Column`; leave current behavior as-is (callers must not pass bool).
- **Option B:** If `condition` is literal `True`, treat as no-op (no filter); if literal `False`, filter to zero rows (or no-op, depending on desired semantics). Other types still require Column.

## Environment

- robin-sparkless: 0.3.0
- Python: 3.x

## References

- Sparkless Robin needs doc: [robin_sparkless_needs.md](https://github.com/eddiethedean/sparkless/blob/main/docs/robin_sparkless_needs.md) (§2b)
