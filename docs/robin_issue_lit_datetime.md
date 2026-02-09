## Summary

`lit()` currently accepts only `None`, `int`, `float`, `bool`, and `str`. Passing a `date` or `datetime` (e.g. from Python's `datetime.date` / `datetime.datetime`) raises:

```
TypeError: lit() supports only None, int, float, bool, str
```

For PySpark parity, it would be useful to extend `lit()` to accept date/datetime types (and optionally other common types).

## Context

PySpark's `F.lit()` can accept date and timestamp values. Integrations like [Sparkless](https://github.com/eddiethedean/sparkless) use literals in filters and expressions (e.g. `col("dt") > lit(some_date)`). Sparkless currently works around by serializing date/datetime to string (e.g. ISO format) before calling `F.lit()`, which avoids the error but loses type semantics in the expression. Extending `lit()` would allow proper date/datetime literals and better parity with PySpark.

## Suggested behavior

- Accept `datetime.date` and `datetime.datetime` (and optionally `datetime.time`) in `lit()`.
- Represent them in the engine in a way that preserves date/timestamp semantics (e.g. for comparisons and date functions).

## Priority

Low / optional; Sparkless has a workaround. This is an enhancement for consistency and convenience.

## Environment

- robin-sparkless: 0.3.0
- Python: 3.x

## References

- Sparkless Robin needs doc: [robin_sparkless_needs.md](https://github.com/eddiethedean/sparkless/blob/main/docs/robin_sparkless_needs.md) (§2c)
