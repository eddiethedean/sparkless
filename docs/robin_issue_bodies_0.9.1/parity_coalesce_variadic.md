## Summary

[Parity] coalesce() should accept multiple column/expression arguments (variadic), like PySpark.

## Expected (PySpark)

- `F.coalesce(F.col("a"), F.col("b"))` returns first non-null of a, b per row.
- PySpark: OK (e.g. 3 rows with c=1, c=2, c=None).

## Actual (Robin 0.9.1)

- `TypeError: py_coalesce() takes 1 positional argument but 2 were given`

## Repro

Run from sparkless repo root:

```bash
python scripts/robin_parity_repros_0.9.1/coalesce_variadic.py
```

### Captured output (2026-02-13)

```
coalesce_variadic: F.coalesce(col1, col2)
Robin: FAILED ['coalesce: TypeError: py_coalesce() takes 1 positional arguments but 2 were given']
PySpark: OK ['coalesce: 3 rows, e.g. Row(a=1, b=None, c=1)']
```

## Affected Sparkless tests

- `tests/parity/functions/test_null_handling.py::TestNullHandlingFunctionsParity::test_coalesce`
