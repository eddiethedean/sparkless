## Summary

[Parity] greatest() and least() should accept multiple column arguments (variadic), like PySpark.

## Expected (PySpark)

- `F.greatest(F.col("a"), F.col("b"), F.col("c"))` returns a column of element-wise max.
- `F.least(F.col("a"), F.col("b"), F.col("c"))` returns a column of element-wise min.
- PySpark: OK (3 rows with g=3, l=1 etc.).

## Actual (Robin 0.9.1)

- `TypeError: py_greatest() takes 1 positional arguments but 3 were given`
- Same for `py_least()`.

## Repro

Run from sparkless repo root:

```bash
python scripts/robin_parity_repros_0.9.1/greatest_least_variadic.py
```

### Captured output (2026-02-13)

```
greatest_least_variadic: F.greatest(a,b,c) and F.least(a,b,c)
Robin: FAILED ['greatest/least: TypeError: py_greatest() takes 1 positional arguments but 3 were given']
PySpark: OK ['greatest/least: 3 rows, e.g. Row(a=1, b=2, c=3, g=3, l=1)']
```

## Affected Sparkless tests

- `tests/parity/functions/test_math.py::TestMathFunctionsParity::test_math_greatest`
- `tests/parity/functions/test_math.py::TestMathFunctionsParity::test_math_least`
