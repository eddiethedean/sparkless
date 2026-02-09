## Summary

When filtering with a condition that compares two columns (e.g. `col("a") > col("b")`), robin-sparkless raises:

```
TypeError: '>' not supported between instances of 'builtins.Column' and 'builtins.Column'
```

PySpark's `DataFrame.filter()` accepts such conditions: both sides can be `Column` expressions. For parity, robin-sparkless should support comparisons where both operands are Columns (e.g. `col_a.gt(col_b)` or `col_a > col_b`).

## Context

Integrations like [Sparkless](https://github.com/eddiethedean/sparkless) translate filter conditions to Robin Columns and call `df.filter(robin_expr)`. When the condition is "column vs column" (e.g. `age > 30` is column vs literal — often works; `col("a") > col("b")` is column vs column — fails with the TypeError above). Sparkless builds the expression using the method API (e.g. `left.gt(right)`); if Robin's Column does not support `.gt(Column)` or the Python `>` operator when the right-hand side is a Column, the call fails.

## Expected behavior

- `df.filter(F.col("a") > F.col("b"))` (or `df.filter(F.col("a").gt(F.col("b")))`) should filter rows where the value in column `a` is greater than the value in column `b`.
- Same for other comparison operators: `<`, `>=`, `<=`, `==`, `!=`.

## Actual behavior

`TypeError: '>' not supported between instances of 'builtins.Column' and 'builtins.Column'` (or similar for other comparison methods).

## Environment

- robin-sparkless: 0.3.0
- Python: 3.x
- OS: any

## References

- Sparkless Robin needs doc: [robin_sparkless_needs.md](https://github.com/eddiethedean/sparkless/blob/main/docs/robin_sparkless_needs.md) (§2a)
- Related: #182 (select/with_column Column expressions)
