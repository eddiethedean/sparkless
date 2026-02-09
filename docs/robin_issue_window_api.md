## Summary

Window functions (e.g. `row_number()`, `rank()`, `dense_rank()`, `sum(...).over(...)`, `lag()`, `lead()`) are a standard part of the PySpark DataFrame API. For parity, robin-sparkless could provide a Window (or window-spec) API and Column expressions that use it, so that callers can do e.g.:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df.select(
    "*",
    F.row_number().over(Window.partitionBy("dept").orderBy(F.col("salary").desc())).alias("rn"),
    F.sum("amount").over(Window.partitionBy("id").orderBy("ts").rowsBetween(-2, 0)).alias("rolling_sum"),
)
```

## Context

[Sparkless](https://github.com/eddiethedean/sparkless) does not yet send window operations to the Robin materializer (it raises SparkUnsupportedOperationError for window ops when using the Robin backend). Adding window support in Sparkless would require robin-sparkless to:

1. Expose a Window (or equivalent) abstraction: partition by columns, order by columns, optional frame (rows between / range between).
2. Support Column expressions that are "windowed", e.g. `F.row_number().over(window_spec)`, `F.sum(col).over(window_spec)`, `F.lag(col, n).over(window_spec)`.

This issue depends on **#182** (select/with_column accepting Column expressions); once expressions are evaluated in `select()`, window expressions would be passed as Column and evaluated the same way.

## Priority

Future / medium. Not blocking current Sparkless Robin support; would unblock window-based parity tests once the materializer supports window ops.

## References

- Sparkless Robin needs doc: [robin_sparkless_needs.md](https://github.com/eddiethedean/sparkless/blob/main/docs/robin_sparkless_needs.md) (§5)
- Depends on: #182 (Column expressions in select/with_column)
