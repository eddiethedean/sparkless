## Summary

When the Robin **plan path** is used (e.g. `to_robin_plan`), orderBy with `F.col("x").desc()` or `.asc()` fails because the plan executor only accepts column specs with `"col"`; it does not handle `{"op": "desc", "left": {"col": "salary"}, "right": null}`.

## Failure (current behavior)

```python
from sparkless.sql import SparkSession
from sparkless.functions import F

spark = SparkSession.builder().master("local").get_or_create()
# Robin backend with plan path (e.g. spark.conf.set("spark.sparkless.useLogicalPlan", "true") if applicable)
df = spark.createDataFrame([(1, 100), (2, 50), (3, 75)], ["id", "salary"])

# When plan path is used: ValueError: orderBy currently supports only column references in plan
result = df.orderBy(F.col("salary").desc())
result.collect()
```

Error from `sparkless/backend/robin/plan_executor.py`:
```
ValueError: orderBy currently supports only column references in plan
```

## Expected behavior

- `df.orderBy(F.col("salary").desc())` should sort by `salary` descending.
- The plan executor should, for each orderBy column spec:
  - If `e` has `"col"`: use it as column name and use the corresponding element of `ascending`.
  - If `e` has `"op": "desc"` (or desc_nulls_last, etc.): extract column name from `e["left"]["col"]` and set ascending to False.
  - If `e` has `"op": "asc"`: extract from left and set ascending to True.
- Then call `df.order_by(col_names, asc_list)`.

## Fix (Sparkless-side)

In `sparkless/backend/robin/plan_executor.py`, in the orderBy branch (around lines 207–223): for each `e` in `col_specs`, if `e` is a dict with `"col"`, use it; else if `e` has `"op"` in (`"desc"`, `"desc_nulls_last"`, `"desc_nulls_first"`), extract column name from `e.get("left", {}).get("col")` and set ascending to False; else if `e["op"]` in (`"asc"`, ...), extract and set True. Build `col_names` and `asc_list` and call `df.order_by(col_names, asc_list)`.
