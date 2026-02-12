## Summary

CaseWhen in select/withColumn can fail with `not found: CASE WHEN ... THEN ... ELSE ... END` when the **plan path** is used. CaseWhen is already translated in `_expression_to_robin` (op path); the plan path may not represent or convert CaseWhen, so the plan builder raises or the executor receives a form it cannot handle.

## Failure (current behavior)

```python
from sparkless.sql import SparkSession
from sparkless.functions import F

spark = SparkSession.builder().master("local").get_or_create()
df = spark.createDataFrame([(1, 10), (2, 50), (3, 90)], ["id", "score"])

# When plan path is used: can fail with not found: Column 'CASE WHEN ...' or SparkUnsupportedOperationError
result = df.select(
    F.when(F.col("score") >= 50, "pass").otherwise("fail").alias("result")
)
result.collect()
```

## Expected behavior

- `df.select(F.when(cond).then(val).otherwise(default).alias("result"))` should produce one column `result` with values "pass"/"fail" (or equivalent).
- Both the operation path and the plan path should support CaseWhen: op path via `_expression_to_robin` (already has CaseWhen branch); plan path via serialization in logical_plan and conversion in robin_plan `_expr_to_robin` to a when/then/otherwise form that plan_executor can execute.

## Fix (Sparkless-side)

- In `sparkless/dataframe/logical_plan.py`: ensure CaseWhen is serialized (e.g. as a structured op or type the plan format supports).
- In `sparkless/dataframe/robin_plan.py` `_expr_to_robin`: add a branch for the CaseWhen serialization shape; produce Robin-format when/then/otherwise (e.g. op "when" with condition and branches) if the plan format supports it.
- In `sparkless/backend/robin/plan_executor.py` `robin_expr_to_column`: if the plan format includes when/then/otherwise, build the Robin Column (e.g. `F.when(...).then(...).otherwise(...)`).
- If the plan format does not support CaseWhen, the plan builder will raise and the engine falls back to the op path; ensure the op path always uses the CaseWhen branch of `_expression_to_robin` for select/withColumn.

Ref: sparkless/dataframe/robin_plan.py, sparkless/dataframe/logical_plan.py, sparkless/backend/robin/plan_executor.py.
