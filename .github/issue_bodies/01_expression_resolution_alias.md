## Summary

With the Robin backend, select/withColumn often fail with `RuntimeError: not found: Column 'map_col'` (or `'full_name'`, `'row_plus_10'`, etc.). Robin resolves columns by **name**; when Sparkless sends an alias name or a stringified expression instead of a Robin Column, Robin looks up that string as a column and fails.

## Failure (current behavior)

```python
from sparkless.sql import SparkSession
from sparkless.functions import F

spark = SparkSession.builder().master("local").config("spark.sql.extensions", "sparkless").get_or_create()
# Ensure Robin backend
spark.conf.set("spark.sql.extensions", "sparkless")

df = spark.createDataFrame([("alice", "x"), ("bob", "y")], ["first_name", "last_name"])

# FAILS: RuntimeError: not found: Column 'full_name' not found
result = df.select(F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("full_name"))
result.collect()
```

```python
# With create_map: FAILS with not found: Column 'map_col' not found
df = spark.createDataFrame([(1, "a", 2, "b")], ["k1", "v1", "k2", "v2"])
result = df.withColumn("map_col", F.create_map(F.col("k1"), F.col("v1"), F.col("k2"), F.col("v2")))
result.collect()
```

## Expected behavior

- `df.select(expr.alias("full_name"))` should produce a single column named `full_name` with the evaluated expression value; Robin should receive a Column expression (e.g. `concat_ws(" ", col("first_name"), col("last_name")).alias("full_name")`), not the string `"full_name"`.
- `df.withColumn("map_col", F.create_map(...))` should add a column named `map_col`; Robin should receive the evaluated map expression and the name, not look up a column named `map_col`.

## Fix (Sparkless-side)

- In the Robin materializer select branch: ensure every aliased expression is translated via `_expression_to_robin` and the result has `.alias(alias_name)` applied so the output column name is the alias.
- In robin_plan.py `_expr_to_robin`: handle serialized alias (op "alias", left=expr, right=literal name) so the plan path produces a Robin-format alias.
- In plan_executor `robin_expr_to_column`: ensure alias is applied so the output column name is the alias.

Ref: `sparkless/backend/robin/materializer.py` (select ~964–990, withColumn ~999–1072), `sparkless/dataframe/robin_plan.py`, `sparkless/backend/robin/plan_executor.py`.
