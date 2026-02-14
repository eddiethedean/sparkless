## Summary

PySpark supports F.when(cond1, val1).when(cond2, val2).otherwise(default) chaining (WhenThen). Robin-sparkless WhenThen may not expose .when() for chaining: `AttributeError: WhenThen has no attribute 'when'` (or similar), so multi-branch case expressions fail.

**Request:** Support chained .when(condition, value) and .otherwise(default) on the result of when() for PySpark parity (CASE WHEN ... WHEN ... ELSE ...).

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"x": 1}, {"x": 2}, {"x": 3}], [("x", "int")])
expr = rs.when(rs.col("x") == 1, rs.lit("one")).when(rs.col("x") == 2, rs.lit("two")).otherwise(rs.lit("other"))
df.select(expr.alias("label")).collect()
```

Error: WhenThen has no attribute 'when' or otherwise not found

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
expr = F.when(F.col("x") == 1, F.lit("one")).when(F.col("x") == 2, F.lit("two")).otherwise(F.lit("other"))
df.select(expr.alias("label")).collect()
spark.stop()
```

PySpark WhenThen supports chained .when() and .otherwise().
