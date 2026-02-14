## Summary

PySpark DataFrame.agg(*exprs) accepts multiple aggregation expressions: df.agg(F.sum("a"), F.avg("b")). Robin-sparkless may have a different signature: `TypeError: DataFrame.agg() takes 1 positional argument but 3 were given` when passing multiple exprs, so global aggregations with more than one expr fail.

**Request:** Support DataFrame.agg(expr1, expr2, ...) with multiple Column expressions for PySpark parity (global aggregation without groupBy).

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"a": 10, "b": 20}, {"a": 30, "b": 40}], [("a", "int"), ("b", "int")])
df.agg(rs.sum("a"), rs.avg("b")).collect()
```

Error: DataFrame.agg() takes 1 positional argument but 3 were given

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(10, 20), (30, 40)], ["a", "b"])
df.agg(F.sum("a"), F.avg("b")).collect()
spark.stop()
```

PySpark agg accepts multiple expressions and returns one row with multiple columns.
