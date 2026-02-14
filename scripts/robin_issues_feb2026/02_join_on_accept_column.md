## Summary

PySpark's `DataFrame.join(other, on=...)` accepts `on` as a Column expression or list of column names. Robin-sparkless requires `on` to be str or list/tuple of str only, causing `TypeError: argument 'on': join 'on' must be str or list/tuple of str` when a Column or other type is passed.

**Request:** Accept Column expression(s) for join `on` for PySpark parity (e.g. `on=F.col("a") == df2.col("a")` or `on=["id"]`).

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
left = create_df([{"id": 1, "v": 10}], [("id", "int"), ("v", "int")])
right = create_df([{"id": 1, "w": 20}], [("id", "int"), ("w", "int")])

# PySpark allows: left.join(right, left.id == right.id) or left.join(right, "id")
result = left.join(right, rs.col("id"))  # or on=Column expression
result.collect()
```

Error: `TypeError: argument 'on': join 'on' must be str or list/tuple of str`

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
left = spark.createDataFrame([(1, 10)], ["id", "v"])
right = spark.createDataFrame([(1, 20)], ["id", "w"])

left.join(right, "id").collect()
# or left.join(right, left.id == right.id)
spark.stop()
```

PySpark accepts string column name(s) or Column expression for join condition.
