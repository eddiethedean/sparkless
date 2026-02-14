## Summary

PySpark `F.greatest(col1, col2, ...)` and `F.least(col1, col2, ...)` accept variadic Column arguments. Robin `py_greatest()` and `py_least()` accept only one positional argument.

**Request:** Support variadic greatest/least for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"a": 1, "b": 2, "c": 3}], [("a", "int"), ("b", "int"), ("c", "int")])
df.select(rs.greatest(rs.col("a"), rs.col("b"), rs.col("c"))).collect()
df.select(rs.least(rs.col("a"), rs.col("b"), rs.col("c"))).collect()
```

Error: py_greatest() / py_least() takes 1 positional arguments but 3 were given

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
df.select(F.greatest(F.col("a"), F.col("b"), F.col("c"))).collect()
df.select(F.least(F.col("a"), F.col("b"), F.col("c"))).collect()
spark.stop()
```

PySpark greatest/least accept two or more Column arguments.
