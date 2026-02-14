## Summary

PySpark DataFrame.orderBy(col, ascending=True) accepts a boolean for ascending. Robin-sparkless expects a different type: TypeError argument ascending bool object cannot be converted to Sequence.

**Request:** Accept a single boolean for ascending in orderBy/sort for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"x": 3}, {"x": 1}, {"x": 2}], [("x", "int")])
df.order_by("x", ascending=False).collect()
```

Error: argument ascending bool cannot be converted to Sequence

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(3,), (1,), (2,)], ["x"])
df.orderBy("x", ascending=False).collect()
spark.stop()
```

PySpark orderBy accepts ascending=True/False.
