## Summary

PySpark `F.struct(col1, col2, ...)` accepts multiple Column arguments. Robin-sparkless struct (e.g. py_struct) has a different signature: `TypeError: py_struct() takes 1 positional arguments but 2 were given` when passing multiple columns.

**Request:** Provide struct() that accepts multiple Column arguments like PySpark (or a PySpark-compatible wrapper).

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"a": 1, "b": 2}], [("a", "int"), ("b", "int")])
df.select(rs.struct(rs.col("a"), rs.col("b")).alias("s")).collect()
```

Error: py_struct() takes 1 positional arguments but 2 were given

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, 2)], ["a", "b"])
df.select(F.struct(F.col("a"), F.col("b")).alias("s")).collect()
spark.stop()
```

PySpark struct() accepts variadic Column arguments.
