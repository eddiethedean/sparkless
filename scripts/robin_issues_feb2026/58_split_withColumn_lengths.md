## Summary

When using `F.split(column, pattern, limit)` and adding the result with `withColumn`, PySpark adds one column whose values are arrays (one array per row). Robin fails with "lengths don't match: unable to add a column of length N to a DataFrame of height 1", i.e. it treats the array elements as row count instead of a single array value per row.

**Request:** withColumn(name, split(...)) should add one column of array type per row, not expand by array length.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"s": "A,B,C"}], [("s", "str")])
df = df.withColumn("arr", rs.split(rs.col("s"), ",", 3))
# Fails: lengths don't match: unable to add a column of length 3 to a DataFrame of height 1
df.collect()
```

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("A,B,C",)], ["s"])
df = df.withColumn("arr", F.split(F.col("s"), ",", 3))
rows = df.collect()
# One row; arr is an array of 3 elements e.g. ["A", "B", "C"]
spark.stop()
```

PySpark adds one column per row whose value is the array result of split.
