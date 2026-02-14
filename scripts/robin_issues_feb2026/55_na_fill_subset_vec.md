## Summary

PySpark na.fill(value, subset=None) accepts subset as list of column names. Robin may expect different type: TypeError argument subset Can't extract str to Vec when passing list of strings.

**Request:** Accept subset as list of column name strings in na.fill() for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"a": 1, "b": None}], [("a", "int"), ("b", "int")])
df.na.fill(0, subset=["b"]).collect()
```

Error: argument subset Can't extract str to Vec

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, None)], ["a", "b"])
df.na.fill(0, subset=["b"]).collect()
spark.stop()
```

PySpark na.fill accepts subset as list of str.
