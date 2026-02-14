## Summary

In PySpark, F.posexplode(column) returns a Column-like that supports .alias("pos", "val") (one or two names for position and value). In Robin, posexplode appears to return a tuple, so .alias(...) fails with AttributeError: tuple object has no attribute alias.

**Request:** posexplode() should return a Column (or struct-like) that supports .alias() with one or two names for PySpark parity in select/withColumn.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"id": 1, "arr": [10, 20]}], [("id", "int"), ("arr", "list<int>")])
df.select("id", rs.posexplode("arr").alias("pos", "val")).collect()
```

Error: tuple object has no attribute alias

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, [10, 20])], ["id", "arr"])
df.select("id", F.posexplode("arr").alias("pos", "val")).collect()
spark.stop()
```

PySpark posexplode returns a Column that supports .alias("pos", "val") for the position and value columns.
