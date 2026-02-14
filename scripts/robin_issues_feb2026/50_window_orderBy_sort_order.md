## Summary

PySpark Window.orderBy accepts F.desc(col) and F.asc(col). Robin may reject: descriptor orderBy for Window does not apply to PySortOrder object.

**Request:** Allow Window.orderBy to accept sort order Column (asc/desc) for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"k": "a", "v": 10}, {"k": "a", "v": 20}], [("k", "string"), ("v", "int")])
w = rs.Window.partition_by("k").order_by(rs.desc("v"))
df.select("k", "v", rs.row_number().over(w)).collect()
```

Error: orderBy does not apply to PySortOrder

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 10), ("a", 20)], ["k", "v"])
w = Window.partitionBy("k").orderBy(F.desc("v"))
df.select("k", "v", F.row_number().over(w)).collect()
spark.stop()
```

PySpark Window.orderBy accepts F.asc and F.desc.
