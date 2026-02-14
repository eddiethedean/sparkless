## Summary

PySpark hour() and other date functions can accept string timestamp columns. Robin: hour operation not supported for dtype str.

**Request:** Support date/time functions on string columns with parsing or document limitation.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"ts": "2024-01-15 14:30:00"}], [("ts", "string")])
df.select(rs.hour(rs.col("ts"))).collect()
```

Error: hour operation not supported for dtype str

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("2024-01-15 14:30:00",)], ["ts"])
df.select(F.hour(F.col("ts"))).collect()
spark.stop()
```

PySpark can parse string and apply hour().
