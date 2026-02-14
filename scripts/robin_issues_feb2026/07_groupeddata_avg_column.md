## Summary

PySpark GroupedData.avg() accepts Column or column name: `df.groupBy("x").avg(F.col("y"))` or `avg("y")`. Robin-sparkless requires column names as strings only: `TypeError: avg() column names must be str: 'Column' object cannot be converted to 'PyString'`.

**Request:** Allow avg() (and similar agg functions) to accept Column expressions for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"k": "a", "v": 10}, {"k": "a", "v": 20}], [("k", "string"), ("v", "int")])
df.group_by("k").avg(rs.col("v")).collect()
```

Error: `avg() column names must be str: TypeError: 'Column' object cannot be converted to 'PyString'`

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 10), ("a", 20)], ["k", "v"])
df.groupBy("k").avg(F.col("v")).collect()
spark.stop()
```

PySpark accepts both avg("v") and avg(F.col("v")).
