## Summary

PySpark WindowSpec has `.rowsBetween(start, end)` and `.rangeBetween(start, end)` for frame bounds (e.g. unboundedPreceding, currentRow, unboundedFollowing). Robin-sparkless Window may not support these: `AttributeError: 'builtins.Window' object has no attribute 'rowsBetween'` when building a window with row/range frame.

**Request:** Implement WindowSpec.rowsBetween(start, end) and WindowSpec.rangeBetween(start, end) with PySpark-compatible constants (e.g. Window.unboundedPreceding, Window.currentRow, Window.unboundedFollowing) for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"k": "a", "v": 10}, {"k": "a", "v": 20}], [("k", "string"), ("v", "int")])
w = rs.Window.partition_by("k").order_by("v").rowsBetween(-1, 1)
df.select("k", "v", rs.sum("v").over(w)).collect()
```

Error: 'builtins.Window' object has no attribute 'rowsBetween' or equivalent

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 10), ("a", 20)], ["k", "v"])
w = Window.partitionBy("k").orderBy("v").rowsBetween(-1, 1)
df.select("k", "v", F.sum("v").over(w)).collect()
spark.stop()
```

PySpark WindowSpec has .rowsBetween() and .rangeBetween() with Window constants.
