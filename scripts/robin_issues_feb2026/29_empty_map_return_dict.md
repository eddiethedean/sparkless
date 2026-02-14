## Summary

PySpark create_map() with no arguments returns a column that evaluates to empty dict in each row. Robin-sparkless may return empty list causing assert [] == {} failures.

**Request:** Ensure empty map result is represented as dict in Python row output for PySpark parity.

---

## Robin-sparkless (current)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"id": 1}], [("id", "int")])
rows = df.with_column("m", rs.create_map()).collect()
assert rows[0]["m"] == {}
```

Current: rows[0]["m"] may be [] so assertion fails.

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,)], ["id"])
rows = df.withColumn("m", F.create_map()).collect()
assert rows[0]["m"] == {}
spark.stop()
```

PySpark empty map is dict in row output.
