## Summary

PySpark select("*") or select("*", "a") expands to all columns. Robin may not: RuntimeError not found Column * not found. select("*") should be treated as all columns.

**Request:** Support select("*") and select("*", col) to mean all columns (and optionally extra) for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"dept": "IT", "name": "Alice"}], [("dept", "string"), ("name", "string")])
df.select("*").collect()
```

Error: not found Column * not found

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("IT", "Alice")], ["dept", "name"])
df.select("*").collect()
spark.stop()
```

PySpark select("*") returns all columns.
