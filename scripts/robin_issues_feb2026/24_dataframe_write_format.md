## Summary

PySpark DataFrame has a `.write` property returning a DataFrameWriter, which has `.mode()`, `.format()`, `.save()`, `.parquet()`, etc. Robin-sparkless may expose write as a function or with a different shape: `AttributeError: 'function' object has no attribute 'format'` when code does `df.write.format("parquet").save(path)`.

**Request:** Expose DataFrame.write as an object with .mode(saveMode), .format(source), .save(path), and optionally .parquet(), .json(), etc., for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"id": 1}], [("id", "int")])
df.write.format("parquet").mode("overwrite").save("/tmp/out")
```

Error: 'function' object has no attribute 'format' or write has no format

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,)], ["id"])
df.write.format("parquet").mode("overwrite").save("/tmp/out")
spark.stop()
```

PySpark DataFrameWriter has .format(), .mode(), .save(), .parquet(), .json().
