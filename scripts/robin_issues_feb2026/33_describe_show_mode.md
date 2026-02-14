## Summary

PySpark uses spark.sql("DESCRIBE TABLE t") and DataFrameWriter has .mode("overwrite") etc. Code that expects describe or show to return a result with a .mode attribute (or session/writer to have .mode) fails: `AttributeError: 'function' object has no attribute 'mode'`. Robin-sparkless may expose sql/describe/show as a raw function so that result or session does not have the expected .mode API.

**Request:** Expose describe table and show tables/columns in a PySpark-compatible way (e.g. spark.sql("DESCRIBE t") returning a DataFrame, and session or writer having .mode where applicable) so that compat layers and tests can call .mode on the appropriate object.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
# Describe table - may need a table to exist
spark.sql("DESCRIBE TABLE some_table")
# Or code that does: spark.conf.get("mode") or df.write.mode("overwrite")
getattr(spark, "conf", None) or getattr(spark, "mode", None)
```

Error: 'function' object has no attribute 'mode' when accessing describe/show result or session config

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,)], ["a"])
df.write.mode("overwrite").parquet("/tmp/t")
spark.sql("DESCRIBE TABLE parquet.`/tmp/t`").show()
spark.stop()
```

PySpark has spark.conf, df.write.mode(), and DESCRIBE returns a DataFrame.
