## Summary

PySpark SparkSession exposes `.mode` (for DataFrameWriter), builder `.option(key, value)`, and catalog methods like `spark.catalog.listDatabases()`, `createDatabase()`. Robin-sparkless exposes these as functions or with different shape: `AttributeError: 'function' object has no attribute 'mode'`, `'builtin_function_or_method' object has no attribute 'option'` / `listDatabases` / `createDatabase`.

**Request:** Expose Session/catalog API compatible with PySpark (mode, option, listDatabases, createDatabase, etc.) for compatibility layers that expect them.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
# Expect session or writer to have .mode
getattr(spark, "mode", None) or getattr(spark.createDataFrame([]), "write", None).mode("overwrite")
# Or catalog
spark.catalog.listDatabases()
```

Error: 'function' object has no attribute 'mode', or catalog has no listDatabases

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
spark.catalog.listDatabases()
spark.stop()
```

PySpark has spark.catalog.listDatabases(), and DataFrameWriter has .mode().
