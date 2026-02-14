## Summary

PySpark exposes `spark.conf.is_case_sensitive()` (or similar) to check whether SQL identifier resolution is case-sensitive. Robin's conf object does not expose this, leading to AttributeError: 'builtin_function_or_method' object has no attribute 'is_case_sensitive'.

**Request:** Expose a way to query case sensitivity for column/schema resolution (e.g. conf.get("spark.sql.caseSensitive") or conf.is_case_sensitive()) for compatibility.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
flag = spark.conf.is_case_sensitive()
```

Error: 'builtin_function_or_method' object has no attribute 'is_case_sensitive'

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
# PySpark: spark.conf.get("spark.sql.caseSensitive") or equivalent API
flag = spark.conf.get("spark.sql.caseSensitive", "false") == "true"
spark.stop()
```

PySpark allows reading case sensitivity configuration for SQL/column resolution.
