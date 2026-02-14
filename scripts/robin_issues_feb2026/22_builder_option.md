## Summary

PySpark's session builder supports `.option(key, value)` for configuration (e.g. `SparkSession.builder().config("spark.app.name", "myapp").getOrCreate()`). Robin-sparkless builder does not expose this: `AttributeError: 'builtin_function_or_method' object has no attribute 'option'` (or no `.config()`), so callers cannot pass session options in a PySpark-compatible way.

**Request:** Add `builder().option(key, value)` and/or `builder().config(key, value)` on SparkSession for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

builder = rs.SparkSession.builder().app_name("test")
builder.option("spark.sql.shuffle.partitions", "2")
spark = builder.get_or_create()
```

Error: 'builtin_function_or_method' object has no attribute 'option'

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("test")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
# or .option("spark.sql.shuffle.partitions", "2")
spark.stop()
```

PySpark supports both .config() and .option() on the builder.
