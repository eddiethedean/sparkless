## Summary

PySpark allows `SparkSession.builder()` (with parentheses) as a callable that returns the builder; some code or generators use this form. Robin exposes `builder` as a property only; calling `builder()` raises TypeError (_SparkSessionBuilder object is not callable).

**Request:** Make SparkSession.builder callable so that builder() returns the same builder instance for method chaining (e.g. builder().appName("x").getOrCreate()).

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
# Many users or code generators write:
spark = rs.SparkSession.builder().app_name("my_app").get_or_create()
```

Error: '_SparkSessionBuilder' object is not callable (or descriptor doesn't apply)

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder().appName("my_app").getOrCreate()
# builder() returns the builder; chaining works
spark.stop()
```

PySpark supports both SparkSession.builder and SparkSession.builder().
