## Summary

PySpark SparkSession has .sparkContext. Robin-sparkless does not. Compatibility code that uses spark.sparkContext fails.

**Request:** Expose sparkContext on SparkSession for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
ctx = spark.sparkContext
```

Error: SparkSession object has no attribute sparkContext

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
ctx = spark.sparkContext
spark.stop()
```

PySpark exposes spark.sparkContext.
