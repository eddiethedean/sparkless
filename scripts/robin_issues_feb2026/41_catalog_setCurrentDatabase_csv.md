## Summary

PySpark catalog has setCurrentDatabase. Reader/Writer have csv. Robin-sparkless does not.

**Request:** Expose catalog.setCurrentDatabase and read.csv / write.csv for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
spark.catalog.setCurrentDatabase("default")
df = spark.read.csv("/tmp/x.csv")
```

Error: no setCurrentDatabase; read has no csv

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
spark.catalog.setCurrentDatabase("default")
df = spark.read.csv("/tmp/x.csv")
spark.stop()
```

PySpark has these APIs.
