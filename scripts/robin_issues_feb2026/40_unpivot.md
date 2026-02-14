## Summary

PySpark DataFrame has unpivot (melt). Robin-sparkless does not.

**Request:** Implement DataFrame.unpivot for PySpark 3.4+ parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"id": 1, "a": 10, "b": 20}], [("id", "int"), ("a", "int"), ("b", "int")])
df.unpivot("id", ["a", "b"], "key", "value")
```

Error: DataFrame has no attribute unpivot

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, 10, 20)], ["id", "a", "b"])
df.unpivot("id", ["a", "b"], "key", "value").collect()
spark.stop()
```

PySpark 3.4+ has unpivot.
