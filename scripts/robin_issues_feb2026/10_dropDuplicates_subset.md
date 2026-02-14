## Summary

PySpark DataFrame has dropDuplicates(subset=None) where subset is an optional list of column names. Robin-sparkless only provides distinct() with no subset.

**Request:** Implement dropDuplicates(subset=[...]) for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 2, "b": 1}], [("a", "int"), ("b", "int")])
df.drop_duplicates(subset=["a"]).collect()
```

Error: dropDuplicates(subset=...) not supported

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, 1), (1, 2), (2, 1)], ["a", "b"])
df.dropDuplicates(subset=["a"]).collect()
spark.stop()
```

PySpark keeps one row per distinct a.
