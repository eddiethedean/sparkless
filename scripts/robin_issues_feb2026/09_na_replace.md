## Summary

PySpark DataFrame has `.na.replace(to_replace, value, subset=None)` for value replacement. Robin-sparkless does not implement this: `NotImplementedError: na.replace(to_replace, value, subset) not available on Robin backend`.

**Request:** Implement DataFrame.na.replace() for PySpark parity (or document as planned).

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"x": "a"}, {"x": "b"}], [("x", "string")])
df.na.replace("a", "A", subset=["x"]).collect()
```

Error: na.replace not available on Robin backend

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a",), ("b",)], ["x"])
df.na.replace("a", "A", subset=["x"]).collect()
spark.stop()
```

PySpark replaces "a" with "A" in column x.
