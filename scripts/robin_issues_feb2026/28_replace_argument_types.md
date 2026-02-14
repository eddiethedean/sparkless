## Summary

PySpark Column.replace accepts dict and list for to_replace and value. Robin-sparkless replace is stricter: TypeError replace to_replace and value must be None int float bool or str.

**Request:** Accept dict and list for to_replace/value where PySpark does.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"x": "a"}, {"x": "b"}], [("x", "string")])
df.with_column("x", rs.col("x").replace({"a": "A", "b": "B"})).collect()
```

Error: replace to_replace and value must be None int float bool or str

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a",), ("b",)], ["x"])
df.withColumn("x", F.col("x").replace({"a": "A", "b": "B"})).collect()
spark.stop()
```

PySpark replace accepts dict and list-to-list.
