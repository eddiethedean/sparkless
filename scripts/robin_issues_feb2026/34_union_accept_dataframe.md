## Summary

PySpark df1.union(df2) accepts any DataFrame (including from the same session or a compat wrapper). Robin-sparkless union may require the exact native DataFrame type: `TypeError: argument 'other': '_PySparkCompatDataFrame' object cannot be converted to 'DataFrame'` when passing a wrapped DataFrame. For full compatibility, union() should accept any object that implements the same DataFrame protocol (or at least accept the same library's DataFrame type when one side is wrapped).

**Request:** Allow union() (and unionByName) to accept DataFrame-like objects or the same session's DataFrame even when wrapped by a compatibility layer (e.g. unwrap internally or accept duck-typed DataFrame).

---

## Robin-sparkless (fails when other is wrapped)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df1 = create_df([{"a": 1}], [("a", "int")])
df2 = create_df([{"a": 2}], [("a", "int")])
# If df2 is wrapped by Sparkless compat, union may reject it
df1.union(df2).collect()
```

Error: argument 'other': '_PySparkCompatDataFrame' object cannot be converted to 'DataFrame'

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df1 = spark.createDataFrame([(1,)], ["a"])
df2 = spark.createDataFrame([(2,)], ["a"])
df1.union(df2).collect()
spark.stop()
```

PySpark union accepts any DataFrame from the same session (and compat wrappers that quack like DataFrame).
