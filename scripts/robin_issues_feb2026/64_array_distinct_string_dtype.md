## Summary

PySpark `F.array_distinct(column)` works on array columns (including array of string). Robin fails with "array_distinct: invalid series dtype: expected `List`, got `str`" when the column or element type is string, or when the implementation expects a List type in a way that differs from PySpark.

**Request:** array_distinct should support array columns including string arrays, matching PySpark semantics.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"arr": ["a", "b", "a"]}], [("arr", "list<str>")])
df.select(rs.array_distinct(rs.col("arr"))).collect()
```

Error: array_distinct: invalid series dtype: expected `List`, got `str` (or similar)

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(["a", "b", "a"],)], ["arr"])
df.select(F.array_distinct(F.col("arr"))).collect()
# Result: one row with array ["a", "b"]
spark.stop()
```

PySpark array_distinct works on array columns of any element type including string.
