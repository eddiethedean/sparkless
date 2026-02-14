## Summary

PySpark DataFrame has `.alias(name)` for subquery aliasing (e.g. in join). Robin-sparkless DataFrame does not: `AttributeError: 'builtins.DataFrame' object has no attribute 'alias'`.

**Request:** Implement DataFrame.alias(name) for PySpark parity in joins/subqueries.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"id": 1, "v": 10}], [("id", "int"), ("v", "int")])
aliased = df.alias("t")
```

Error: 'builtins.DataFrame' object has no attribute 'alias'

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, 10)], ["id", "v"])
aliased = df.alias("t")
spark.stop()
```

PySpark returns a DataFrame with alias "t" for use in join/select.
