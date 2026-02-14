## Summary

PySpark DataFrame has unionAll(other) as an alias for union(other). Robin-sparkless does not: AttributeError builtins.DataFrame object has no attribute unionAll.

**Request:** Implement DataFrame.unionAll() as alias for union() for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df1 = create_df([{"a": 1}], [("a", "int")])
df2 = create_df([{"a": 2}], [("a", "int")])
df1.unionAll(df2).collect()
```

Error: DataFrame object has no attribute unionAll

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df1 = spark.createDataFrame([(1,)], ["a"])
df2 = spark.createDataFrame([(2,)], ["a"])
df1.unionAll(df2).collect()
spark.stop()
```

PySpark unionAll is alias for union.
