## Summary

PySpark Column.isin(*cols) accepts multiple values: col.isin(1, 2, 3) or col.isin([1,2,3]). Robin-sparkless has different signature: TypeError Column.isin() takes 1 positional arguments but 2 were given, or int cannot be converted to PyList.

**Request:** Support Column.isin(value1, value2, ...) and isin(list) for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"x": 1}, {"x": 2}, {"x": 3}], [("x", "int")])
df.filter(rs.col("x").isin(1, 2)).collect()
```

Error: Column.isin() takes 1 positional arguments but 2 were given

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
df.filter(F.col("x").isin(1, 2)).collect()
spark.stop()
```

PySpark isin accepts variadic values or a list.
