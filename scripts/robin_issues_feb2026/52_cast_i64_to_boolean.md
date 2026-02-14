## Summary

PySpark allows casting numeric (e.g. int) to boolean (0 -> false, non-zero -> true). Robin may not: RuntimeError casting from i64 to boolean not supported.

**Request:** Support cast from integer/long to boolean for PySpark parity (0 false, non-zero true).

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"x": 0}, {"x": 1}], [("x", "int")])
df.select(rs.col("x").cast("boolean")).collect()
```

Error: casting from i64 to boolean not supported

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(0,), (1,)], ["x"])
df.select(F.col("x").cast("boolean")).collect()
spark.stop()
```

PySpark cast int to boolean: 0 false, non-zero true.
