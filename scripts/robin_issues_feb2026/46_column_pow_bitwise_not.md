## Summary

PySpark Column supports pow and bitwise NOT. Robin raises unsupported operand type for pow or unary tilde.

**Request:** Implement Column pow and bitwise NOT for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"x": 2}], [("x", "int")])
df.select(rs.col("x").cast("int")).collect()
df.select((rs.col("x") * rs.col("x")).alias("sq")).collect()
```

Use pow(x, 2) or x ** 2; Robin fails with pow or invert.

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(2,)], ["x"])
df.select(F.pow(F.col("x"), 2)).collect()
spark.stop()
```

PySpark Column supports pow and bitwise ops.
