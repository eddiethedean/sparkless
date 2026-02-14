## Summary

PySpark format_string(format, col1, col2, ...) accepts format plus variadic columns. Robin py_format_string takes 2 args only.

**Request:** Support format_string with variadic columns for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"a": 1, "b": 2}], [("a", "int"), ("b", "int")])
df.select(rs.format_string("a=%d b=%d", rs.col("a"), rs.col("b"))).collect()
```

Error: py_format_string takes 2 positional arguments but 3 given

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, 2)], ["a", "b"])
df.select(F.format_string("a=%d b=%d", F.col("a"), F.col("b"))).collect()
spark.stop()
```

PySpark format_string accepts format plus variadic columns.
