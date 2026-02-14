## Summary

PySpark `F.coalesce(col1, col2, ...)` accepts multiple Column arguments. Robin `py_coalesce()` accepts only one positional argument.

**Request:** Support variadic coalesce for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"salary": None}, {"salary": 100}], [("salary", "int")])
df.select(rs.coalesce(rs.col("salary"), rs.lit(0))).collect()
```

Error: py_coalesce() takes 1 positional arguments but 2 were given

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(None,), (100,)], ["salary"])
df.select(F.coalesce(F.col("salary"), F.lit(0))).collect()
spark.stop()
```

PySpark coalesce accepts two or more Column (or value) arguments.
