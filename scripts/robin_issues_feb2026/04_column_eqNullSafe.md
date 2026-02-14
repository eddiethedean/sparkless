## Summary

PySpark Column has `.eqNullSafe(other)` for null-safe equality (NULL-safe comparison). Robin-sparkless Column does not implement this, causing `AttributeError: 'builtins.Column' object has no attribute 'eqNullSafe'`.

**Request:** Implement `Column.eqNullSafe(other)` to match PySpark semantics (NULL eq NULL → true; NULL eq value → false).

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"a": 1}, {"a": None}], [("a", "int")])
df.select(rs.col("a").eqNullSafe(rs.lit(1))).collect()
```

Error: `AttributeError: 'builtins.Column' object has no attribute 'eqNullSafe'`

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,), (None,)], ["a"])
df.select(F.col("a").eqNullSafe(F.lit(1))).collect()
spark.stop()
```

PySpark returns a boolean column: true where a equals 1 or both are NULL; false otherwise.
