## Summary

PySpark Column has `.isNull()` and `.isNotNull()` (or `.isnotnull()`) for null checks. Robin-sparkless Column may not expose these or uses different names: `AttributeError: 'builtins.Column' object has no attribute 'isNull'` (or isnotnull), causing parity failures in filter/select with null checks.

**Request:** Implement Column.isNull() and Column.isNotNull() (or isnotnull()) returning a boolean Column for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"a": 1}, {"a": None}], [("a", "int")])
df.filter(rs.col("a").isNull()).collect()
```

Error: 'builtins.Column' object has no attribute 'isNull'

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,), (None,)], ["a"])
df.filter(F.col("a").isNull()).collect()
df.filter(F.col("a").isNotNull()).collect()
spark.stop()
```

PySpark Column has .isNull() and .isNotNull().
