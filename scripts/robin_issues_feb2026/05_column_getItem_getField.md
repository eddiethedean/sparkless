## Summary

PySpark Column supports `.getItem(i)` for array index, `.getField(name)` for struct field, and often `col["key"]` for map/struct. Robin-sparkless raises `AttributeError: 'builtins.Column' object has no attribute 'getItem'` / `getField` or `'builtins.Column' object is not subscriptable`.

**Request:** Implement getItem (array index), getField (struct field), and/or subscript `col[key]` / `col[i]` for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"arr": [1, 2, 3]}], [("arr", "array<int>")])  # or equivalent)
df.select(rs.col("arr").getItem(0)).collect()
# or struct: df.select(rs.col("s").getField("f")).collect()
```

Error: `AttributeError: 'builtins.Column' object has no attribute 'getItem'`

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([([1, 2, 3],)], ["arr"])
df.select(F.col("arr").getItem(0)).collect()
spark.stop()
```

PySpark Column has getItem(i), getField(name), and supports subscript for map/struct.
