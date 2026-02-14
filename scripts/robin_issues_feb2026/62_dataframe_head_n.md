## Summary

PySpark `DataFrame.head(n)` returns the first n rows as a list. Robin DataFrame.head() appears to be missing the required positional argument `n`, so head() fails with "DataFrame.head() missing 1 required positional argument: 'n'".

**Request:** Implement head(n) to return the first n rows (list of Row-like) for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"x": 1}, {"x": 2}, {"x": 3}], [("x", "int")])
rows = df.head(2)
```

Error: DataFrame.head() missing 1 required positional argument: 'n'

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
rows = df.head(2)
# Returns list of 2 Row objects
spark.stop()
```

PySpark head(n) returns the first n rows as a list.
