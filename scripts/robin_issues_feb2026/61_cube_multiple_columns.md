## Summary

PySpark DataFrame.cube accepts multiple column names as star-args or a list. Robin DataFrame.cube() takes only one positional argument, so cube("dept", "year") or cube(["dept", "year"]) fails.

**Request:** Support cube with multiple columns (variadic or list/tuple) for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df(
    [{"dept": "A", "year": 2023}, {"dept": "B", "year": 2023}],
    [("dept", "str"), ("year", "int")]
)
result = df.cube("dept", "year").count()
```

Error: DataFrame.cube() takes 1 positional arguments but 2 were given

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("A", 2023), ("B", 2023)], ["dept", "year"])
df.cube("dept", "year").count()
df.cube(["dept", "year"]).count()
spark.stop()
```

PySpark cube accepts multiple column names as star-args or a list.
