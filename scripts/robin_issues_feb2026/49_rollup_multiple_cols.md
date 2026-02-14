## Summary

PySpark rollup accepts multiple columns. Robin may take only one: rollup() takes 1 positional argument but 2 were given.

**Request:** Support rollup(col1, col2, ...) for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"a": 1, "b": 2, "v": 10}], [("a", "int"), ("b", "int"), ("v", "int")])
df.rollup("a", "b").count().collect()
```

Error: rollup takes 1 positional argument but 2 given

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, 2, 10)], ["a", "b", "v"])
df.rollup("a", "b").count().collect()
spark.stop()
```

PySpark rollup accepts multiple columns.
