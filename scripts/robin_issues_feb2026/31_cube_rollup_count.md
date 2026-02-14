## Summary

PySpark Cube and Rollup GroupedData support .count(), .agg(), etc. Robin-sparkless CubeRollupData (or equivalent) may not expose count: `AttributeError: 'builtins.CubeRollupData' object has no attribute 'count'`, breaking cube/rollup aggregations.

**Request:** Implement .count() (and standard agg methods) on cube/rollup grouped data for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"a": 1, "b": 2, "v": 10}], [("a", "int"), ("b", "int"), ("v", "int")])
gd = df.cube("a", "b")
gd.count().collect()
```

Error: 'builtins.CubeRollupData' object has no attribute 'count'

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, 2, 10)], ["a", "b", "v"])
df.cube("a", "b").count().collect()
spark.stop()
```

PySpark cube() and rollup() return GroupedData that supports count(), agg(), etc.
