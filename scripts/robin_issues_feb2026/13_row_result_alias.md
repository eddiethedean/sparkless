## Summary

After `df.select(expr.alias("map_col"))`, PySpark Row objects use the alias as the key: `row["map_col"]`. Robin-sparkless result rows may use a different key (e.g. expression string), causing `KeyError: "Key 'map_col' not found in row"` when downstream code expects the alias.

**Request:** Ensure collect() / Row objects use the column alias as the key when an alias was set, for PySpark parity.

---

## Robin-sparkless (fails or inconsistent)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"x": 1}], [("x", "int")])
rows = df.select(rs.lit(42).alias("map_col")).collect()
val = rows[0]["map_col"]
```

If Row uses a different key (e.g. "42" or "lit(42)"), KeyError when accessing "map_col".

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,)], ["x"])
rows = df.select(F.lit(42).alias("map_col")).collect()
assert rows[0]["map_col"] == 42
spark.stop()
```

PySpark Row keys are the output column names (aliases).
