## Summary

PySpark GroupedData has `.pivot(pivot_col, values=None)` for pivot tables. Robin-sparkless GroupedData does not: `AttributeError: 'builtins.GroupedData' object has no attribute 'pivot'`.

**Request:** Implement GroupedData.pivot() for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"region": "N", "year": 2023, "sales": 100}, {"region": "S", "year": 2023, "sales": 200}], [("region", "string"), ("year", "int"), ("sales", "int")])
df.group_by("year").pivot("region").sum("sales").collect()
```

Error: `AttributeError: 'builtins.GroupedData' object has no attribute 'pivot'`

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("N", 2023, 100), ("S", 2023, 200)], ["region", "year", "sales"])
df.groupBy("year").pivot("region").sum("sales").collect()
spark.stop()
```

PySpark produces a pivot table with year and columns per region.
