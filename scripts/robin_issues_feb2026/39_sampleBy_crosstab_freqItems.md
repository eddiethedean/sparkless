## Summary

PySpark has DataFrame.sampleBy, crosstab, freqItems. Robin-sparkless does not.

**Request:** Implement sampleBy, crosstab, freqItems for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"x": "a", "y": 1}], [("x", "string"), ("y", "int")])
df.sampleBy("x", {"a": 0.5})
df.crosstab("x", "y")
df.freqItems(["x"])
```

Error: no attribute sampleBy / crosstab / freqItems

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 1)], ["x", "y"])
df.sampleBy("x", {"a": 0.5})
df.crosstab("x", "y")
df.freqItems(["x"])
spark.stop()
```

PySpark has these methods.
