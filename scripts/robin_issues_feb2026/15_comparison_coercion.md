## Summary

PySpark allows some comparisons between string and numeric/date. Robin-sparkless is stricter and raises when comparing string with numeric or date with string.

**Request:** Align comparison behavior with PySpark or document strict typing.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"id": 1, "label": "1"}], [("id", "int"), ("label", "string")])
df.filter(rs.col("id") == rs.col("label")).collect()
```

Error: cannot compare string with numeric type

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, "1")], ["id", "label"])
df.filter(F.col("id") == F.col("label")).collect()
spark.stop()
```

PySpark may coerce and return matching rows.
