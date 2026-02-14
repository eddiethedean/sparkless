## Summary

PySpark regexp_extract (and similar) supports lookahead/lookbehind in regex patterns. Robin-sparkless regex engine does not: `RuntimeError: regex parse error: look-around, including look-ahead and look-behind, is not supported`.

**Request:** Support lookahead/lookbehind in regex where used by regexp_extract (or document limitation).

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"s": "hello world"}], [("s", "string")])
# Lookbehind: (?<=hello )\w+
df.select(rs.regexp_extract(rs.col("s"), r"(?<=hello )\w+", 0)).collect()
```

Error: look-around, including look-ahead and look-behind, is not supported

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("hello world",)], ["s"])
df.select(F.regexp_extract(F.col("s"), r"(?<=hello )\w+", 0)).collect()
spark.stop()
```

PySpark returns "world" for the lookbehind pattern.
