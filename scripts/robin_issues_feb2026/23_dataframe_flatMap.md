## Summary

PySpark DataFrame has flatMap (and map) to apply a function to each row and flatten. Robin-sparkless does not expose flatMap on DataFrame: AttributeError function object has no attribute flatMap.

**Request:** Implement DataFrame.flatMap(f) with PySpark-compatible semantics.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"word": "a b"}], [("word", "string")])
out = df.flatMap(lambda row: row["word"].split())
```

Error: function or DataFrame has no attribute flatMap

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import Row
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a b",)], ["word"])
out = df.rdd.flatMap(lambda row: [Row(word=w) for w in row.word.split()]).toDF()
out.collect()
spark.stop()
```

PySpark supports flatMap on RDD; DataFrame has rdd.flatMap or equivalent.
