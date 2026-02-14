## Summary

In some aggregations PySpark allows duplicate output column names (e.g. multiple agg expressions producing same logical name). Robin-sparkless raises `RuntimeError: duplicate: column with name 'value' has more than one occurrence` and rejects the result.

**Request:** Either allow duplicate column names in agg result where PySpark does, or document and suggest aliasing to unique names.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"g": "a", "value": 10}, {"g": "a", "value": 20}], [("g", "string"), ("value", "int")])
df.group_by("g").agg(rs.sum("value"), rs.avg("value")).collect()
```

If both outputs are named "value", Robin may error: duplicate column name.

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 10), ("a", 20)], ["g", "value"])
df.groupBy("g").agg(F.sum("value"), F.avg("value")).collect()
spark.stop()
```

PySpark typically produces sum(value) and avg(value) with distinct names or allows disambiguation.
