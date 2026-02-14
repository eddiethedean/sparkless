## Summary

PySpark DataFrame.filter(condition) and where(condition) accept a string SQL expression: df.filter("salary > 55000"). Robin interprets the string as a column name and raises: not found Column salary > 55000 not found.

**Request:** When filter/where receives a string, treat it as a SQL expression (expr) not a column name, for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"dept": "IT", "salary": 60000}], [("dept", "string"), ("salary", "int")])
df.filter("salary > 55000").collect()
```

Error: not found Column salary > 55000 not found

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("IT", 60000)], ["dept", "salary"])
df.filter("salary > 55000").collect()
spark.stop()
```

PySpark filter(string) evaluates string as SQL expression.
