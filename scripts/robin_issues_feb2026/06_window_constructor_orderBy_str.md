## Summary

PySpark supports `Window.partitionBy("col").orderBy("col")` with string column names and `Window()` (no-arg) for unbounded window. Robin-sparkless raises `TypeError: No constructor defined for Window` or `descriptor 'orderBy' for 'builtins.Window' objects doesn't apply to a 'str' object` when strings are passed.

**Request:** Provide Window constructor and allow partitionBy/orderBy to accept string column names for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"dept": "A", "salary": 100}, {"dept": "A", "salary": 200}], [("dept", "string"), ("salary", "int")])
w = rs.Window.partition_by("dept").order_by("salary")
df.select("dept", "salary", rs.row_number().over(w).alias("rn")).collect()
```

Error: `No constructor defined for Window` or orderBy doesn't apply to str.

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("A", 100), ("A", 200)], ["dept", "salary"])
w = Window.partitionBy("dept").orderBy("salary")
df.select("dept", "salary", F.row_number().over(w).alias("rn")).collect()
spark.stop()
```

PySpark Window accepts string column names in partitionBy/orderBy.
