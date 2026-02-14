## Summary

PySpark filter/where accept a Column or a literal bool. Robin may reject when the condition is built from Column operations: TypeError condition must be a Column or literal bool (True/False). Some compat layers pass Column expressions that Robin does not recognize as valid condition type.

**Request:** Accept any Column expression (including binary ops like col("a") > 1) as filter condition, not only literal bool, for PySpark parity.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"x": 1}, {"x": 2}], [("x", "int")])
df.filter(rs.col("x") > 1).collect()
```

Error: condition must be a Column or literal bool (if Robin receives wrapped/Sparkless Column)

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,), (2,)], ["x"])
df.filter(F.col("x") > 1).collect()
spark.stop()
```

PySpark filter accepts Column expression.
