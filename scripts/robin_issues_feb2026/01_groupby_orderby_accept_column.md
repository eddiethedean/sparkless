## Summary

PySpark accepts `Column` expressions for `groupBy()`, `orderBy()`, and aggregation column arguments (e.g. `agg(F.sum(F.col("x")))`). Robin-sparkless currently expects only string column names in these APIs, causing `TypeError: argument 'col': 'str' object cannot be converted to 'Column'` (or the reverse) when Sparkless/compat passes Column for PySpark parity.

**Request:** Accept Column expressions where PySpark does (groupBy, orderBy, agg column names), or accept both str and Column.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
df = create_df([{"dept": "A", "salary": 100}, {"dept": "A", "salary": 200}], [("dept", "string"), ("salary", "int")])

# PySpark allows groupBy(F.col("dept")) or groupBy("dept")
gd = df.group_by(rs.col("dept"))  # or df.groupBy(rs.col("dept"))
gd.agg(rs.sum(rs.col("salary"))).collect()
```

Error observed: `TypeError: argument 'col': 'str' object cannot be converted to 'Column'` (or similar when Column is passed where str expected).

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("A", 100), ("A", 200)], ["dept", "salary"])

gd = df.groupBy(F.col("dept"))
gd.agg(F.sum(F.col("salary"))).collect()
spark.stop()
```

PySpark accepts both `groupBy("dept")` and `groupBy(F.col("dept"))`; same for `orderBy` and agg column arguments.
