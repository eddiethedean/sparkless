## Summary

PySpark unionByName(allowMissingColumns=False) can match columns by name with case insensitivity in some configs. Robin-sparkless may require exact case match: `RuntimeError: lengths don't match: unable to vstack, column names don't match: "Age" and "AGE"`, breaking parity when data comes from different case sources.

**Request:** Support optional case-insensitive column name matching in unionByName (or a session/config option) for PySpark parity where column names differ only by case.

---

## Robin-sparkless (fails)

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df1 = create_df([{"Name": "Alice", "Age": 25}], [("Name", "string"), ("Age", "int")])
df2 = create_df([{"Name": "Bob", "AGE": 30}], [("Name", "string"), ("AGE", "int")])
df1.union_by_name(df2).collect()
```

Error: column names don't match: "Age" and "AGE"

---

## PySpark (expected)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df1 = spark.createDataFrame([("Alice", 25)], ["Name", "Age"])
df2 = spark.createDataFrame([("Bob", 30)], ["Name", "AGE"])
df1.unionByName(df2).collect()
spark.stop()
```

PySpark with case-insensitive config can match Age and AGE; otherwise same-name columns align by position/name.
