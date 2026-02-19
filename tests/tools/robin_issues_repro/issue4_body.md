### Summary
PySpark unionByName can coalesce columns with different types (e.g. String and Int64) into a common type. Robin-sparkless fails with `type String is incompatible with expected type Int64`, so unionByName semantics differ from PySpark.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
df2 = spark.createDataFrame([("2", "b")], ["id", "name"])  # id is string
rows = df1.unionByName(df2).collect()
print(rows)
```

**PySpark output:** Succeeds. Typically `[Row(id=1, name='a'), Row(id='2', name='b')]` — PySpark promotes or coerces the `id` column so both rows can be combined.

### robin-sparkless (current behavior)
**Error seen from Sparkless:** `collect failed: type String is incompatible with expected type Int64`

**Request:** Document or implement unionByName behavior when same-named columns have different types (e.g. coercion or clear error message) to match PySpark semantics where possible.
