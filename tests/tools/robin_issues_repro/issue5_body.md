### Summary
After a join, collect fails with `not found: ID` (or similar). PySpark resolves column names in join results (including case-insensitive resolution when configured). Robin-sparkless may use different name resolution or casing, causing column not found at collect.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([(1, "x")], ["id", "val"])
df2 = spark.createDataFrame([(1, "y")], ["ID", "other"])  # different case
j = df1.join(df2, df1["id"] == df2["ID"])
print(j.collect())
print(j.columns)
```

**PySpark output:** Collect succeeds. Result has both `id` and `ID` (or resolved names) and rows like `[Row(id=1, val='x', ID=1, other='y')]`. Column names in the result are predictable and accessible.

### robin-sparkless (current behavior)
**Error seen from Sparkless:** `collect failed: not found: ID`

**Request:** Ensure join result schema exposes columns so that names (or case-insensitive resolution) match what PySpark would expose, or document the resolution rules to avoid "not found" at collect.
