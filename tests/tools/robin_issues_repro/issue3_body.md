### Summary
PySpark allows comparisons between string and numeric columns (e.g. string column eq int literal) and coerces types. Robin-sparkless fails at collect with an error like `cannot compare string with numeric type (i64)` or `filter predicate was not of type boolean`, so filter semantics differ from PySpark.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("123",), ("456",)], ["s"])
# String column compared to int: PySpark coerces and evaluates
rows = df.filter(df["s"] == 123).collect()
print(rows)
```

**PySpark output:** `[Row(s='123')]` — PySpark coerces the string column to numeric for comparison and returns the matching row.

### robin-sparkless (current behavior)
Filter/collect fails with:
- `collect failed: cannot compare string with numeric type (i64)`
- `collect failed: filter predicate was not of type boolean`

**Request:** Document or implement PySpark-like coercion (or explicit cast requirements) for filter predicates so that string/numeric comparisons behave like PySpark.
