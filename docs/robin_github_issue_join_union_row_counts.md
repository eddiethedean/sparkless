# [PySpark parity] Join and union return wrong row counts

## Summary

When executing logical plans that include **join** or **union** via the robin-sparkless crate (e.g. through Sparkless v4), the crate returns incorrect row counts. Sparkless sends the correct payload (left data plus plan with `other_data`, `other_schema`, and join keys or union payload). Observed: **inner join** returns 0 rows instead of 2; **union** / **unionByName** return only the left side rows (e.g. 2) instead of left + right (e.g. 4). PySpark returns the correct counts.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.spark_types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

# --- Union: expect 4 rows (2 + 2), Robin returns 2
schema = StructType([
    StructField("Name", StringType()),
    StructField("Value", IntegerType()),
])
df1 = spark.createDataFrame([("Alice", 1), ("Bob", 2)], schema=schema)
df2 = spark.createDataFrame([("Charlie", 3), ("Diana", 4)], schema=schema)
unioned = df1.unionByName(df2)
count_union = unioned.count()
# Expected: 4
# Observed: 2 (only left side)

# --- Join: expect 2 rows, Robin returns 0
emp_schema = StructType([
    StructField("Name", StringType()),
    StructField("Id", IntegerType()),
    StructField("Dept", StringType()),
])
dept_schema = StructType([
    StructField("Dept", StringType()),
    StructField("Name", StringType()),
])
df_emp = spark.createDataFrame([("Alice", 1, "IT"), ("Bob", 2, "HR")], schema=emp_schema)
df_dept = spark.createDataFrame([("IT", "Engineering"), ("HR", "Human Resources")], schema=dept_schema)
joined = df_emp.join(df_dept, "Dept", "inner")
count_join = joined.count()
# Expected: 2
# Observed: 0
```

**Observed:** `count_union == 2` (expected 4), `count_join == 0` (expected 2).

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

schema = StructType([
    StructField("Name", StringType()),
    StructField("Value", IntegerType()),
])
df1 = spark.createDataFrame([("Alice", 1), ("Bob", 2)], schema=schema)
df2 = spark.createDataFrame([("Charlie", 3), ("Diana", 4)], schema=schema)
unioned = df1.unionByName(df2)
assert unioned.count() == 4, "Union should have 4 rows"

emp_schema = StructType([
    StructField("Name", StringType()),
    StructField("Id", IntegerType()),
    StructField("Dept", StringType()),
])
dept_schema = StructType([
    StructField("Dept", StringType()),
    StructField("Name", StringType()),
])
df_emp = spark.createDataFrame([("Alice", 1, "IT"), ("Bob", 2, "HR")], schema=emp_schema)
df_dept = spark.createDataFrame([("IT", "Engineering"), ("HR", "Human Resources")], schema=dept_schema)
joined = df_emp.join(df_dept, "Dept", "inner")
assert joined.count() == 2, "Inner join on Dept should have 2 rows"
```

**Expected:** Union returns 4 rows; inner join returns 2 rows.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.11.4 via PyO3 extension.
- PySpark 3.x.

## References

- Sparkless doc: `docs/robin_unsupported_ops.md` (Join and union row counts).
