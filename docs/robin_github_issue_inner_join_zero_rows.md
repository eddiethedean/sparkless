# [PySpark parity] Inner join returns 0 rows instead of 2

## Summary

When executing a logical plan with an **inner join** via the robin-sparkless crate (e.g. through Sparkless v4), the crate returns **0 rows** instead of the expected 2. Sparkless sends the correct payload (left data, other data, other schema, join keys). PySpark returns 2 rows for the same data and join condition.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.spark_types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

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
print("Robin inner join count:", count_join)
```

**Observed:** `count_join == 0` (expected 2).

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

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
print(joined.collect())
```

**Expected:** `joined.count() == 2` and two rows (Alice/IT/Engineering, Bob/HR/Human Resources).

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.11.4 via PyO3 extension.
- PySpark 3.x.

**Fixed in robin-sparkless 0.11.5.**

## References

- Sparkless doc: `docs/robin_github_issue_join_union_row_counts.md`.
