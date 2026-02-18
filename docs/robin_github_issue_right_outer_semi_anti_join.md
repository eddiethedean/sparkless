# [PySpark parity] Right, outer, semi, and anti join return wrong row count

## Summary

When executing **right**, **outer**, **left_semi**, or **left_anti** join via the robin-sparkless crate (e.g. through Sparkless v4), the crate returns **0 rows** or incorrect results. PySpark returns correct row counts. Inner/left may work; right/outer/semi/anti do not.

## Robin-sparkless reproduction (Sparkless v4, Robin-only)

```python
from sparkless import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()
emp_df = spark.createDataFrame([
    {"id": 1, "name": "Alice", "dept_id": 10},
    {"id": 2, "name": "Bob", "dept_id": 20},
    {"id": 3, "name": "Charlie", "dept_id": 10},
    {"id": 4, "name": "David", "dept_id": 30},
])
dept_df = spark.createDataFrame([
    {"dept_id": 10, "name": "IT"},
    {"dept_id": 20, "name": "HR"},
    {"dept_id": 40, "name": "Finance"},
])

right_join = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "right")
outer_join = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "outer")
semi_join = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_semi")
anti_join = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_anti")

# Expected: right=3, outer=5, semi=3, anti=1. Robin often returns 0.
print(right_join.count(), outer_join.count(), semi_join.count(), anti_join.count())
```

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()
emp_df = spark.createDataFrame([
    {"id": 1, "name": "Alice", "dept_id": 10},
    {"id": 2, "name": "Bob", "dept_id": 20},
    {"id": 3, "name": "Charlie", "dept_id": 10},
    {"id": 4, "name": "David", "dept_id": 30},
])
dept_df = spark.createDataFrame([
    {"dept_id": 10, "name": "IT"},
    {"dept_id": 20, "name": "HR"},
    {"dept_id": 40, "name": "Finance"},
])

assert emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "right").count() == 3
assert emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "outer").count() == 5
assert emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_semi").count() == 3
assert emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_anti").count() == 1
```

## Environment

- Sparkless v4, robin-sparkless 0.11.7. PySpark 3.x.
