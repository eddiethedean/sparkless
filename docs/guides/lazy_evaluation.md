# Lazy Evaluation


Enable lazy mode and queue operations until an action is called.

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession()
df = spark.createDataFrame([{ "x": i } for i in range(5)])

lazy_df = df.withLazy(True).filter(F.col("x") > 1).withColumn("y", F.lit(1))
# No execution yet
rows = lazy_df.collect()  # materializes queued operations
```

## Materialization in Set Operations

When using set operations like `unionByName()` with DataFrames that have lazy operations queued, sparkless automatically materializes all pending operations before performing the union. This ensures correct results, especially in diamond dependency scenarios where the same DataFrame is used in multiple branches.

```python
# Example: Diamond dependency pattern
existing = spark.createDataFrame([(1, "a", 100), (2, "b", 200)], ["id", "name", "value"])

# Branch A: Filter and transform
branch_a = existing.filter(F.col("id") == 1).withColumn("source", F.lit("A"))

# Branch B: Different filter and transform (same source DataFrame)
branch_b = existing.filter(F.col("id") == 2).withColumn("source", F.lit("B"))

# unionByName automatically materializes both branches before unioning
# This prevents data duplication and ensures correct results
result = branch_a.unionByName(branch_b)
```

**Important**: When combining DataFrames with `unionByName()` or `union()`, all lazy operations in both DataFrames are automatically materialized to ensure data correctness. This is especially important when the same source DataFrame is used in multiple transformation branches (diamond dependencies).
