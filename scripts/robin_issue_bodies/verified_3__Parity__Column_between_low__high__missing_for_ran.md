## Summary

PySpark's `Column` has `between(low, high)` for inclusive range filters. robin-sparkless Column does not expose `between`, so filter expressions using between cannot be expressed when using Robin directly.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"v": 10}, {"v": 25}, {"v": 50}], [("v", "int")])

# AttributeError: col.between not found
df.filter(F.col("v").between(F.lit(20), F.lit(30))).collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"v": 10}, {"v": 25}, {"v": 50}])
out = df.filter(F.col("v").between(F.lit(20), F.lit(30))).collect()  # 1 row (v=25)
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/06_filter_between.py
```

Robin reports: `col.between not found`. PySpark path succeeds.

## Context

- Sparkless tests fail with "Operation 'Operations: filter' is not supported" when the filter uses between(); the Sparkless Robin materializer cannot translate it because Robin Column lacks this method.
- Representative Sparkless test: `tests/test_issue_261_between.py`
