# [PySpark parity] groupBy + agg (sum, count) in logical plan

## Summary

For PySpark parity, the robin-sparkless engine should support **groupBy** with multiple aggregations (e.g. **sum**, **count**) and produce the same results as PySpark. When run via Sparkless v4 (Robin), this flow may fail with a plan/execution error or produce incorrect results.

## Robin-sparkless reproduction

```bash
cd sparkless
python scripts/repro_robin_issue_groupby_agg.py robin
```

**Code (Robin path):**

```python
from sparkless import SparkSession
from sparkless import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
data = [{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "b", "v": 30}]
df = spark.createDataFrame(data)
result = df.groupBy("k").agg(
    F.sum("v").alias("sum(v)"),
    F.count("v").alias("count(v)"),
)
rows = result.collect()
# Expected: k="a" -> sum(v)=30, count(v)=2; k="b" -> sum(v)=30, count(v)=1
```

**Observed:** Plan/execution error (e.g. "invalid plan: groupBy ...") or wrong row/counts.

## PySpark reproduction (expected behavior)

```bash
python scripts/repro_robin_issue_groupby_agg.py pyspark
```

**Code (PySpark):**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
data = [{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "b", "v": 30}]
df = spark.createDataFrame(data)
result = df.groupBy("k").agg(
    F.sum("v").alias("sum(v)"),
    F.count("v").alias("count(v)"),
)
rows = result.collect()
# k="a": sum(v)=30, count(v)=2; k="b": sum(v)=30, count(v)=1
```

**Expected:** Two rows; k="a" has sum(v)=30, count(v)=2; k="b" has sum(v)=30, count(v)=1.

## References

- Sparkless: docs/robin_parity_from_skipped_tests.md (groupBy + agg).
- Sparkless test (skipped): tests/unit/dataframe/test_logical_plan.py - test_groupBy_via_plan_interpreter.
