# [PySpark parity] between, power (**), and cast in logical plan

## Summary

For PySpark parity, the robin-sparkless engine should support in the logical plan (or equivalent execution): **(1) between(lower, upper)** (inclusive), **(2) power (**)** , and **(3) cast to string**. When run via Sparkless v4 (Robin), a pipeline of filter(between) + withColumn(power) + withColumn(cast) may fail or produce wrong results.

## Robin-sparkless reproduction

```bash
cd sparkless
python scripts/repro_robin_issue_between_power_cast.py robin
```

**Code (Robin path):**

```python
from sparkless import SparkSession
from sparkless import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
data = [{"a": 2, "b": 10}, {"a": 5, "b": 20}, {"a": 8, "b": 30}]
df = spark.createDataFrame(data)
result = (
    df.filter(F.col("a").between(3, 7))
    .withColumn("squared", F.col("a") ** 2)
    .withColumn("a_str", F.col("a").cast("string"))
)
rows = result.collect()
# Expected: 1 row; a=5, squared=25, a_str="5"
```

**Observed:** Plan/expression error (e.g. "expression must have 'col', 'lit', 'op', or 'fn'" or "withColumn must have 'expr'") or wrong values.

## PySpark reproduction (expected behavior)

```bash
python scripts/repro_robin_issue_between_power_cast.py pyspark
```

**Code (PySpark):**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
data = [{"a": 2, "b": 10}, {"a": 5, "b": 20}, {"a": 8, "b": 30}]
df = spark.createDataFrame(data)
result = (
    df.filter(F.col("a").between(3, 7))
    .withColumn("squared", F.col("a") ** 2)
    .withColumn("a_str", F.col("a").cast("string"))
)
rows = result.collect()
# 1 row: a=5, squared=25, a_str="5"
```

**Expected:** Single row with `a=5`, `squared=25`, `a_str="5"`.

## References

- Sparkless: `docs/robin_parity_from_skipped_tests.md` (between, power, cast).
- Sparkless test (skipped): `tests/unit/dataframe/test_logical_plan.py` — `test_plan_interpreter_cast_between_power`.
