# [PySpark parity] row_number() over (partition by col) window

## Summary

For PySpark parity, the robin-sparkless engine should support **window functions** such as **row_number()** with **partition_by** (and optionally order_by). When run via Sparkless v4 (Robin), `row_number().over(Window.partitionBy("col"))` may fail with a plan error or produce incorrect partition ordering.

## Robin-sparkless reproduction

```bash
cd sparkless
python scripts/repro_robin_issue_window_row_number.py robin
```

**Code (Robin path):**

```python
from sparkless import SparkSession
from sparkless import functions as F
from sparkless.window import Window

spark = SparkSession.builder.appName("repro").getOrCreate()
data = [
    {"dept": "A", "salary": 10},
    {"dept": "A", "salary": 20},
    {"dept": "B", "salary": 30},
]
df = spark.createDataFrame(data)
win = Window.partitionBy("dept")
result = df.withColumn("rn", F.row_number().over(win)).select("dept", "salary", "rn")
rows = result.collect()
# Expected: dept A has rn 1, 2; dept B has rn 1
```

**Observed:** Plan error (e.g. "select payload must be array of column names or {name, expr} objects" or "withColumn must have 'expr'") or wrong rn values per partition.

## PySpark reproduction (expected behavior)

```bash
python scripts/repro_robin_issue_window_row_number.py pyspark
```

**Code (PySpark):**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("repro").getOrCreate()
data = [
    {"dept": "A", "salary": 10},
    {"dept": "A", "salary": 20},
    {"dept": "B", "salary": 30},
]
df = spark.createDataFrame(data)
win = Window.partitionBy("dept")
result = df.withColumn("rn", F.row_number().over(win)).select("dept", "salary", "rn")
rows = result.collect()
# dept A: rn 1, 2; dept B: rn 1
```

**Expected:** Three rows; partition "A" has `rn` 1 and 2; partition "B" has `rn` 1.

## References

- Sparkless: `docs/robin_parity_from_skipped_tests.md` (window row_number).
- Sparkless test (skipped): `tests/unit/dataframe/test_logical_plan.py` — `test_plan_interpreter_window_row_number`.
