# [PySpark parity] Window expressions: crate expects fn-based format

## Summary

Window functions (e.g. `row_number().over(window)`) executed via the robin-sparkless crate may fail with **type window requires 'fn'** or **expression must have 'col', 'lit', 'op', or 'fn'**. Sparkless sends `{"fn": "<name>", "args": [...], "window": {"partition_by": [...], "order_by": [...]}}`. If the crate expects different keys or shape, window ops fail. PySpark supports these in select/withColumn.

## Robin-sparkless reproduction (Sparkless v4, Robin-only)

```python
from sparkless import SparkSession
from sparkless import functions as F
from sparkless.window import Window

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([
    {"dept": "A", "salary": 100},
    {"dept": "A", "salary": 200},
    {"dept": "B", "salary": 150},
])
w = Window.partitionBy("dept").orderBy("salary")
result = df.withColumn("row_num", F.row_number().over(w)).collect()
```

**Observed:** Error such as `type window requires 'fn'` or expression must have col/lit/op/fn.

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([
    {"dept": "A", "salary": 100},
    {"dept": "A", "salary": 200},
    {"dept": "B", "salary": 150},
])
w = Window.partitionBy("dept").orderBy("salary")
result = df.withColumn("row_num", F.row_number().over(w)).collect()
assert len(result) == 3
assert [r["row_num"] for r in result] in ([1, 2, 1], [1, 1, 2])
```

## Environment

- Sparkless v4, robin-sparkless 0.11.5. PySpark 3.x.
