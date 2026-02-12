## Summary

PySpark supports `df.orderBy(F.col("value").desc_nulls_last())`. Robin's `Column.desc_nulls_last()` exists (in 0.8+), but `df.order_by(col.desc_nulls_last())` fails with `TypeError: argument 'cols': 'PySortOrder' object cannot be converted to 'Sequence'`. So the order_by API does not accept the result of desc_nulls_last() (PySortOrder) for PySpark parity.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
data = [{"value": "A"}, {"value": "B"}, {"value": None}, {"value": "C"}, {"value": "D"}]
df = create_df(data, [("value", "string")])

# TypeError: argument 'cols': 'PySortOrder' object cannot be converted to 'Sequence'
df.order_by(F.col("value").desc_nulls_last()).collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame(
    [{"value": "A"}, {"value": "B"}, {"value": None}, {"value": "C"}, {"value": "D"}]
)
out = df.orderBy(F.col("value").desc_nulls_last()).collect()  # 5 rows, nulls last
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/repro_robin_limitations/11_order_by_nulls.py
```

Robin fails with `TypeError: argument 'cols': 'PySortOrder' object cannot be converted to 'Sequence'`. PySpark succeeds.

## Context

- Related to #245 (Column.desc_nulls_last / nulls ordering methods). Here the method exists but order_by does not accept the resulting sort order object.
- Robin version: 0.8.0+ (sparkless pyproject.toml).
