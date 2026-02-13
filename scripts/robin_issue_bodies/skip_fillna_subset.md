## Summary

PySpark's `DataFrame.fillna(value, subset=[...])` fills nulls in the given columns only. Robin's `fillna()` may not accept a `subset` keyword argument, causing "DataFrame.fillna() got an unexpected keyword argument 'subset'".

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
df = create_df([{"a": 1, "b": None}, {"a": None, "b": 2}], [("a", "int"), ("b", "int")])
# TypeError: fillna() got an unexpected keyword argument 'subset'
out = df.fillna(0, subset=["b"])
out.collect()
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1, None), (None, 2)], ["a", "b"])
df.fillna(0, subset=["b"]).collect()
```

## Expected

`DataFrame.fillna(value, subset=[...])` should accept an optional `subset` list of column names and fill only those columns.

## Actual

TypeError: unexpected keyword argument 'subset'.

## How to reproduce

```bash
python scripts/robin_parity_repros/22_fillna_subset.py
```

## Context

Sparkless v4 skip list; test_fillna_subset skipped. Can be folded into na.fill issue if Robin implements both via a single API. [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md).
