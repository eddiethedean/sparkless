## Summary

PySpark's `DataFrame.unionByName(other, allowMissingColumns=True)` unions two DataFrames by column name and fills missing columns with null when one side does not have the column. Robin's `union_by_name` may not accept `allow_missing_columns`, causing TypeError.

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
df1 = create_df([{"a": 1, "b": 2}], [("a", "int"), ("b", "int")])
df2 = create_df([{"a": 3, "c": 4}], [("a", "int"), ("c", "int")])
# TypeError: union_by_name() got an unexpected keyword argument 'allow_missing_columns'
out = df1.union_by_name(df2, allow_missing_columns=True)
out.collect()
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df1 = spark.createDataFrame([(1, 2)], ["a", "b"])
df2 = spark.createDataFrame([(3, 4)], ["a", "c"])
df1.unionByName(df2, allowMissingColumns=True).collect()
# Result has columns a, b, c; b/c filled with null where missing
```

## Expected

`union_by_name(other, allow_missing_columns=True)` (or equivalent kwarg) should be supported; when True, columns missing on one side are filled with null.

## Actual

TypeError: unexpected keyword argument 'allow_missing_columns'.

## How to reproduce

```bash
python scripts/robin_parity_repros/24_union_by_name_allow_missing.py
```

## Context

Sparkless v4 skip list; case-insensitive unionByName and related tests. [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md).
