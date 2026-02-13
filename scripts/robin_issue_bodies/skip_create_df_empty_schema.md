## Summary

PySpark allows `spark.createDataFrame([], schema)` to create an empty DataFrame with a given schema. Robin's `create_dataframe_from_rows` may raise "schema must not be empty" or reject empty data.

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
# RuntimeError or similar: schema must not be empty / empty data not allowed
df = create_df([], [("a", "int"), ("b", "string")])
df.collect()  # []
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.master("local[1]").getOrCreate()
schema = StructType([
    StructField("a", IntegerType()),
    StructField("b", StringType()),
])
df = spark.createDataFrame([], schema)
df.count()  # 0
```

## Expected

`create_dataframe_from_rows([], schema)` should return an empty DataFrame with the given schema (column names and types), no rows.

## Actual

RuntimeError or ValueError: schema must not be empty, or empty data not allowed.

## How to reproduce

```bash
python scripts/robin_parity_repros/23_create_df_empty_schema.py
```

## Context

Sparkless v4 skip list; test_issue_270_tuple_dataframe and similar skipped. [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md). #256 covers array column type; this is empty data + schema.
