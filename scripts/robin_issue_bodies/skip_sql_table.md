## Summary

PySpark provides `SparkSession.sql(query)` and `SparkSession.table(name)` to run SQL and reference registered tables. Robin does not expose these on the session, so Sparkless (and any PySpark-migrating code) cannot use `spark.sql("SELECT ...")` or `spark.table("t")`.

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
# AttributeError or NotImplementedError: no sql() or table()
df = spark.sql("SELECT 1 as one")
df2 = spark.table("my_table")
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.sql("SELECT 1 as one")
df.collect()  # [Row(one=1)]
# spark.table("name") returns DataFrame for a registered table (e.g. after createOrReplaceTempView)
```

## Expected

`SparkSession.sql(query: str)` and `SparkSession.table(name: str)` should exist and return a DataFrame (or raise a clear error if the table does not exist / SQL is invalid).

## Actual

Robin's SparkSession does not expose `sql()` or `table()`, leading to AttributeError or Sparkless raising NotImplementedError when wrapping.

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/19_spark_sql_table.py
```

## Context

Sparkless v4 skip list: tests that use `spark.sql()` or `spark.table()` are skipped. See [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md).
