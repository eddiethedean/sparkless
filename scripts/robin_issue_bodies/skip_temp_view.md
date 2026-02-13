## Summary

PySpark allows `df.createOrReplaceTempView("name")` to register a DataFrame as a temp view, then `spark.table("name")` or `spark.sql("SELECT * FROM name")` to query it. Robin does not expose createOrReplaceTempView (or equivalent) or resolve temp views in sql/table.

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
df = create_df([{"a": 1}], [("a", "int")])
# AttributeError or missing: createOrReplaceTempView / create_or_replace_temp_view
df.createOrReplaceTempView("t")
spark.table("t").collect()
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,)], ["a"])
df.createOrReplaceTempView("t")
spark.table("t").collect()  # [Row(a=1)]
```

## Expected

DataFrame should have `createOrReplaceTempView(name)` (or `create_or_replace_temp_view(name)`). Session's `sql()` and `table()` should resolve temp views. See also issue for [SparkSession.sql/table](skip_sql_table.md).

## Actual

No createOrReplaceTempView on DataFrame; no temp view resolution in session.

## How to reproduce

Requires sql/table (see 19_spark_sql_table.py). After those exist, register a DataFrame and call spark.table("name").

## Context

Sparkless v4 skip list. [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md).
