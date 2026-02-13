## Summary

PySpark's `Window.partitionBy("col1", "col2")` and `Window.orderBy("col")` accept column names (strings). Robin's Window API may require Column objects, leading to "descriptor orderBy for 'builtins.Window' objects doesn't apply to a 'str' object" when tests pass strings.

## Robin reproduction

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
    spark, "_create_dataframe_from_rows"
)
df = create_df(
    [{"dept": "A", "salary": 100}, {"dept": "A", "salary": 200}],
    [("dept", "string"), ("salary", "int")],
)
# TypeError if Window expects Column not str
w = rs.Window.partition_by("dept").order_by("salary")
# or: w = rs.Window.partitionBy("dept").orderBy("salary")
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, Window

spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("A", 100), ("A", 200)], ["dept", "salary"])
w = Window.partitionBy("dept").orderBy("salary")  # strings accepted
```

## Expected

`Window.partitionBy(*cols)` and `Window.orderBy(*cols)` should accept column names (str) as well as Column expressions, for PySpark compatibility.

## Actual

TypeError: descriptor orderBy for Window doesn't apply to a 'str' object (or similar).

## How to reproduce

```bash
python scripts/robin_parity_repros/20_window_accept_str.py
```

## Context

Sparkless v4 skip list; test_window_arithmetic and related tests skipped. [docs/v4_robin_skip_list_to_issues.md](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_robin_skip_list_to_issues.md). Related: #187 (Window API).
