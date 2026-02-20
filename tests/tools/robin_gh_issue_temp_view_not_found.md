# [PySpark parity] Table or view not found after createOrReplaceTempView

## Summary

After registering a DataFrame as a temp view with **createOrReplaceTempView("view_name")**, calling **spark.table("view_name")** or **spark.sql("SELECT * FROM view_name")** fails with **Table or view not found** (or equivalent). In PySpark, temp views are stored in the session catalog and are immediately queryable via `spark.table()` and `spark.sql()`.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only, using the robin-sparkless crate via PyO3):

```python
from sparkless import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()

df = spark.createDataFrame(
    [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
    ["id", "name"],
)
df.createOrReplaceTempView("my_view")

# Expect to read the view back
result = spark.table("my_view").collect()
# Or: result = spark.sql("SELECT * FROM my_view").collect()
```

**Observed:** `AnalysisException: Table or view not found: my_view` (or equivalent from the crate/session layer). The temp view registered by createOrReplaceTempView is not visible to the catalog used by `table()` or `sql()`.

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()

df = spark.createDataFrame(
    [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
    ["id", "name"],
)
df.createOrReplaceTempView("my_view")

result = spark.table("my_view").collect()
assert len(result) == 2
assert result[0]["id"] == 1 and result[0]["name"] == "a"

# SQL also works
result2 = spark.sql("SELECT * FROM my_view").collect()
assert len(result2) == 2
```

**Expected:** Temp view is registered in the session catalog; `spark.table("my_view")` and `spark.sql("SELECT * FROM my_view")` both return the DataFrame’s data. No error.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.0** via PyO3 extension.
- PySpark 3.x.

## Request

Support session-scoped temp views so that after a DataFrame is registered as a temp view (createOrReplaceTempView), the same session can resolve that view name in `table()` and in SQL (e.g. `SELECT * FROM view_name`), matching PySpark semantics.
