# [PySpark parity] Table or view not found for schema-qualified name

## Summary

When using **spark.table("schema_name.table_name")** (schema-qualified table name), the robin-sparkless crate fails with **Table or view 'schema_name.table_name' not found**. Creating the table with `df.write.saveAsTable("schema_name.table_name")` and then reading with `spark.table("schema_name.table_name")` does not succeed. This affects tests such as `test_parquet_format_append_detached_df_visible_to_active_session`, `test_parquet_format_append_detached_df_visible_to_multiple_sessions`.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only, with SQL feature):

```python
from sparkless import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([{"id": 1, "name": "a"}])
# Create schema and table
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
df.write.mode("overwrite").saveAsTable("test_schema.test_table")
# Read back with schema-qualified name
tbl = spark.table("test_schema.test_table")
tbl.collect()
```

**Observed:** `ValueError: table(test_schema.test_table) failed: Table or view 'test_schema.test_table' not found` (or similar).

## PySpark (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([{"id": 1, "name": "a"}])
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
df.write.mode("overwrite").saveAsTable("test_schema.test_table")
tbl = spark.table("test_schema.test_table")
tbl.collect()  # OK
```

**Expected:** schema-qualified table names are resolved in the catalog and table() returns the DataFrame.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate **0.12.2** (with `sql` feature) via PyO3 extension.
- PySpark 3.x.

## Request

Support schema-qualified table names in catalog/table() so that `spark.table("schema.table")` works after saveAsTable.
