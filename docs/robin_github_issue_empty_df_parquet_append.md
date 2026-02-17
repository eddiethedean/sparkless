# [PySpark parity] Empty DataFrame with explicit schema + parquet table append / catalog

## Summary

When using the robin-sparkless engine (via Sparkless v4), creating an **empty DataFrame with an explicit schema** and then writing it as a **parquet-format table** and appending rows fails or raises "can not infer schema from empty dataset". The same flow works in PySpark.

This blocks parity for: (1) empty table creation with schema, (2) parquet table create/append and reading back via `spark.table()` with correct schema and visibility.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only; no backend config):

```bash
cd sparkless  # Sparkless repo root
python scripts/repro_robin_issue_empty_df_parquet.py robin
```

**Code (Robin path — may raise or fail):**

```python
from sparkless import SparkSession
from sparkless.spark_types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("repro").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])

# 1) Empty DataFrame with explicit schema (PySpark allows this)
empty_df = spark.createDataFrame([], schema)

# 2) Create table from empty DF, then append and read back
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
empty_df.write.format("parquet").mode("overwrite").saveAsTable("test_schema.repro_empty_table")
r1 = spark.table("test_schema.repro_empty_table")  # expect count 0
df1 = spark.createDataFrame([{"id": 1, "name": "a"}], schema)
df1.write.format("parquet").mode("append").saveAsTable("test_schema.repro_empty_table")
r2 = spark.table("test_schema.repro_empty_table")  # expect count 1
```

**Observed:** `ValueError: can not infer schema from empty dataset` (or catalog/table read failure) at some step in this flow.

## PySpark reproduction (expected behavior)

```bash
python scripts/repro_robin_issue_empty_df_parquet.py pyspark
```

**Code (PySpark — expected):**

```python
from pyspark.sql import SparkSession as PySparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = PySparkSession.builder.appName("repro").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])

empty_df = spark.createDataFrame([], schema)
spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
empty_df.write.format("parquet").mode("overwrite").saveAsTable("test_schema.repro_empty_table")
r1 = spark.table("test_schema.repro_empty_table")  # count 0
df1 = spark.createDataFrame([{"id": 1, "name": "a"}], schema)
df1.write.format("parquet").mode("append").saveAsTable("test_schema.repro_empty_table")
r2 = spark.table("test_schema.repro_empty_table")  # count 1
```

**Expected:** All steps succeed; `r1.count() == 0`, `r2.count() == 1`.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate (e.g. 0.11.x) via PyO3 extension.
- PySpark: 3.x.

## References

- Sparkless parity doc: `docs/robin_parity_from_skipped_tests.md` (parquet table append).
- Sparkless tests skipped under Robin: `tests/parity/dataframe/test_parquet_format_table_append.py`.
