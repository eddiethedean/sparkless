## Summary

PySpark provides `F.soundex(col)` for phonetic encoding of strings. robin-sparkless does not expose a `soundex` function, so string parity tests (e.g. soundex) fail when run with Robin backend (e.g. 0 rows vs expected 3).

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"name": "Alice"}, {"name": "Bob"}, {"name": "Robert"}], [("name", "string")])

# F.soundex not found
df.with_column("snd", F.soundex(F.col("name"))).collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"name": "Alice"}, {"name": "Bob"}, {"name": "Robert"}])
out = df.withColumn("snd", F.soundex(F.col("name"))).collect()  # 3 rows with snd column
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/05_parity_string_soundex.py
```

Robin reports: `F.soundex not found`. PySpark path succeeds.

## Context

- Sparkless parity test: `tests/parity/functions/test_string.py::TestStringFunctionsParity::test_soundex` fails with AssertionError (DataFrames not equivalent / row count mismatch) when backend is Robin because soundex is not available.
