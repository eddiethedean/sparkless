# [Parity] encode(col, charset) / decode(col, charset) missing

## Summary

PySpark provides `F.encode(col, charset)` and `F.decode(col, charset)` for string encoding/decoding (e.g. UTF-8, US-ASCII). Robin-sparkless does not expose `encode` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
data = [{"s": "hello"}]
df = create_df(data, [("s", "string")])
out = df.select(rs.encode(rs.col("s"), "UTF-8").alias("e"))  # AttributeError: no encode
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("hello",)], ["s"])
out = df.select(F.encode("s", "UTF-8").alias("e"))
out.collect()
```

## Expected

- Module exposes `encode(col, charset)` and `decode(col, charset)`.
- Supported charsets include UTF-8, US-ASCII, ISO-8859-1, etc.

## Actual

- `robin_sparkless` has no `encode` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/40_encode_decode.py`
