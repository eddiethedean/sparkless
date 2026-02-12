## Summary

With the Robin backend, many expressions in select/withColumn fail because `_expression_to_robin` does not handle: `lit(None)`, `to_date`/`to_timestamp`, `regexp_replace`, `format_string`, `split` with limit, `log`/`log10`, `round(column, scale)`, `array(...)`, `explode(...)`. Adding support (where Robin’s API exists) would fix these.

## Failure (current behavior)

```python
from sparkless.sql import SparkSession
from sparkless.functions import F

spark = SparkSession.builder().master("local").get_or_create()
df = spark.createDataFrame([(1, "2024-01-15"), (2, "2024-02-20")], ["id", "date_str"])

# FAILS: not found: Column 'to_date(date_str)' or similar / withColumn unsupported
result = df.withColumn("dt", F.to_date(F.col("date_str")))
result.collect()
```

```python
# lit(None): FAILS or wrong behavior
result = df.withColumn("nullable_col", F.lit(None))
```

```python
# regexp_replace: FAILS
df = spark.createDataFrame([("a.b.c",)], ["text"])
result = df.withColumn("cleaned", F.regexp_replace(F.col("text"), r"\.\d+", ""))
```

```python
# split with limit: FAILS - not found: Column 'split(Value, ,, -1)'
df = spark.createDataFrame([("a,b,c",)], ["value"])
result = df.withColumn("arr", F.split(F.col("value"), ",", -1))
```

```python
# round: FAILS - not found: Column 'round(val, 0)'
df = spark.createDataFrame([(3.14159,)], ["val"])
result = df.withColumn("r", F.round(F.col("val"), 0))
```

```python
# array: FAILS - not found: Column 'array(a, b, c)'
df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
result = df.withColumn("arr", F.array(F.col("a"), F.col("b"), F.col("c")))
```

## Expected behavior

- `F.to_date(col)` / `F.to_timestamp(col)` (and with format where supported) should translate to Robin’s equivalent and produce a date/timestamp column.
- `F.lit(None)` should produce a null literal column (Robin `F.lit(None)` or equivalent).
- `F.regexp_replace(col, pattern, replacement)` should translate if Robin supports it.
- `F.split(col, delim, limit)` should translate when Robin supports split with limit.
- `F.round(col, scale)` should translate if Robin has round.
- `F.array(*cols)` should translate to Robin’s array constructor if available.
- `F.explode(col)` should translate if Robin has explode.

## Fix (Sparkless-side)

In `sparkless/backend/robin/materializer.py`, in `_expression_to_robin`:
- **lit(None):** For `Literal` with value `None`, return `F.lit(None)` (ensure `_lit_value_for_robin(None)` is acceptable to Robin).
- **to_date / to_timestamp:** Add branches for op `to_date` and `to_timestamp` (unary or with format); map to Robin API if available.
- **regexp_replace:** Add op (column, pattern, replacement) and optionally position; map to Robin if supported.
- **format_string:** Add if Robin has equivalent; otherwise document and return None.
- **split with limit:** Extend split handling for third argument (limit).
- **log / round:** Add unary or binary log (e.g. log10) and `round(column, scale)` if Robin exposes them.
- **array:** Add op `array` (value = list of columns) → `F.array(*robin_cols)` if Robin has it.
- **explode:** Add op `explode` (column) → `F.explode(inner)` if Robin has it.

Implement only where Robin’s API is confirmed (check robin-sparkless). For unsupported functions, document and return None.

Ref: `_expression_to_robin` in sparkless/backend/robin/materializer.py.
