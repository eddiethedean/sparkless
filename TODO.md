# Sparkless Backend Limitations

This document tracks PySpark operations that are not supported by the sparkless (Polars) backend used in unit tests. Tests requiring these operations must be marked with `@pytest.mark.requires_real_spark`.

## Unsupported Operations

### 1. Alias-Based Column References in Joins

**Pattern**: `F.col("alias.column")` in join conditions

```python
result_alias = result.alias("r")
stock_alias = stock_df.alias("stk")
result_alias.join(
    stock_alias,
    F.col("r.variant_id") == F.col("stk.variant_id"),  # NOT SUPPORTED
    "left",
)
```

**Error**: `ValueError: Join column 'r.variant_id' not found in left DataFrame`

**Workaround**: Pre-select and rename columns to avoid ambiguity, then join on matching column names.

### 2. Column-Based Drop

**Pattern**: `.drop(F.col("alias.column"))`

```python
result.drop(F.col("stk.variant_id"))  # NOT SUPPORTED
```

**Error**: `TypeError: 'Column' object is not iterable`

**Workaround**: Use string-based drop: `.drop("column_name")`.

### 3. String-Based Cast on Aggregations

**Pattern**: `F.sum("col").cast("long")` or `F.lit(None).cast("long")`

```python
F.sum("stock_qty").cast("long").alias("stock_qty")  # NOT SUPPORTED
F.lit(None).cast("long")  # NOT SUPPORTED
F.lit(None).cast("double")  # NOT SUPPORTED
```

**Error**: `ValueError: Unsupported Sparkless type: <class 'str'>`

**Workaround**: Use PySpark type objects: `.cast(LongType())` or `.cast(DoubleType())`. Alternatively, alias first then cast in a separate `.select()`.

### 4. Multi-Condition Join with & Operator

**Pattern**: Compound join conditions using `&`

```python
result.join(
    other,
    (result["col1"] == other["col1"]) & (result["col2"] == other["col2"]),  # NOT SUPPORTED
    "left",
)
```

**Error**: `ValueError: Join keys must be column name(s) or a ColumnOperation`

**Workaround**: Join on a list of matching column names: `.join(other, ["col1", "col2"], "left")`.

### 5. Column Object as withColumn Name

**Pattern**: Using a `Column` schema constant as the first arg to `withColumn`

```python
from pipelines.shared.schemas import WithOrgSchema as BS
result.withColumn(BS.ORGANIZATION_ID, F.lit("org-1"))  # NOT SUPPORTED if Column object
```

**Error**: `TypeError: 'Column' object is not iterable`

**Workaround**: Use the string value: `.withColumn("organization_id", F.lit("org-1"))`.

### 6. Row Constructor with None Values

**Pattern**: `spark.createDataFrame([Row(field=None)])` — sparkless cannot infer the type of a column when all values are `None`.

```python
from pyspark.sql import Row

# NOT SUPPORTED — sparkless cannot infer type from None
df = spark.createDataFrame([
    Row(id="1", image_url=None, is_active=True),
])
```

**Error**: `ValueError: Some of types cannot be determined after inferring`

**Workaround**: Use `createDataFrame` with an explicit `StructType` schema:

```python
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

schema = StructType([
    StructField("id", StringType()),
    StructField("image_url", StringType()),
    StructField("is_active", BooleanType()),
])
df = spark.createDataFrame(
    [{"id": "1", "image_url": None, "is_active": True}],
    schema=schema,
)
```

**Note**: This also applies to `MockDataFrame` — accessing `.columns` on a `MockDataFrame` raises `AttributeError`. Production code that checks `column_name in df.columns` should handle this gracefully with a `try/except`.

## Affected Test Files

| Test File | Reason |
|-----------|--------|
| `tests/layers/gold/shared/transformations/dimensions/test_shop_analytics.py` | Alias joins, Column drops, string casts |
| `tests/integration/storage/test_shop_analytics_delta.py` | Delta write/read (requires_real_spark by directory) |

## How to Run

```bash
# Default: sparkless only (skips requires_real_spark tests)
just unit-tests

# With real Spark (runs all tests including requires_real_spark)
USE_REAL_SPARK=1 just integration-tests
```
