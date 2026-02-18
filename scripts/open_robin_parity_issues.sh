#!/bin/bash
# Open GitHub issues on eddiethedean/robin-sparkless for PySpark parity gaps.
# Run from repo root: bash scripts/open_robin_parity_issues.sh
# Requires: gh auth, repo access to eddiethedean/robin-sparkless

set -e
REPO="eddiethedean/robin-sparkless"

# Issue 1: orderBy nulls_first / nulls_last
gh issue create -R "$REPO" --title "[PySpark parity] orderBy does not support nulls_first / nulls_last" --body '## Summary
In PySpark, `orderBy(F.col("x").desc_nulls_last())` places nulls last; `asc_nulls_first()` places nulls first. Robin-sparkless does not support configurable null placement in sort; null ordering differs from PySpark.

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()
schema = StructType([StructField("value", StringType(), True)])
df = spark.createDataFrame([{"value": "A"}, {"value": "B"}, {"value": None}, {"value": "C"}, {"value": "D"}], schema=schema)
result = df.orderBy(F.col("value").desc_nulls_last()).collect()
# Expected: D, C, B, A, None (nulls last)
assert result[4]["value"] is None
```

## Robin-sparkless (Sparkless v4)
```python
from sparkless import SparkSession
from sparkless import functions as F
from sparkless.spark_types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()
schema = StructType([StructField("value", StringType(), True)])
df = spark.createDataFrame([{"value": "A"}, {"value": "B"}, {"value": None}, {"value": "C"}, {"value": "D"}], schema=schema)
result = df.orderBy(F.col("value").desc_nulls_last()).collect()
# Actual: null ordering may differ (e.g. nulls first or wrong order)
```

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 2: withField
gh issue create -R "$REPO" --title "[PySpark parity] withField struct update not supported" --body '## Summary
PySpark supports `F.col("struct_col").withField("new_field", value)` to add or replace a field in a struct. Robin-sparkless does not support this expression; plans fail or return incorrect results.

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("my_struct", StructType([StructField("value_1", IntegerType()), StructField("value_2", StringType())])),
])
df = spark.createDataFrame([{"id": "1", "my_struct": {"value_1": 10, "value_2": "x"}}], schema=schema)
result = df.withColumn("my_struct", F.col("my_struct").withField("value_1", F.lit(99))).collect()
assert result[0]["my_struct"]["value_1"] == 99
```

## Robin-sparkless (Sparkless v4)
Same code via Sparkless v4 (Robin execution) fails or returns wrong struct.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 3: create_map
gh issue create -R "$REPO" --title "[PySpark parity] create_map semantics / support" --body '## Summary
PySpark supports `F.create_map(key1, val1, key2, val2, ...)` to build a MapType column. Robin-sparkless may not support it or return different structure/ordering.

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([{"val1": "a", "val2": 1}])
row = df.select(F.create_map(F.lit("key1"), F.col("val1"), F.lit("key2"), F.col("val2")).alias("map_col")).collect()[0]
assert row["map_col"] == {"key1": "a", "key2": 1}
```

## Robin-sparkless (Sparkless v4)
Same code via Sparkless v4 (Robin execution): fails with unsupported expression op create_map, or returns different map shape/values.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 4: CSV inferSchema
gh issue create -R "$REPO" --title "[PySpark parity] CSV inferSchema behavior differs from PySpark" --body '## Summary
When reading CSV with `inferSchema=True`, PySpark infers column types (int, double, boolean, etc.). Robin-sparkless CSV path may keep all columns as string or infer differently.

## PySpark (expected)
```python
from pyspark.sql import SparkSession
import tempfile, os
spark = SparkSession.builder.getOrCreate()
with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
    f.write("name,age,salary,active\nAlice,25,50000.5,true\nBob,30,60000,false\n")
    path = f.name
df = spark.read.option("header", True).option("inferSchema", True).csv(path)
# age should be int/long, salary double, active boolean
os.unlink(path)
```

## Robin-sparkless (Sparkless v4)
Same flow via Sparkless v4: schema may be all string or types mismatch.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 5: cast/astype
gh issue create -R "$REPO" --title "[PySpark parity] cast/astype semantics differ from PySpark" --body '## Summary
Column.cast() and Column.astype() in PySpark convert types with well-defined semantics. Robin-sparkless may return different types (e.g. int vs long) or fail for some casts.

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([{"x": 1}, {"x": 2}])
r = df.select(F.col("x").cast("string").alias("s")).collect()[0]
assert r["s"] == "1"
```

## Robin-sparkless (Sparkless v4)
Same code: cast may fail or return different type/value.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 6: UDF
gh issue create -R "$REPO" --title "[PySpark parity] UDF expression not supported" --body '## Summary
PySpark supports UDFs via `F.udf(lambda x: ..., returnType)`. Robin-sparkless reports "unsupported expression op: udf" when the plan is executed.

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([{"text": "hello"}, {"text": "world"}])
udf_upper = F.udf(lambda x: x.upper(), StringType())
result = df.withColumn("upper_text", udf_upper(F.col("text"))).collect()
assert result[0]["upper_text"] == "HELLO"
```

## Robin-sparkless (Sparkless v4)
Same code: ValueError: Robin execute_plan failed: expression: unsupported expression op: udf

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 7: SQL aggregation alias
gh issue create -R "$REPO" --title "[PySpark parity] SQL does not support alias in aggregation SELECT (e.g. COUNT(v) AS cnt)" --body '## Summary
In PySpark, `spark.sql("SELECT grp, COUNT(v) AS cnt FROM t GROUP BY grp")` returns a column named "cnt". Robin-sparkless SQL fails with "unsupported SELECT item in aggregation: ExprWithAlias { expr: Function(COUNT...), alias: cnt }".

## PySpark (expected)
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([{"grp": 1, "v": 10}, {"grp": 1, "v": 20}, {"grp": 2, "v": 30}])
df.createOrReplaceTempView("t")
rows = spark.sql("SELECT grp, COUNT(v) AS cnt FROM t GROUP BY grp ORDER BY grp").collect()
assert (rows[0]["grp"], rows[0]["cnt"]) == (1, 2)
```

## Robin-sparkless (Sparkless v4)
Same code: Robin SQL failed: unsupported SELECT item in aggregation (alias).

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 8: String functions (translate, levenshtein, soundex, etc.)
gh issue create -R "$REPO" --title "[PySpark parity] String functions not supported: translate, substring_index, levenshtein, soundex, crc32, xxhash64, get_json_object, json_tuple, regexp_extract_all" --body '## Summary
PySpark supports many string/JSON functions. Robin-sparkless reports "unsupported expression op: <name>" for: translate, substring_index, levenshtein, soundex, crc32, xxhash64, get_json_object, json_tuple, regexp_extract_all.

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([{"s": "hello world"}])
df.select(F.regexp_extract_all(F.col("s"), r"\w+", 0).alias("m")).collect()  # ["hello", "world"]
df.select(F.levenshtein(F.lit("kitten"), F.lit("sitting"))).collect()       # 3
```

## Robin-sparkless (Sparkless v4)
Same expressions: unsupported expression op: regexp_extract_all, levenshtein, etc.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 9: date_trunc / to_date
gh issue create -R "$REPO" --title "[PySpark parity] date_trunc and to_date expressions not supported" --body '## Summary
PySpark supports F.date_trunc(unit, col) and F.to_date(col, format). Robin-sparkless reports "unsupported expression op: date_trunc" and "unsupported expression op: to_date".

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([{"d": "2024-03-15"}])
df.select(F.to_date(F.col("d")).alias("dt")).collect()
df.select(F.date_trunc("month", F.to_date(F.col("d"))).alias("month")).collect()
```

## Robin-sparkless (Sparkless v4)
Same code: unsupported expression op: to_date / date_trunc.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 10: format_string, log
gh issue create -R "$REPO" --title "[PySpark parity] format_string and log() expressions not supported" --body '## Summary
PySpark supports F.format_string() and F.log(). Robin-sparkless reports "unsupported expression op: format_string" and "unsupported expression op: log".

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([{"Name": "Alice", "IntegerValue": 123}])
df.select(F.format_string("%s: %d", F.col("Name"), F.col("IntegerValue"))).collect()
df.select(F.log(F.lit(2.0), F.lit(8.0))).collect()  # log base 2 of 8 = 3
```

## Robin-sparkless (Sparkless v4)
Same code: unsupported expression op: format_string / log.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 11: approx_count_distinct window
gh issue create -R "$REPO" --title "[PySpark parity] Window function approx_count_distinct not supported" --body '## Summary
PySpark supports approx_count_distinct in window: F.approx_count_distinct("col").over(window). Robin-sparkless reports "unsupported window fn approx_count_distinct (supported: row_number, rank, dense_rank, percent_rank, ntile, lag, lead, sum, avg)".

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([{"type": "A", "value": 1}, {"type": "A", "value": 10}, {"type": "B", "value": 5}])
w = Window.partitionBy("type")
df.withColumn("approx_distinct", F.approx_count_distinct("value").over(w)).collect()
```

## Robin-sparkless (Sparkless v4)
Same code: unsupported window fn approx_count_distinct.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 12: Union type coercion
gh issue create -R "$REPO" --title "[PySpark parity] Union type coercion differs (e.g. Int64 vs String)" --body '## Summary
PySpark allows union of DataFrames with compatible types (e.g. string with int64 coerces). Robin-sparkless may fail with "type Int64 is incompatible with expected type String" or similar.

## PySpark (expected)
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([{"x": "a"}])
df2 = spark.createDataFrame([{"x": 1}])  # int
union_df = df1.union(df2)  # PySpark coerces or allows
union_df.collect()
```

## Robin-sparkless (Sparkless v4)
collect_as_json_rows failed: type Int64 is incompatible with expected type String.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 13: Join parity (inner/left return 0 rows)
gh issue create -R "$REPO" --title "[PySpark parity] Inner and left join return 0 rows (join parity)" --body '## Summary
When executing inner or left join via Sparkless v4 (Robin), result row count is 0 instead of expected (e.g. inner=3, left=4). Right/outer/semi/anti are already known issues; inner and left should return correct counts.

## PySpark (expected)
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp = spark.createDataFrame([{"id": 1, "name": "Alice", "dept_id": 10}, {"id": 2, "name": "Bob", "dept_id": 20}, {"id": 3, "name": "Charlie", "dept_id": 10}, {"id": 4, "name": "David", "dept_id": 30}])
dept = spark.createDataFrame([{"dept_id": 10, "name": "IT"}, {"dept_id": 20, "name": "HR"}, {"dept_id": 40, "name": "Finance"}])
assert emp.join(dept, emp.dept_id == dept.dept_id, "inner").count() == 3
assert emp.join(dept, emp.dept_id == dept.dept_id, "left").count() == 4
```

## Robin-sparkless (Sparkless v4)
Same code: inner and left join return 0 rows.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 14: Empty schema / table operations
gh issue create -R "$REPO" --title "[PySpark parity] Empty DataFrame schema and table operations (can not infer schema from empty dataset)" --body '## Summary
Operations that involve empty DataFrames or table/schema creation (CREATE TABLE, saveAsTable, spark.table(), DESCRIBE, etc.) raise "can not infer schema from empty dataset" or fail in Robin path. PySpark handles empty DF with explicit schema and table ops.

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
spark = SparkSession.builder.getOrCreate()
schema = StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True)])
empty = spark.createDataFrame([], schema)
empty.write.mode("overwrite").saveAsTable("my_table")
spark.table("my_table").count()  # 0
```

## Robin-sparkless (Sparkless v4)
ValueError: can not infer schema from empty dataset at some step.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

# Issue 15: Array / explode
gh issue create -R "$REPO" --title "[PySpark parity] Array column and explode semantics differ" --body '## Summary
Robin-sparkless may report "array column value must be null or array" when processing array columns, or "unsupported expression op: explode". PySpark supports array types and F.explode().

## PySpark (expected)
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([{"arr": [1, 2, 3]}, {"arr": [10, 20]}])
df.select(F.explode(F.col("arr")).alias("x")).collect()
df.select(F.col("arr").getItem(1)).collect()
```

## Robin-sparkless (Sparkless v4)
session/df: array column value must be null or array; or unsupported expression op: explode.

**Environment:** Sparkless v4, robin-sparkless 0.11.7'

echo "Done. Created 15 parity issues on $REPO"
