# GitHub issue bodies for robin-sparkless (eddiethedean/robin-sparkless)

## Issue 1: create_dataframe_from_rows – struct value must be object or array (PySpark parity)

**Title:** create_dataframe_from_rows: accept struct values in PySpark-equivalent formats (object or positional array)

**Body:**

### Summary
When creating a DataFrame from rows that include struct columns, robin-sparkless fails with `struct value must be object or array` when the struct is passed in a format that PySpark accepts (e.g. tuple/list as positional struct fields). PySpark accepts both dict (by field name) and tuple/list (by position) for struct columns.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("nested", StructType([
        StructField("a", IntegerType()),
        StructField("b", StringType()),
    ])),
])

# PySpark accepts dict for struct
df1 = spark.createDataFrame([("x", {"a": 1, "b": "y"})], schema)
df1.collect()  # OK

# PySpark also accepts tuple/list for struct (positional)
df2 = spark.createDataFrame([("x", (1, "y"))], schema)
df2.collect()  # OK
```

### robin-sparkless (current behavior)

In `session.rs`, struct values are required to be JSON object or array:

```rust
// src/session.rs (robin-sparkless) – current check
} else if let Some(obj) = v.as_ref().and_then(|x| x.as_object()) {
    // ... by field name
} else if let Some(arr) = v.as_ref().and_then(|x| x.as_array()) {
    // ... by position
} else {
    return Err(PolarsError::ComputeError(
        "struct value must be object or array".into(),
    ));
}
```

When the Sparkless Python layer sends rows from `createDataFrame([("A", {"value_1": 1, "value_2": "x"})], schema)`, the struct is converted to a JSON object. If the conversion passes a tuple as something other than a JSON array (e.g. serialized differently), or if the caller sends structs in another common format, the crate errors. Request: document the exact JSON shape expected for struct/array columns and/or accept the same struct representations PySpark does (object by name, array by position) so that create_dataframe_from_rows matches PySpark semantics.

### Error seen from Sparkless
`ValueError: create_dataframe_from_rows failed: struct value must be object or array`

---

## Issue 2: create_dataframe_from_rows – array column value must be null or array (PySpark parity)

**Title:** create_dataframe_from_rows: array column value must be null or array (PySpark accepts list)

### Summary
Creating a DataFrame from rows with array columns can fail with `array column value must be null or array`. PySpark createDataFrame accepts Python lists for array columns; the crate may expect a different JSON representation or reject some list payloads.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("arr", ArrayType(IntegerType())),
])

df = spark.createDataFrame([("x", [1, 2, 3]), ("y", [4, 5])], schema)
df.collect()  # [Row(id='x', arr=[1, 2, 3]), Row(id='y', arr=[4, 5])]
```

### robin-sparkless (current behavior)

```rust
// src/session.rs – array handling
} else if let Some(arr) = v.as_ref().and_then(|x| x.as_array()) {
    // ...
} else {
    return Err(PolarsError::ComputeError(
        "array column value must be null or array".into(),
    ));
}
```

So the value must be JSON null or a JSON array. If the caller sends array column as a JSON array and it still fails, the issue may be nested element types or encoding. Request: ensure any JSON array payload that PySpark would treat as an array column is accepted (or document the exact expected format), and align behavior with PySpark for nested arrays and nulls.

### Error seen from Sparkless
`ValueError: create_dataframe_from_rows failed: array column value must be null or array`

---

## Issue 3: Filter/collect – string vs numeric comparison (type coercion)

**Title:** Filter predicate: type coercion for string vs numeric comparison (PySpark parity)

### Summary
PySpark allows comparisons between string and numeric columns (e.g. string column eq int literal) and coerces types. Robin-sparkless fails at collect with an error like `cannot compare string with numeric type (i64)` or `filter predicate was not of type boolean`, so filter semantics differ from PySpark.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("123",), ("456",)], ["s"])
# String column compared to int: PySpark coerces and evaluates
df.filter(df["s"] == 123).collect()  # [Row(s='123')]
```

### robin-sparkless
Filter/collect should accept the same comparisons as PySpark (with coercion) or return a clear “unsupported comparison” error. Currently Sparkless sees:
- `collect failed: cannot compare string with numeric type (i64)`
- `collect failed: filter predicate was not of type boolean`

Request: document or implement PySpark-like coercion (or explicit cast requirements) for filter predicates so that string/numeric comparisons behave like PySpark.

---

## Issue 4: unionByName – type String incompatible with expected type Int64

**Title:** unionByName: type coercion when column types differ (PySpark parity)

### Summary
PySpark unionByName can coalesce columns with different types (e.g. String and Int64) into a common type. Robin-sparkless fails with something like `type String is incompatible with expected type Int64`, so unionByName semantics differ from PySpark.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
df2 = spark.createDataFrame([("2", "b")], ["id", "name"])  # id is string
# PySpark can union by name and coerce/union types
df1.unionByName(df2).collect()  # Often succeeds or promotes to string
```

### robin-sparkless
Request: document or implement unionByName behavior when same-named columns have different types (e.g. coercion or clear error message) to match PySpark semantics where possible.

### Error seen from Sparkless
`collect failed: type String is incompatible with expected type Int64`

---

## Issue 5: Join – column not found after join (e.g. "not found: ID")

**Title:** Join: column resolution / case sensitivity (e.g. "not found: ID") – PySpark parity

### Summary
After a join, collect fails with `not found: ID` (or similar). PySpark resolves column names in join results (including case-insensitive resolution when configured). Robin-sparkless may use different name resolution or casing, causing column not found at collect.

### PySpark (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([(1, "x")], ["id", "val"])
df2 = spark.createDataFrame([(1, "y")], ["ID", "other"])  # different case
j = df1.join(df2, df1["id"] == df2["ID"])
j.collect()  # PySpark resolves id/ID in result
```

### robin-sparkless
Request: ensure join result schema exposes columns so that names (or case-insensitive resolution) match what PySpark would expose, or document the resolution rules to avoid "not found" at collect.

### Error seen from Sparkless
`collect failed: not found: ID`

---

## New parity issues observed (from recent Robin test run)

These were seen when running the full test suite with `SPARKLESS_TEST_BACKEND=robin` and may be worth filing or triaging as Sparkless vs robin-sparkless.

### Issue 6: Session / builder config API (PySpark parity)

**Title:** SparkSession.builder.config() and spark.conf missing (Robin)

**Summary:** PySpark supports `SparkSession.builder.config("key", "value").getOrCreate()` and `spark.conf.get("key")` / `spark.conf.set(...)` / `spark.conf.is_case_sensitive()`. With the Robin backend, `RobinSparkSessionBuilder` has no attribute `config` and `RobinSparkSession` has no attribute `conf`, so case-sensitivity and other session configuration tests fail.

**Error seen:**  
`AttributeError: 'RobinSparkSessionBuilder' object has no attribute 'config'`  
`AttributeError: 'RobinSparkSession' object has no attribute 'conf'`

**Action:** Implement or wrap in Sparkless: `builder.config(key, value)` and `session.conf` (e.g. `conf.get`, `conf.set`, `is_case_sensitive()`) delegating to the crate if it supports config, or stubbing for compatibility.

---

### Issue 7: create_dataframe_from_rows – "schema must not be empty when rows are not empty"

**Summary:** The crate returns `create_dataframe_from_rows: schema must not be empty when rows are not empty` in cases where Sparkless sends non-empty rows. PySpark createDataFrame accepts rows with an explicit schema; an empty or missing schema with non-empty data may be rejected. This can be a Sparkless bug (sending empty schema in some code path) or a crate requirement that differs from PySpark (e.g. when schema is inferred or passed differently).

**Error seen:**  
`ValueError: create_dataframe_from_rows failed: create_dataframe_from_rows: schema must not be empty when rows are not empty`

**Action:** In Sparkless, ensure we never call the crate with non-empty rows and an empty/missing schema. If the crate expects a different contract, document or align with PySpark semantics.

---

### Issue 8: cast() – type object vs string (Sparkless or crate)

**Summary:** PySpark allows `F.cast(col, IntegerType())` or `col.cast("int")`. The Sparkless Robin extension may pass a Python type object (e.g. `IntegerType`) to the crate, which expects a string type name, leading to `'IntegerType' object cannot be converted to 'PyString'`.

**Error seen:**  
`TypeError: argument 'type_name': 'IntegerType' object cannot be converted to 'PyString'`

**Action:** In Sparkless, normalize cast type to a string (e.g. `IntegerType()` → `"int"`) before calling the crate, so the crate receives a type name string; or extend the crate to accept type objects if desired.

---

### Issue 9: F.sum(column) signature (PySpark parity)

**Summary:** PySpark supports `F.sum(col)` (column argument) and `df.sum("col_name")`. With Robin, `sum()` may be exposed with a different signature (e.g. no column argument), causing `sum() takes 1 positional arguments but 2 were given` when tests call `F.sum("col")` or similar.

**Error seen:**  
`TypeError: sum() takes 1 positional arguments but 2 were given`

**Action:** If the crate supports sum with a column argument, expose it in the Sparkless extension and Python F so that `F.sum(col)` works; otherwise track as crate API parity.

---

### Issue 10: Date vs datetime comparison semantics

**Summary:** A test such as `test_datetime_less_than_date` (datetime column compared to date) returns 0 rows with Robin, while PySpark may return rows depending on coercion and comparison rules. Suggests possible difference in date/datetime comparison or type coercion.

**Error seen:**  
`test_datetime_less_than_date - assert 0 == 1` (e.g. `len(rows) == 0` where one row was expected)

**Action:** Document or align date/datetime comparison and coercion with PySpark so that expressions like `datetime_col < date_literal` behave consistently.

---

### Issue 11: F.expr / F.struct not exposed

**Summary:** Tests that use `F.expr("...")` or `F.struct(...)` fail with `module 'sparkless.sql.functions' has no attribute 'expr'` or `'struct'`. PySpark provides both. Either the robin-sparkless crate does not expose them, or the Sparkless extension does not wire them; in either case it is a parity gap.

**Error seen:**  
`AttributeError: module 'sparkless.sql.functions' has no attribute 'expr'`  
`AttributeError: module 'sparkless.sql.functions' has no attribute 'struct'`

**Action:** If the crate has expr/struct, expose them in the Sparkless extension and F; if not, track as crate parity and optionally add stubs that raise a clear error when used with Robin.
