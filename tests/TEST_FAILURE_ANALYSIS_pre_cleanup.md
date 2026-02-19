# Test Failure Analysis (Robin Backend)

**Run:** `SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12`  
**Results file:** `tests/results_robin_20260218_194747.txt`

## Summary

| Phase     | Passed | Failed | Errors | Skipped |
|-----------|--------|--------|--------|---------|
| Unit      | 271    | 746    | 118    | 4       |
| Parity    | 17     | 233    | 13     | 6       |

**Total failures/errors:** 979 unit + 246 parity = **1225** (excluding passed/skipped).

---

## 1. Tests to DELETE (obsolete or wrong abstraction)

These tests target removed behavior (v4 backend/Polars) or are invalid for Robin-only execution.

### 1.1 Remove entire files (reference removed code)

| File | Reason |
|------|--------|
| `tests/unit/backend/test_robin_unsupported_raises.py` | Empty after unskip; targeted legacy BackendFactory. |
| `tests/unit/backend/test_robin_optional.py` | References `BackendFactory.list_available_backends()`, `create_storage_backend`, `create_materializer`, `create_export_backend` — all removed in v4. |
| `tests/unit/backend/test_robin_materializer.py` | References `BackendFactory`, `_robin_available_cache`; tests legacy materializer. |
| `tests/test_backend_capability_model.py` | References `PolarsMaterializer`, `can_handle_operation` — backend package removed. |
| `tests/test_issue_160_*.py` (all 9 files) | Use Polars materializer / BackendFactory / cache; backend removed in v4. |

### 1.2 Consider deleting or moving to archive

| File / area | Reason |
|-------------|--------|
| `tests/unit/dataframe/test_inferschema_parity.py` | 50+ failures/errors: all use `spark.read.option(...)` or CSV/inferSchema. Robin reader has no `.option()` and CSV path differs. Either implement reader options in Sparkless (fix) or drop/move to archive if out of scope for Robin. |
| Parity tests that require PySpark-specific SQL/DDL | e.g. `createDatabase`, `CREATE TABLE AS SELECT` without Hive — mark or move if Robin will not support. |

---

## 2. Robin-sparkless (upstream crate) — file issues in robin-sparkless

Failures that indicate the **robin-sparkless** crate does not match PySpark semantics. Fix or track in [robin-sparkless](https://github.com/eddiethedean/robin-sparkless).

### 2.1 Type / comparison semantics

| Error pattern | Example tests | Action |
|---------------|----------------|--------|
| `collect failed: cannot compare string with numeric type (i64)` | test_issues_225_231 (numeric_eq_string) | Crate: allow or coerce string vs numeric comparison. |
| `collect failed: not found: ID` | test_column_case_variations (join_all_case_variations) | Crate: case-sensitive column resolution; match PySpark default (case-insensitive). |
| `collect failed: type String is incompatible with expected type Int64` | test_union_type_coercion (union_by_name) | Crate: union type coercion to match PySpark. |
| `create_dataframe_from_rows failed: array column value must be null or array` | test_issues_225_231 (getItem), test_array_type_robust, test_array_parameter_formats | Crate: accept/createDataFrame array representation. |
| `create_dataframe_from_rows failed: unsupported type 'map<string,string>'` | test_create_map (many), test_issues_225_231 (getItem map), test_inferschema_parity (complex_types) | Crate: support MapType in createDataFrame. |
| `create_dataframe_from_rows failed: struct value must be object or array` | test_column_case_variations (nested_struct_field_access) | Crate: struct representation. |
| `create_dataframe_from_rows failed: array element type '...' not supported` | test_array_type_robust (nested_arrays, triple_nested) | Crate: nested array element types. |

### 2.2 Execution / SQL

| Error pattern | Example tests | Action |
|---------------|----------------|--------|
| `ValueError: select expects Column or str` / `cannot convert to Column` | test_create_map (most tests) | Crate or Sparkless: accept F.create_map() / expression results as Column in select. |
| `Table or view 'X' not found` (when view exists) | test_column_case_variations (sql_queries_all_case_variations) | Crate/session: temp view registration or lookup. |

### 2.3 Join / aggregation semantics

| Error pattern | Example tests | Action |
|---------------|----------------|--------|
| Join result type/coercion (e.g. int vs string in result) | test_join_type_coercion (test_join_int64_with_string, test_join_int32_with_string, test_join_float_with_string, test_join_type_coercion_parity_pyspark) | Crate: join type coercion to match PySpark. |
| AssertionError on join output (tuple in set) | Same as above | Same as above. |

---

## 3. Fix Sparkless (Python / Robin integration layer)

Failures that should be fixed in **this repo** (Sparkless): missing APIs, wrong conversion, or session/reader implementation.

### 3.1 PyColumn missing methods (Rust PyColumn or Python wrapper)

Robin’s `PyColumn` is missing PySpark Column methods. Add them in the extension or wrap with Python fallbacks.

| Missing attribute | Affected areas | Fix |
|-------------------|----------------|-----|
| `astype` | test_column_astype (all), test_chained_arithmetic (some) | Implement on PyColumn (or map to F.cast in Python). |
| `desc_nulls_last`, `asc_nulls_last`, `desc_nulls_first`, `asc_nulls_first` | test_column_ordering (all) | Implement on PyColumn. |
| `desc`, `asc` | test_column_case_variations (orderBy_all_case_variations), test_column_ordering | Implement on PyColumn. |
| `isin` | test_issues_225_231 (TestIssue226IsinWithValues) | Implement on PyColumn. |
| `getItem` | test_issues_225_231 (TestIssue227GetItem) | Implement on PyColumn. |
| `substr` | test_column_substr (all) | Implement on PyColumn (or F.substring). |
| `startswith` | test_column_case_variations (filter_all_case_variations) | Implement on PyColumn. |
| `withField` | test_withfield (empty_dataframe and others) | Implement on PyColumn. |
| `name` | test_column_case_variations (attribute_access_all_case_variations) | Implement on PyColumn. |

### 3.2 Column reverse operators (int/float * PyColumn, etc.)

| Error pattern | Affected | Fix |
|---------------|----------|-----|
| `unsupported operand type(s) for *: 'int' and 'builtins.PyColumn'` (and +, -, /, %) | test_chained_arithmetic, test_string_arithmetic, test_column_case_variations | Implement `__radd__`, `__rsub__`, `__rmul__`, `__rtruediv__`, `__rmod__` on PyColumn so literals on the left work. |

### 3.3 fillna / subset

| Error pattern | Affected | Fix |
|---------------|----------|-----|
| `'builtins.PyColumn' object is not callable` | test_fillna_subset (all), test_column_case_variations (withColumnRenamed, selectExpr, replace, dropna, etc.) | fillna(subset=[...]) or internal handling is treating a Column as callable; fix fillna implementation for Robin DataFrame. |

### 3.4 Reader API

| Error pattern | Affected | Fix |
|---------------|----------|-----|
| `'RobinDataFrameReader' object has no attribute 'option'` | test_inferschema_parity (all CSV/option-based tests) | Implement `option(key, value)` on RobinDataFrameReader and pass through to engine. |

### 3.5 Session / lifecycle

| Error pattern | Affected | Fix |
|---------------|----------|-----|
| `'RobinSparkSession' object has no attribute 'stop'` | test_udf_comprehensive (all) | Implement `stop()` on RobinSparkSession. |
| `type object 'RobinSparkSession' has no attribute '_has_active_session'` | test_create_map (test_create_map_empty_in_nested_expressions) | Implement or replace _has_active_session for Robin. |
| `RuntimeError: Cannot perform max aggregate function: No active SparkSession found` | test_create_map (test_create_map_in_groupby_agg) | Ensure global/session context is set when running aggregations. |

### 3.6 Functions module (F.)

| Error pattern | Affected | Fix |
|---------------|----------|-----|
| `module 'sparkless.sql.functions' has no attribute 'first'` | test_first_ignorenulls (all) | Expose `first` in _robin_functions. |
| `module 'sparkless.sql.functions' has no attribute 'rank'` | test_column_case_variations (window_functions_with_case_variations) | Expose `rank` in _robin_functions. |

### 3.7 GroupedData / pivot

| Error pattern | Affected | Fix |
|---------------|----------|-----|
| `NotImplementedError: GroupedData.agg() not yet fully implemented for Robin backend` | test_column_case_variations (groupBy, agg, chained_operations, complex_query), test_string_arithmetic (groupby agg) | Implement or delegate GroupedData.agg() for Robin. |
| `'RobinGroupedData' object has no attribute 'pivot'` | test_column_case_variations (pivot_all_case_variations) | Implement pivot on RobinGroupedData. |

### 3.8 withField / struct conversion

| Error pattern | Affected | Fix |
|---------------|----------|-----|
| `'tuple' object cannot be converted to 'PyDict'` | test_withfield (all) | When calling into Rust, convert Python tuple (struct rows) to dict/PyDict or the representation the crate expects. |
| `'tuple' object cannot be converted to 'PyDict'` (createDataFrame) | test_issue_270_tuple_dataframe (all) | createDataFrame(tuple data, schema): convert tuple rows to the structure Robin expects (e.g. dict-like). |

### 3.9 Schema / NoneType

| Error pattern | Affected | Fix |
|---------------|----------|-----|
| `'NoneType' object has no attribute 'fields'` | test_column_case_variations (select_all_case_variations, unionByName_all_case_variations) | Ensure DataFrame.schema (or .schema.fields) is never None for Robin DataFrames. |

### 3.10 Error message parity

| Error pattern | Affected | Fix |
|---------------|----------|-----|
| `createDataFrame requires schema when data is empty` vs expected `can not infer schema from empty dataset` | test_inferschema_parity (test_create_dataframe_empty_list) | Align Sparkless error message with PySpark (or relax test). |

### 3.11 Join column handling

| Error pattern | Affected | Fix |
|---------------|----------|-----|
| `'builtins.PyColumn' object is not iterable` | test_join_type_coercion (test_join_with_left_on_right_on) | left_on/right_on handling for Robin; expect list/iterable of columns. |

---

## 4. Suggested order of work

1. **Delete** obsolete tests (section 1) to reduce noise.
2. **Sparkless fixes** that unblock many tests:
   - PyColumn: `astype`, `desc_nulls_last`/`asc_nulls_last`/`desc`/`asc`, `isin`, `getItem`, `substr`, reverse operators (`__r*__`).
   - RobinDataFrameReader: `option()`.
   - RobinSparkSession: `stop()`.
   - Functions: `first`, `rank`.
   - fillna(subset=...) and Column-not-callable.
   - withField / createDataFrame tuple → dict (or crate-compatible) conversion.
3. **Robin-sparkless** (upstream): create_dataframe_from_rows (map, array, struct), select/Column conversion for create_map, comparison/union coercion, join type coercion, temp view lookup.
4. **Re-run** with `SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12` and iterate.

---

## 5. Results file location

Full stdout/stderr: **`tests/results_robin_20260218_194747.txt`**

To regenerate:

```bash
SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12 2>&1 | tee tests/results_robin_$(date +%Y%m%d_%H%M%S).txt
```
