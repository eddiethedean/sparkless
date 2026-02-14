# Sparkless failures backlog (to fix in Sparkless)

Test failures classified as **Sparkless's fault** (compat layer, wrapper API, fixtures, or createDataFrame/schema), not Robin parity. Generated from test run `test_run_20260213_194551.txt` (1940 failed, 687 passed). Robin parity issues are tracked separately and filed in [robin-sparkless](https://github.com/eddiethedean/robin-sparkless).

**Classification output:** `tests/sparkless_backlog_failures.txt`, `tests/sparkless_backlog_failures_by_category.txt`  
**Script:** `scripts/classify_failures_robin_sparkless.py`

---

## 1. Expression / type conversion (compat layer)

Sparkless passes Sparkless expression types (ColumnOperation, Literal, WindowSpec, CaseWhen, WindowFunction, etc.) to Robin without converting them to Robin's Column/Window. Robin's Python API expects its own types.

| Category | Error pattern | Count (approx) | Suggested fix |
|----------|---------------|---------------|---------------|
| expression_conversion_column_operation | `'ColumnOperation' object cannot be converted to 'Column'` | 120+ | In `_robin_compat.py` (or caller), convert Sparkless ColumnOperation to Robin Column (e.g. resolve to column name or build Robin expr) before calling select/filter/with_column/agg. |
| expression_conversion_windowspec | `'WindowSpec' object cannot be converted to 'Window'` | 50+ | Tests use `sparkless.window.Window` (Sparkless) while F is Robin; use `sparkless.sql.Window` when backend is Robin, or convert WindowSpec to Robin Window in compat. |
| expression_conversion_literal | `'Literal' object cannot be converted to 'Column'` | 4 | Convert Literal to Robin lit() in compat before passing to Robin. |
| expression_conversion_other | `'WindowFunction'`, `'CaseWhen'`, `'Column'` (Sparkless), `'int'/'float'/'list'` etc. cannot be converted to `'Column'` | 320+ | Extend compat to convert WindowFunction, CaseWhen, Sparkless Column, and scalar/list to Robin Column where applicable. |

**Representative test IDs:**
- `tests/test_issue_138_column_drop_reference.py::TestIssue138ColumnDropReference::test_drop_then_select`
- `tests/test_issue_188_string_concat_cache.py::TestStringConcatenationCacheEdgeCases::*`
- `tests/test_issue_290_udf_multiple_arguments.py::*`
- `tests/parity/functions/test_window_orderby_list_parity.py::*`
- `tests/unit/test_window_arithmetic.py::*`
- `tests/test_issue_335_window_orderby_list.py::*`, `tests/test_issue_336_window_function_comparison.py::*`
- `tests/test_issue_339_column_subscript.py::*`, `tests/test_issue_330_struct_field_alias.py::*`

---

## 2. Missing or wrong wrapper API

The PySpark-compat wrapper (`_PySparkCompatDataFrame`, `_NaCompat`) is missing methods or returns the wrong type.

| Category | Error pattern | Suggested fix |
|----------|---------------|---------------|
| wrapper_nacompat_replace | `'_NaCompat' object has no attribute 'replace'` | Add `replace(self, to_replace, value=None, subset=None)` on `_NaCompat` that calls Robin's replace/fill or equivalent and returns wrapped DataFrame. |
| wrapper_dataframe_where | `'builtins.DataFrame' object has no attribute 'where'` | Add `where(self, condition)` on `_PySparkCompatDataFrame` that delegates to `filter(condition)`. |
| wrapper_columns_callable | `assert <function ...> == ['id', 'name']` (df.columns is a method, not list) | In `_PySparkCompatDataFrame.__getattr__("columns")`, if Robin exposes `columns` as callable, return `list(robin_df.columns())` so PySpark-style `df.columns` returns a list. |
| wrapper_crossjoin | `'builtins.DataFrame' object has no attribute 'crossJoin'` | Add `crossJoin(self, other)` that delegates to Robin join with how="cross" (or equivalent). |
| wrapper_dropduplicates_subset | `_distinct() got an unexpected keyword argument 'subset'` | In compat, implement `dropDuplicates(subset=...)` by selecting subset columns then calling distinct, or forward subset to Robin if supported. |
| wrapper_materialized | `'builtins.DataFrame' object has no attribute '_materialized'` | Add property or stub for tests that expect `_materialized`; or fix tests to not rely on internal attr. |

**Representative test IDs:**
- `tests/test_issue_287_na_replace.py::*` (replace)
- `tests/test_issue_260_eq_null_safe.py::*` (where)
- `tests/examples/test_unified_infrastructure_example.py::TestUnifiedInfrastructure::test_basic_operation` (columns)
- `tests/test_issue_200_list_rows_with_column_schema.py::*` (columns)

---

## 3. createDataFrame / schema

Failures from createDataFrame (pandas, tuple, list of rows, schema inference) or from schema/column list handling.

| Category | Error pattern | Suggested fix |
|----------|---------------|---------------|
| createDataFrame_schema_or_columns | `argument of type 'function' is not iterable` | Robin's DataFrame exposes `columns` as callable; Sparkless code that does `in df.columns` or iterates expects a list. Use `_compat_column_names(robin_df)` (or call `columns()` and wrap) everywhere compat touches column names. |
| createDataFrame_schema_inference | `assert StringType(...) == IntegerType(...)` | Schema inference in createDataFrame or reader returns string-only; preserve or infer numeric/date types when creating from pandas or dicts. **Phase 5 / follow-up:** This is deferred; fixing it requires changing schema inference or create_dataframe_via_robin so column types (e.g. int) are preserved instead of forced to string. |

**Representative test IDs:**
- `tests/test_column_availability.py::*`
- `tests/test_issue_212_select_with_column_list.py::*`
- `tests/unit/test_issues_225_231.py::TestIssue229PandasDataFrameSupport::*`
- `tests/parity/internal/test_session.py::TestSessionParity::test_createDataFrame_with_explicit_schema`

---

## 4. Fixtures / test setup

Tests use Sparkless types (e.g. `Window` from `sparkless.window`) with Robin-backed session, so expressions are Sparkless types and Robin rejects them.

| Category | Suggested fix |
|----------|---------------|
| Fixtures | For Robin-backed runs, fixtures should use `Window` (and F) from `sparkless.sql` so that WindowSpec is Robin's type. In `tests/fixtures/spark_imports.py` `_load_mock_spark_imports()`, use `from sparkless.sql import Window` (or get Window from same module as F) when backend is Robin. |

**Representative test IDs:** All tests that use `imports.Window` with Robin and fail with "WindowSpec cannot be converted to Window" (see expression_conversion_windowspec above).

---

## 5. Other Sparkless

Any remaining failures clearly in Sparkless code paths (not Robin API/semantics). See `tests/sparkless_backlog_failures_by_category.txt` for the full list by category.

---

## Summary counts (from classifier)

| Bucket | Count |
|--------|-------|
| Sparkless backlog | 531 |
| Robin parity | 314 |
| Unclear (needs manual/repro) | 1101 |

Total parsed: 1946 (FAILED + ERROR lines from test run).
