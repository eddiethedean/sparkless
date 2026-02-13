# Sparkless backlog: failures to fix in Sparkless (not Robin)

This document tracks test failures that are attributable to **Sparkless** (expression translation, join key synthesis, literal-as-column, etc.), not Robin parity gaps. Robin parity gaps are reported to [robin-sparkless](https://github.com/eddiethedean/robin-sparkless).

**Robin parity issues created 2026-02-12:** #262 (round string), #263 (array empty), #264 (posexplode missing), #265 (date/string comparison), #266 (eqNullSafe type coercion).

---

## Failure patterns attributable to Sparkless

### 1. Operation 'Operations: withColumn' is not supported

**Root cause:** Sparkless Robin materializer does not translate certain expressions in withColumn (e.g. UDF, chained to_timestamp+regexp_replace, CaseWhen, window functions). Robin *does* support withColumn with basic expressions; the gap is Sparkless translation.

**Representative tests:**
- `tests/test_issue_290_udf_multiple_arguments.py` (UDF – Robin has no UDF; this may be Robin scope)
- `tests/test_issue_288_casewhen_operators.py` (CaseWhen with arithmetic)
- `tests/test_issue_145_string_cast.py::test_string_cast_works_with_to_timestamp`
- `tests/test_issue_151_to_timestamp_validation.py`
- `tests/test_issue_160_dropped_column_execution_plan.py`
- `tests/parity/dataframe/test_double_join_empty_aggregated.py`

**Sparkless fix:** Extend `_expression_to_robin` and `_can_handle_with_column` in `sparkless/backend/robin/materializer.py` to translate more expression shapes. Note: UDF is a Robin limitation (no UDF support).

---

### 2. Operation 'Operations: filter' is not supported

**Root cause:** Sparkless does not translate certain filter expressions (complex AND/OR, isin with expression, like with expr, etc.). Robin supports filter; the gap is translation.

**Representative tests:**
- `tests/unit/test_issues_225_231.py::TestIssue226IsinWithValues::test_isin_with_empty_list`
- `tests/parity/dataframe/test_filter_isinstance_ordering.py::test_string_operations_in_filters`
- `tests/parity/dataframe/test_filter_isinstance_ordering.py::test_logical_operations_in_filters`

**Sparkless fix:** Extend `_simple_filter_to_robin` for more expression shapes. Robin 0.8.3 supports `isin([])` (verified in `scripts/repro_robin_limitations/12_isin_empty_list.py`).

---

### 3. Operation 'Operations: select' is not supported

**Root cause:** Sparkless does not translate certain select expressions (CaseWhen, window+alias, compound expressions, getItem, regexp_extract).

**Representative tests:**
- `tests/unit/test_issues_225_231.py::TestIssue227GetItem::*` (getItem with array/map)
- `tests/unit/test_issues_225_231.py::TestIssue228RegexLookAheadLookBehind::*`
- `tests/unit/test_window_arithmetic.py::test_avg_window_with_arithmetic`
- `tests/unit/test_window_arithmetic.py::test_sum_window_with_arithmetic`

**Sparkless fix:** Extend `_expression_to_robin` and `_can_handle_select` for CaseWhen, window functions with arithmetic, getItem, regexp_extract.

---

### 4. RuntimeError: not found: Column 'X' not found (expression resolution)

**Root cause:** Robin resolves columns by **name**. Sparkless sometimes sends an expression string or alias name (e.g. `'Alice'`, `'IT'`, `'score DESC'`, `'(row_number() OVER ...) * 2'`) so Robin looks for a column with that literal name and fails. The fix is Sparkless: send evaluated Robin expressions, not names.

**Representative tests:**
- `tests/test_issue_203_filter_with_string.py` – literal 'IT' in filter (dept value)
- `tests/test_issue_230_case_insensitive_column_matching.py::test_filter_case_insensitive` – 'Alice'
- `tests/integration/test_case_sensitivity.py::test_case_insensitive_filter` – 'Alice'
- `tests/integration/test_case_sensitivity.py::test_case_sensitive_sql_queries_require_exact_case` – 'IT'
- `tests/unit/test_window_arithmetic.py::test_dense_rank_with_arithmetic` – 'score DESC'
- `tests/unit/test_window_arithmetic.py::test_rank_with_arithmetic` – 'score DESC'
- `tests/unit/test_window_arithmetic.py::test_window_function_chained_operations` – '(percent_rank() OVER ...) * 100'
- `tests/unit/test_window_arithmetic.py::test_ntile_with_arithmetic` – '(ntile() OVER ...) - 1'
- `tests/unit/test_window_arithmetic.py::test_complex_nested_arithmetic` – '((row_number() OVER ...) * 2) + 10'
- `tests/parity/functions/test_aggregate_cast_parity.py::test_cast_with_empty_groups` – 'A'
- `tests/test_issue_286_aggregate_function_arithmetic.py::test_empty_group_handling` – 'Charlie'
- `tests/test_issue_288_casewhen_operators.py::test_casewhen_chained_operations` – 'Alice'

**Sparkless fix:** Extend `_expression_to_robin` and select/withColumn handling so that aliased expressions are translated to Robin expr + `.alias(name)` and the resulting Column is used, not the alias name. Handle literals in filter (F.lit) vs column names. Window+arithmetic: translate full expression, not stringified form.

---

### 5. RuntimeError: datatypes of join keys don't match - __sparkless_join_key__

**Root cause:** Sparkless synthesizes a temp join key column `__sparkless_join_key__` when left/right have different key column names. The synthesized column can have mismatched dtypes (str vs i64) between left and right.

**Representative tests:**
- `tests/test_issue_152_sql_column_aliases.py::test_sql_with_inner_join_and_aliases`
- `tests/test_issue_152_sql_column_aliases.py::test_sql_with_left_join_and_aliases`
- `tests/parity/sql/test_advanced.py::test_sql_with_inner_join`
- `tests/test_issue_332_cast_alias_select.py::test_cast_alias_select_with_join`

**Sparkless fix:** Ensure join key synthesis in `sparkless/backend/robin/materializer.py` (or SQL executor) produces matching dtypes for left and right `__sparkless_join_key__` columns.

---

### 6. lengths don't match (split limit – Sparkless translation)

**Root cause:** Robin 0.8.3 supports `F.split(col, pattern, limit)` (verified in `scripts/robin_parity_repros/07_split_limit.py`). Sparkless parity test fails with "lengths don't match: unable to add a column of length N to a DataFrame of height 1". This suggests Sparkless translates split incorrectly (wrong number of output rows or column shape).

**Representative tests:**
- `tests/parity/functions/test_split_limit_parity.py::test_split_with_limit_parity`
- `tests/parity/functions/test_split_limit_parity.py::test_split_with_limit_1_parity`
- `tests/test_issue_328_split_limit.py::*`

**Sparkless fix:** Review how Sparkless translates `F.split(col, pattern, limit)` to Robin. Ensure output column length matches DataFrame height.

---

### 7. Case sensitivity / DID NOT RAISE

**Root cause:** Tests expect Sparkless to raise when case-sensitive mode requires exact column match and wrong case is used. Robin/Sparkless may not enforce case sensitivity in the same way as PySpark.

**Representative tests:**
- `tests/integration/test_case_sensitivity.py::test_case_sensitive_mode_exact_match_required` – Failed: DID NOT RAISE
- `tests/integration/test_case_sensitivity.py::test_case_sensitive_filter_fails_with_wrong_case` – Failed: DID NOT RAISE
- `tests/integration/test_case_sensitivity.py::test_case_sensitive_select_fails_with_wrong_case` – Failed: DID NOT RAISE
- `tests/integration/test_case_sensitivity.py::test_case_sensitive_groupBy_fails_with_wrong_case` – Failed: DID NOT RAISE
- `tests/integration/test_case_sensitivity.py::test_ambiguity_detection` – AssertionError: assert None == 'Alice'

**Sparkless fix:** Align case sensitivity behavior with session config `spark.sql.caseSensitive`. When case-sensitive, validate column names and raise if wrong case. Handle ambiguity detection for duplicate names.

---

### 8. Other Sparkless-owned patterns

- **Parity assertion (DataFrames not equivalent):** Result schema, order, or values differ. May be Sparkless translation or Robin result format. Investigate per test.
- **Assert None / wrong result:** Computed column is None (expression not applied). Check `_expression_to_robin` coverage.
- **KeyError in row_number:** `tests/test_issue_414_row_number_over_descending.py` – Robin indexing vs Sparkless translation for orderBy(desc()).

---

## Repro scripts

Repro scripts for Robin parity verification: `scripts/robin_parity_repros/`. Run all: `python scripts/robin_parity_repros/run_all_repros.py`.
