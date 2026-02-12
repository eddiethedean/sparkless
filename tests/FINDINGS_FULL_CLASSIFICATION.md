# Full Failure Classification (Robin backend, 2026-02-11)

This document classifies test failures from a full run with **Robin backend** (`python -m pytest -n 10`). Run summary: **1397 failed, 1313 passed, 31 skipped, 4 errors**. Every failure is assigned to one of: **Sparkless** (fix in materializer/reader/tests), **Robin** (upstream limitation or API gap), **Parity** (result/behavior difference), or **Environment/skip**.

---

## Classification Summary

| Classification | Est. count | Reference |
|----------------|------------|-----------|
| **Sparkless: Expression resolution** (alias, selectExpr, withColumn expr name) | ~900+ | docs/robin_limitations_sparkless_fixes.md §8 |
| **Sparkless: Unsupported filter** (complex filter / isin empty) | ~80 | robin-sparkless #244; materializer filter translation |
| **Sparkless: Unsupported withColumn** (complex expr) | ~30 | _expression_to_robin coverage |
| **Sparkless: Unsupported select** (CaseWhen, window+alias, compound expr) | ~50 | §3, §4 |
| **Sparkless: Unsupported join** | ~20 | Plan/materializer join support |
| **Robin: Column resolved by name** ("not found: Column 'expr'") | ~1100 | Robin resolves by name; Sparkless must send evaluated expr |
| **Robin: Type strictness** (Int64 vs String, date vs string) | ~15 | robin-sparkless #201, #199 |
| **Robin: UDF / input_file_name / unsupported API** | ~50 | v4 scope; no UDF on Robin |
| **Parity** (DataFrames not equivalent, assert value) | ~80 | Result/ordering/type parity |
| **Test/setup** (DID NOT RAISE, ValueError schema, errors) | ~20 | Case sensitivity, backend, collection errors |
| **Documented v4 skip** | 31 | tests/unit/v4_robin_skip_list.txt |

---

## 1. RuntimeError: not found: Column 'X' not found

**Classification:** Mix of **Sparkless** (expression resolution) and **Robin** (resolution by name).

**Cause:** Robin resolves columns by **name**. Sparkless often sends an expression **string** or **alias name** (e.g. `full_name`, `map_col`, `(id % 2)`, `row_number() OVER (...)`) so Robin looks for a column with that literal name and fails.

**Sparkless fix:** Extend `_expression_to_robin` and select/withColumn handling so that:
- Select/withColumn use **evaluated Robin expressions**, not alias names or expr strings.
- Aliased expressions (e.g. `F.col("a").alias("map_col")`) are translated to a Robin expr + `.alias("map_col")` and the resulting Column is used in the plan, not the name `"map_col"`.

**Sub-patterns:**

| Pattern example | Count (approx) | Owner |
|-----------------|----------------|-------|
| `'map_col'`, `'map()'`, `'empty_map'` | 40+ | Sparkless (create_map/alias) |
| `'first'`, `'val'`, `'partial'`, `'domain'`, `'extracted'` | 30+ | Sparkless (getItem/alias, regexp_extract alias) |
| `'row_plus_10'`, `'percentile'`, `'zero_indexed'`, window+arithmetic alias | 22 | Sparkless (window + arithmetic alias) |
| `'full_name'`, `'upper(name)'`, `'substring(...)'` | 20+ | Sparkless (selectExpr/alias) |
| `'concat(first_name,  , last_name)'` | 5+ | Sparkless (concat→concat_ws) |
| `'to_timestamp_regexp_replace(...)'`, `'to_date(...)'` | 30+ | Sparkless (chained expr in withColumn) |
| `'udf(value)'`, `'udf(name)'` | 28+ | Robin (no UDF) / v4 skip |
| `'array(...)'`, `'struct(...)'`, `'explode(...)'` | 40+ | Sparkless (array/struct/explode translation) |
| `'input_file_name()'` | 9 | Robin / Sparkless (unsupported fn) |
| `'(id % 2)'`, `'(row_number() OVER ...)'` | 10+ | Sparkless (compound expr as single column) |
| `'Alice'`, `'IT'`, `'150'` (literal in filter) | 10+ | Sparkless (literal in filter/update) |
| `'None'` (lit(None)) | 8 | Sparkless (F.lit(None) translation) |
| `'Log10'`, `'format_string(...)'`, `'round(val, 0)'` | 10+ | Sparkless (function name or expr translation) |
| `'CASE WHEN ...'` (CaseWhen stringified) | 6 | Sparkless (§3 CaseWhen→when/then/otherwise) |
| `'my_struct'`, `'StructVal'` (struct field) | 40+ | Sparkless (withField/struct; §8, #195/#198) |

**Reference:** docs/robin_limitations_sparkless_fixes.md §8; robin-sparkless #195, #198.

---

## 2. SparkUnsupportedOperationError: Operation 'Operations: filter' is not supported

**Classification:** **Sparkless** (filter translation) + **Robin** (isin empty).

**Cause:** Materializer rejects the filter because:
- Filter condition uses an expression `_expression_to_robin` cannot translate (e.g. `isin([])`, complex AND/OR, like with expr, etc.), or
- Robin has no `Column.isin([])` (robin-sparkless #244).

**Count:** ~80+ failures.

**Sparkless:** Extend `_simple_filter_to_robin` for more expression shapes; keep isin([]) as Robin gap until #244.

**Reference:** robin-sparkless #244.

---

## 3. SparkUnsupportedOperationError: Operation 'Operations: withColumn' is not supported

**Classification:** **Sparkless.**

**Cause:** The withColumn expression is not translatable by `_expression_to_robin` (e.g. chained to_timestamp+regexp_replace, window, CaseWhen, etc.).

**Count:** ~30+.

**Reference:** docs/robin_limitations_sparkless_fixes.md §8; extend _expression_to_robin.

---

## 4. SparkUnsupportedOperationError: Operation 'Operations: select' is not supported

**Classification:** **Sparkless.**

**Cause:** Select list contains an expression (e.g. CaseWhen, window, opaque) that the materializer cannot translate.

**Count:** ~10+.

**Reference:** §3 (CaseWhen), §4 (window).

---

## 5. SparkUnsupportedOperationError: Operation 'Operations: join' is not supported

**Classification:** **Sparkless** (plan/join handling) or test expects join in a form Robin path doesn’t support.

**Count:** ~20+.

**Reference:** Plan executor / materializer join support.

---

## 6. RuntimeError: type Int64 is incompatible with expected type String / cannot compare string with numeric

**Classification:** **Robin** (type strictness).

**Cause:** Robin does not coerce types in union/join/filter like PySpark.

**Reference:** robin-sparkless #201.

---

## 7. RuntimeError: cannot compare 'date/datetime/time' to a string value

**Classification:** **Robin** (type strictness).

**Reference:** Robin datetime/string comparison semantics.

---

## 8. RuntimeError: datatypes of join keys don't match / lengths don't match: unable to vstack

**Classification:** **Robin** (type/column strictness) or **Sparkless** (schema/column order).

---

## 9. AssertionError: DataFrames are not equivalent

**Classification:** **Parity** (Robin vs expected behavior/result).

**Cause:** Result schema, order, or values differ from expected (e.g. string functions, math, null handling, window results).

**Count:** ~80.

---

## 10. AssertionError: Expected result=X, got None / assert None == value

**Classification:** **Sparkless** or **Robin** (expression not evaluated or wrong column).

**Cause:** Computed column is None (e.g. withColumn expr not applied, or Robin returns null).

---

## 11. Failed: DID NOT RAISE

**Classification:** **Test / Robin behavior.**

**Cause:** Test expects an exception (e.g. case-sensitive mode wrong case); Robin may not raise in the same way.

**Files:** integration/test_case_sensitivity.py.

---

## 12. ValueError: RobinMaterializer requires a non-empty schema

**Classification:** **Sparkless** (edge case: empty schema to materializer).

---

## 13. ValueError: Unsupported materializer type: polars

**Classification:** **Environment / v4.** v4 is Robin-only; tests that force Polars fail by design.

---

## 14. UDF / input_file_name / unsupported functions

**Classification:** **Robin** or **v4 scope** (no UDF on Robin; input_file_name may be missing or named differently).

**Reference:** v4_behavior_changes_and_known_differences.md; skip or adapt tests.

---

## 15. Documented v4 skip (v4_robin_skip_list.txt)

**Classification:** **Documented v4 skip.**

31 tests are skipped when `SPARKLESS_TEST_BACKEND=robin` via patterns in `tests/unit/v4_robin_skip_list.txt`. These are out of scope for Robin in v4.

---

## 16. Collection errors (4)

| Item | Classification |
|------|----------------|
| tests/parity/dataframe/test_parquet_format_table_append.py | Fixture/setup (parquet table with Robin) |
| tests/test_backend_capability_model.py | Backend/capability model |
| tests/test_issue_160_manual_cache_manipulation.py | Fixture/session |
| tests/test_issue_160_nested_operations.py | Fixture/session |

---

## Sparkless-side action items (from this classification)

1. **Expression resolution (§8):** Ensure select/withColumn pass **Robin Column expressions** (from `_expression_to_robin`) and not alias names or stringified expr. Fix alias handling so Robin receives expr.alias(name), not a column named name.
2. **concat → concat_ws:** Already implemented; verify all concat(col, lit(sep), col) paths use it.
3. **CaseWhen (§3):** Already in _expression_to_robin; verify plan path and select use it.
4. **Window (§4):** Window translation exists; fix window+arithmetic and window+alias so the resulting column is one Robin expression.
5. **Filter:** Extend filter translation for more expr shapes; document isin([]) as Robin #244.
6. **lit(None):** Translate F.lit(None) to a form Robin accepts (e.g. proper null literal).
7. **to_date / to_timestamp / format_string / split(..., -1):** Add or extend _expression_to_robin for these where Robin has equivalent API.

---

## Robin-side / upstream references

| Issue | Summary |
|-------|---------|
| #244 | Column.isin() not found; isin([]) unsupported |
| #245 | Column.desc_nulls_last() / nulls ordering not found |
| #195, #198 | Struct/map/expr API (withField, element_at, etc.) |
| #201 | Type strictness (Int64 vs String, union coercion) |
| #199 | Cast string to boolean semantics |

---

## Files used for this run

- **Failure list:** `failure_counts.txt` (from `pytest -n 10 --tb=no -q` with FAILED lines post-processed).
- **Skip list:** `tests/unit/v4_robin_skip_list.txt`.
- **Doc:** `docs/robin_limitations_sparkless_fixes.md`.
