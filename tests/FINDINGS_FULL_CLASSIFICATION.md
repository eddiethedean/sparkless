# Full Failure Classification (2026-02-11)

This document classifies all test failures from `test_results.txt` (run with Robin backend, 10 workers). Every failure is accounted for by one of: Robin limitation, Wrong Sparkless materializer, Bad test expectation, Documented v4 skip, or Environment/setup.

## Classification Summary

| Classification | Count | Reference |
|----------------|-------|-----------|
| Wrong Sparkless materializer (orderBy not translated) | ~12 | robin_limitations_sparkless_fixes.md §7; robin-sparkless #245 |
| Wrong Sparkless materializer (expression resolution) | ~35 | robin_limitations_sparkless_fixes.md §8; robin-sparkless #195, #198 |
| Sparkless Reader inferSchema string-only | ~15 | robin_limitations_sparkless_fixes.md §9; v4 design |
| Robin limitation (isin, desc_nulls_last) | ~2 | robin-sparkless #244, #245 |
| Robin type strictness (union coercion) | ~3 | robin-sparkless #201 |
| Robin filter unsupported (isin[]) | 1 | robin-sparkless #244 |
| Documented v4 skip | many | v4_robin_skip_list.txt |
| Parity (DataFrames not equivalent) | ~128 | Various expression/result parity |

## Unit Test Failures by Pattern

### 1. SparkUnsupportedOperationError: Operation 'Operations: orderBy' is not supported

**Classification:** Wrong Sparkless materializer  
**Reference:** robin_limitations_sparkless_fixes.md §7; robin-sparkless #245

| Test |
|------|
| test_column_ordering.py (all) |
| test_column_case_variations.py::test_orderBy_all_case_variations |

Robin has `order_by` but lacks `Column.desc_nulls_last()` (#245). Sparkless does not translate orderBy to Robin.

---

### 2. SparkUnsupportedOperationError: Operation 'Operations: filter' is not supported

**Classification:** Robin limitation (isin not on Column)  
**Reference:** robin-sparkless #244

| Test |
|------|
| test_issues_225_231.py::TestIssue226IsinWithValues::test_isin_with_empty_list |

---

### 3. RuntimeError: not found: Column 'X' not found (withField, map, getItem, selectExpr, chained arithmetic)

**Classification:** Wrong Sparkless materializer (expression resolution)  
**Reference:** robin_limitations_sparkless_fixes.md §8; robin-sparkless #195, #198

| Test file | Pattern |
|-----------|---------|
| test_withfield.py | withField expression |
| test_create_map.py | map_col, map() |
| test_issues_225_231.py (getItem) | first, val (alias) |
| test_column_case_variations.py | full_name (selectExpr), Alice (filter literal), ID (join) |
| test_chained_arithmetic.py | reverse arithmetic, compound expressions |
| test_column_astype.py | substring, num_str (alias) |

---

### 4. AssertionError: id should be LongType / flag1 should be BooleanType / etc.

**Classification:** Sparkless Reader inferSchema string-only  
**Reference:** robin_limitations_sparkless_fixes.md §9; v4 design

| Test file |
|-----------|
| test_inferschema_parity.py |

---

### 5. AssertionError: assert None == '1' / assert None == 1 (astype returning None)

**Classification:** Wrong Sparkless materializer (astype/cast translation)  
**Reference:** Phase 7 expression coverage; robin-sparkless #200

| Test file |
|-----------|
| test_column_astype.py |

---

### 6. RuntimeError: type Int64 is incompatible with expected type String

**Classification:** Robin type strictness  
**Reference:** robin-sparkless #201

| Test file |
|-----------|
| test_union_type_coercion.py |

---

### 7. RuntimeError: casting from string to boolean failed

**Classification:** Wrong Sparkless materializer or Robin semantics  
**Reference:** robin-sparkless #199

| Test |
|------|
| test_column_astype.py::test_astype_boolean |

---

### 8. Documented v4 skip (v4_robin_skip_list.txt)

**Classification:** Documented v4 skip  
**Reference:** tests/unit/v4_robin_skip_list.txt; docs/v4_behavior_changes_and_known_differences.md

Tests in the skip list are not run when SPARKLESS_TEST_BACKEND=robin. The test_results.txt run used Backend: mock (Robin). Some failures overlap with skip list patterns.

---

## Parity Test Failures

Parity failures (tests/parity/): ~128 failed. Patterns:

- **AssertionError: DataFrames are not equivalent** — Expression or result parity; Robin/PySpark result shape or value difference.
- **SparkUnsupportedOperationError: Operation 'Operations: join' is not supported** — Sparkless materializer.
- **SparkUnsupportedOperationError: Operation 'Operations: orderBy' is not supported** — Sparkless materializer (§7).
- **RuntimeError: not found: Column 'X'** — Expression resolution (§8).

---

## Robin Issues Filed (2026-02-11)

| # | Title |
|---|-------|
| 244 | [0.7.0 repro] Column.isin() not found |
| 245 | [0.7.0 repro] Column.desc_nulls_last() and nulls ordering methods not found |

---

## Repro Results (robin-sparkless 0.7.0)

All 10 original repros (01–10) pass with 0.7.0. New repros 11 and 12 confirm:
- 11: Robin lacks `Column.desc_nulls_last()` → #245 filed
- 12: Robin lacks `Column.isin()` → #244 filed
