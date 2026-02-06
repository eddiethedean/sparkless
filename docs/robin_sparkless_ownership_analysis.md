# Robin Mode Ownership Analysis: Sparkless vs robin-sparkless

**Date:** 2026-02-06  
**Reference run:** `test_results_robin_mode.txt` — 1,656 failed, 1,050 passed, 22 skipped  
**Purpose:** Determine whether failures require Sparkless adaptations or robin-sparkless fixes.

---

## Executive Summary

**Most failures require Sparkless adaptations.** The robin-sparkless package exposes the APIs needed (filter, select, join, group_by, etc.), but uses a **method-based comparison API** (e.g. `col.gt(lit)`) instead of Python operators (`col > lit`). Sparkless currently generates operator-based expressions, which fail. Additionally, Sparkless’ Robin materializer has narrow `can_handle` rules and supports only a subset of operations.

**robin-sparkless issues identified:** Column class does not implement `__gt__`, `__lt__`, etc.; Python `col > lit` raises `TypeError`. Upstream fix would be adding operator overloads for PySpark compatibility.

---

## 1. Failure Categories

### Category A: SparkUnsupportedOperationError (Sparkless)

**Cause:** Robin materializer’s `can_handle_operation()` returns False; Sparkless raises fail-fast.

**Examples:**
- `test_create_map_with_literals`: `Operation 'Operations: select' is not supported` — select with Column expressions (e.g. `F.create_map(...).alias("map_col")`) fails because materializer only accepts `select([str, str, ...])`, not Column objects.
- UDF tests, withField tests, window comparison tests — these use operations the materializer does not declare support for.

**Owner:** **Sparkless** — extend `can_handle_operation` and add translation for:
- select with Column/expression payloads
- create_map, withField, UDFs (if robin-sparkless supports them; otherwise document as unsupported)

---

### Category B: Row count mismatch mock=0 (Mixed – Sparkless translation bug confirmed)

**Cause:** Filter/join returns 0 rows when it should return N.

**Root cause (confirmed):** Sparkless Robin materializer uses Python operators:
```python
return robin_col > robin_lit   # Fails!
```
robin-sparkless `Column` does **not** implement `__gt__`; it raises:
```
TypeError: '>' not supported between instances of 'builtins.Column' and 'builtins.Column'
```

robin-sparkless expects the method-based API:
```python
robin_col.gt(robin_lit)  # Works
```

**Owner:** **Sparkless** — change `_simple_filter_to_robin()` in `sparkless/backend/robin/materializer.py` to use `.gt()`, `.lt()`, `.ge()`, `.le()`, `.eq()`, `.ne()` instead of `>`, `<`, etc.

**Optional robin-sparkless improvement:** Add `__gt__`, `__lt__`, etc. for PySpark-style `col > lit` usage.

---

### Category C: Operations robin-sparkless may not support

**Examples:** create_map, UDFs, rlike lookaround, withField, window functions with complex expressions.

**Owner:** Depends on robin-sparkless API:
- If robin-sparkless exposes the function → **Sparkless** must translate to it.
- If not → either **Sparkless** documents as unsupported (fail-fast) or **robin-sparkless** adds support.

---

### Category D: AssertionError / wrong result (needs isolation)

**Cause:** Result shape or values differ from expected.

**Owner:** Requires per-test isolation:
- If translation to robin-sparkless is correct → likely **robin-sparkless** bug.
- If translation is wrong → **Sparkless** bug.

---

## 2. Action Items

### Sparkless (high priority)

| Item | File | Description |
|------|------|-------------|
| 1 | `sparkless/backend/robin/materializer.py` | Use `col.gt(lit)`, `col.lt(lit)`, etc. instead of `col > lit` in `_simple_filter_to_robin()` |
| 2 | `sparkless/backend/robin/materializer.py` | Broaden select handling for Column expressions where possible |
| 3 | `sparkless/backend/robin/materializer.py` | Extend `can_handle_operation` for more op shapes (with clear docs on limits) |

### Sparkless (medium priority)

| Item | Description |
|------|-------------|
| 4 | Document which operations are supported vs unsupported for Robin backend |
| 5 | Consider a fallback path: when Robin cannot handle an op, use Polars materializer if available |

### robin-sparkless (optional)

| Item | Description |
|------|-------------|
| 1 | Add `__gt__`, `__lt__`, `__ge__`, `__le__`, `__eq__`, `__ne__` to Column for PySpark compatibility |

---

## 3. Verification

After fixing the filter translation (item 1):

```bash
# Should pass filter and join parity
SPARKLESS_TEST_BACKEND=robin python -m pytest \
  tests/parity/dataframe/test_filter.py::TestFilterParity::test_filter_operations \
  tests/parity/dataframe/test_join.py::TestJoinParity::test_inner_join \
  -v --no-cov
```

---

## 4. Test Run Commands

```bash
# Full Robin run
SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python -m pytest \
  tests/ --ignore=tests/archive -n 12 --dist loadfile -v --tb=short \
  2>&1 | tee test_results_robin_mode.txt

# Sample with long tracebacks
SPARKLESS_TEST_BACKEND=robin python scripts/run_robin_failure_sample.py --max 50
```
