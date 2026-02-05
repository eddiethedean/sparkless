# Robin Mode Failure Analysis

This document summarizes failure reasons when running the Sparkless test suite with the Robin backend (`SPARKLESS_TEST_BACKEND=robin`, `SPARKLESS_BACKEND=robin`), buckets them by cause, and classifies ownership (Sparkless vs robin-sparkless).

**Reference run:** Full suite (see [robin_mode_test_report.md](robin_mode_test_report.md)): **1,640 FAILED**, **867 PASSED**, **21 SKIPPED**, **7 ERROR**.  
**Sample run:** A subset of failing tests plus all 7 parquet ERROR tests was run with `--tb=long`; output is in `tests/robin_failures_sample.txt`.

---

## 1. Counts by bucket and exception type

### Full-suite totals (from reference run)

| Result   | Count |
|---------|-------|
| FAILED  | 1,640 |
| ERROR   | 7     |
| PASSED  | 867   |
| SKIPPED | 21    |

### Sample-based exception breakdown (from `robin_failures_sample.txt`)

From the sampled FAILED tests and the 7 ERROR tests:

| Bucket | Exception / cause | Sample count | Owner |
|--------|--------------------|--------------|--------|
| **A**  | `SparkUnsupportedOperationError` / "Backend 'robin' does not support these operations" | 8 of 12 FAILED in sample | Sparkless (policy) |
| **A**  | `QueryExecutionException` (wraps same reason) | 1 | Sparkless |
| **B**  | `ValueError: RobinMaterializer PoC only supports schema with exactly 3 columns` | 1 | Sparkless (or robin-sparkless if API limits schema) |
| **C**  | `AssertionError` (wrong row count / wrong result) | 2 (sample) + 7 (parquet tests when run without ERROR) | Sparkless or robin-sparkless (see below) |
| **C**  | `TypeError: '>' not supported between instances of 'builtins.Column' and 'builtins.Column'` | 2 | Sparkless (comparison/row handling) |
| **C**  | `AssertionError: assert 'robin' in ['mock', 'pyspark']` | 1 | Sparkless (test/fixture expectation) |
| **D**  | `AttributeError: 'RobinStorageManager' object has no attribute 'db_path'` (teardown) | 7 (all ERRORs) | Sparkless (Robin storage delegate) |

**Note:** The 7 parquet tests both **FAIL** (AssertionError on row counts) and **ERROR** (teardown `db_path`). So the 7 ERRORs are teardown failures; the same tests also fail in the test body with wrong results.

---

## 2. Bucket descriptions and ownership

### Bucket A – SparkUnsupportedOperationError / "does not support these operations"

- **Cause:** Robin materializer’s `can_handle_operations()` returns False for the op set; Sparkless raises and does **not** fall back to Polars.
- **Owner:** **Sparkless** (current fail-fast policy).
- **Sample ops seen:** `select`, `union`, `filter`, `join`, `orderBy`, and combinations.

**Options for Sparkless:**  
(1) Keep fail-fast and document;  
(2) Add optional “Robin + Polars fallback” so unsupported ops run with Polars (hybrid mode);  
(3) Extend the Robin materializer to support more ops (limited by what robin_sparkless exposes).

---

### Bucket B – ValueError / "exactly 3 columns"

- **Cause:** Robin materializer’s strict 3-column PoC schema (`RobinMaterializer` only supports that shape for `robin_sparkless.create_dataframe`).
- **Owner:** **Sparkless** if we can relax the schema in our materializer when robin_sparkless supports it; **robin-sparkless** if their `create_dataframe` only supports that shape (feature request there).

---

### Bucket C – AssertionError / wrong result / TypeError (Column comparison)

- **Wrong row count / wrong result:** If the failure happens **after** calling robin_sparkless (e.g. after `materialize()` returns), the wrong result is likely **robin-sparkless** (engine or API). If it happens in Sparkless (schema handling, row conversion, storage append semantics), **Sparkless**.
- **Parquet append tests:** Same 7 tests fail with AssertionError (e.g. “Table should have 1 row after append, got 2”). This points to **Sparkless** storage/session semantics (e.g. how Robin storage delegate handles append/visibility) or test expectations.
- **TypeError Column vs Column:** Comparison or ordering of Sparkless `Column` objects that are not simplified before reaching Robin or internal comparison → **Sparkless** (comparison/row handling or test helper).
- **`assert 'robin' in ['mock', 'pyspark']`:** Test or fixture assumes only mock/pyspark backends → **Sparkless** (test/fixture update).

---

### Bucket D – ERROR (setup/teardown)

- **Cause:** All 7 ERRORs are **AttributeError: 'RobinStorageManager' object has no attribute 'db_path'** in test teardown. The test file accesses `self.spark._storage.db_path`; `RobinStorageManager` delegates to Polars storage but does not expose a `db_path` attribute.
- **Owner:** **Sparkless** (Robin storage delegate compatibility).
- **Fix:** Add a `db_path` property (or attribute) to `RobinStorageManager` that mirrors `PolarsStorageManager.db_path` (e.g. expose the same path used when constructing the delegate).

---

## 3. Sparkless-fixable items

1. **Robin storage delegate:** Add `db_path` to `RobinStorageManager` (e.g. `self._db_path = db_path or ":memory:"` and a `db_path` property) so tests/fixtures that use `spark._storage.db_path` do not raise in teardown. This clears the 7 ERRORs.
2. **Optional Polars fallback:** When Robin cannot handle the op set, optionally run with Polars instead of raising (hybrid Robin + Polars mode). Document the current fail-fast behavior if we keep it.
3. **Use robin_sparkless arbitrary schema:** Robin-sparkless already provides `create_dataframe_from_rows(data, schema)` for arbitrary column names and types. Extend the Robin materializer to use it (instead of only `create_dataframe` for 3 columns) so we support any schema.
4. **Extend Robin materializer to more operations:** Robin-sparkless already exposes `with_column`, `order_by`, `group_by`, `join`, `union`, and `GroupedData` aggregations. Extend the Sparkless materializer to translate and call these (in addition to filter/select/limit) so more workloads run on the Robin backend.
5. **Parquet/table append semantics:** If append/visibility behavior under Robin storage is wrong, fix storage/session logic for the Robin delegate.
6. **Test/fixture expectations:** Update tests that assume only `['mock', 'pyspark']` (e.g. include `'robin'` where appropriate).
7. **Column comparison / TypeError:** Avoid comparing or ordering unresolved `Column` objects in code paths used by Robin or in assertion helpers; resolve to values or use supported expressions.

---

## 4. robin-sparkless items (for upstream)

**Finding (from examining robin-sparkless):** The robin-sparkless package already provides the APIs we need:

- **Arbitrary schema:** `create_dataframe_from_rows(data, schema)` accepts `data` (list of dicts or lists) and `schema` as a list of `(name, dtype_str)` (e.g. `[("id", "bigint"), ("name", "string")]`). Supported dtypes include bigint, double, string, boolean, date, timestamp, etc. Only `create_dataframe()` is 3-column (i64, i64, str); the flexible entry point is `create_dataframe_from_rows`.
- **Operations:** The DataFrame API already exposes `filter`, `select`, `with_column`, `order_by`, `order_by_exprs`, `group_by`, `limit`, `union`, `union_by_name`, `join`, and `GroupedData` (count, sum, avg, min, max, agg, etc.). See robin-sparkless `robin_sparkless.pyi` and Rust `src/dataframe/` (transformations, joins, aggregations).

So **no upstream feature requests are needed** for “more operations” or “flexible schema.” The gap is on the **Sparkless** side: our [Robin materializer](sparkless/backend/robin/materializer.py) currently uses only `create_dataframe` (3-column) and supports only filter/select/limit. We should:

1. Use **`create_dataframe_from_rows`** when the schema is not exactly 3 columns (or always, for consistency), converting Sparkless `StructType` and row data into the format robin_sparkless expects.
2. **Extend the materializer** to translate and invoke more operations (withColumn, groupBy, orderBy, join, union, etc.) using the existing robin_sparkless DataFrame API.

**Bugs:** Any concrete bug in robin_sparkless (wrong result, missing API, signature mismatch) should still be reported upstream with repro and environment. See [robin_sparkless_issues.md](robin_sparkless_issues.md) for issue templates.

---

## 5. How to re-run this analysis

1. **Full Robin run (summary):**
   ```bash
   pip install robin-sparkless  # if needed
   SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python -m pytest tests/ --ignore=tests/archive -n 10 -v --tb=short 2>&1 | tee tests/robin_mode_test_results.txt
   ```

2. **Sample of failing tests with long tracebacks:**
   ```bash
   SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python scripts/run_robin_failure_sample.py --max 50 --output tests/robin_failures_sample.txt
   ```

3. **Only the 7 parquet ERROR tests:**
   ```bash
   SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python scripts/run_robin_failure_sample.py --parquet-only --output tests/robin_failures_sample.txt
   ```

4. **Parse exception types:** Inspect `tests/robin_failures_sample.txt` for lines starting with `FAILED` or `ERROR`; the text after the last ` - ` is the exception type and message. Count by exception type and message to refresh the table in §1.

---

## 6. Files touched / added

- **Report:** `docs/robin_mode_failure_analysis.md` (this file).
- **Sample output:** `tests/robin_failures_sample.txt` (appended by the script).
- **Script:** `scripts/run_robin_failure_sample.py` (runs a fixed list of failing test IDs with `--tb=long` and appends to a file).
