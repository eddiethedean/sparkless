# Robin Mode Test Run Report

**Date:** 2026-02-05  
**Configuration:** `SPARKLESS_TEST_BACKEND=robin`, `SPARKLESS_BACKEND=robin`  
**Command:** `pytest tests/ --ignore=tests/archive -n 10 -v --tb=short`  
**Backend:** robin-sparkless (Rust/Polars engine)

---

## Summary

| Result  | Count |
|---------|-------|
| **PASSED**  | 867 |
| **FAILED**  | 1,640 |
| **SKIPPED** | 21 |
| **ERROR**   | 7 |
| **Total recorded** | **2,535** |

**Note:** The run was interrupted by a timeout before the full suite completed (2,545 tests were scheduled). The counts above are from the captured output; a small number of tests may not have finished.

---

## Result file

Full verbose output is stored in:

- **`tests/robin_mode_test_results.txt`** (5,077 lines)

---

## Errors (7)

All ERROR outcomes were in one file (setup or teardown failure):

- `tests/parity/dataframe/test_parquet_format_table_append.py`
  - `test_parquet_format_append_detached_df_visible_to_active_session`
  - `test_parquet_format_append_detached_df_visible_to_multiple_sessions`
  - `test_parquet_format_append_to_existing_table`
  - `test_parquet_format_append_to_new_table`
  - `test_parquet_format_multiple_append_operations`
  - `test_pipeline_logs_like_write_visible_immediately`
  - `test_storage_manager_detached_write_visible_to_session`

---

## Failure areas (sample)

Failed tests are spread across many modules. Representative areas:

- **Parity / DataFrame:** filter, join (inner/left/right/outer/semi/anti), select, set operations (union), transformations (distinct, orderBy, withColumn, limit), window (cume_dist, dense_rank, first_value, lag, etc.), parquet/table append persistence.
- **Integration:** case sensitivity (case_sensitive_*, case_insensitive_*, ambiguity_detection).
- **Examples:** `test_comparison`, `test_with_backend_info` in unified infrastructure example.

Many failures are expected while the Robin backend has limited op coverage (e.g. only a subset of operations are delegated to robin-sparkless; the rest fall back or fail).

---

## Passing areas (sample)

Tests that passed include:

- Documentation: `test_examples_show_v2_features`, `test_example_outputs_captured`
- Fixture compatibility: session creation, multiple sessions, context manager, cleanup, SparkContext
- Function API: `current_date`, `current_timestamp`, static methods, signatures
- Parity: cross_join, groupby, aggregations (sum, avg, count, min, max, multiple aggs, groupby multiple columns, global aggregation, nulls), grouped_data mean parity
- Unit: backend (robin optional), core (column resolver), and many others that either don’t require a full session or exercise paths supported by the Robin backend

---

## How to reproduce

```bash
# Ensure robin-sparkless is installed
pip install robin-sparkless

# Run all tests in Robin mode with 10 workers, verbose
SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python -m pytest tests/ --ignore=tests/archive -n 10 -v --tb=short 2>&1 | tee tests/robin_mode_test_results.txt
```

---

## Conclusion

- Robin mode is active for the run: session creation uses the Robin backend and fixtures assert `backend_type == "robin"`, so no tests silently used the Polars backend.
- A large proportion of tests fail or error under Robin, consistent with the current Robin backend supporting only a subset of operations (e.g. simple filter/select/limit in the PoC materializer); the rest either hit unsupported paths or fall back in ways that don’t match test expectations.
- Next steps for improving Robin mode pass rate: extend the Robin materializer and backend to support more operations and align behavior with the Polars backend where appropriate.
