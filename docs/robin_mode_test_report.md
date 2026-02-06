# Robin Mode Test Run Report

**Date:** 2026-02-05  
**Configuration:** `SPARKLESS_TEST_BACKEND=robin`, `SPARKLESS_BACKEND=robin`  
**Command:** `pytest tests/ --ignore=tests/archive -n 10 -v --tb=short`  
**Backend:** robin-sparkless (Rust/Polars engine)

---

## Summary

| Result  | Count |
|---------|-------|
| **PASSED**  | 1,005 |
| **FAILED**  | 1,505 |
| **SKIPPED** | 21 |
| **ERROR**   | 0 |
| **Total recorded** | **2,531** |

**Note:** The run was interrupted by a 10-minute timeout before the full suite completed (2,545 tests scheduled). Counts are from the captured output; the last ~14 tests may not have finished. Join parity tests use `@pytest.mark.timeout(60)` so they no longer stall the run; the interrupt occurred after test_outer_join and remaining tests were not executed.

---

## Result files

- **`tests/robin_mode_test_results.txt`** — Full verbose pytest output (tee of stdout/stderr).
- **`tests/robin_mode_failed_tests.txt`** — One line per failed test (unique test IDs).

---

## Errors

This run had **0** ERRORs. Previously, seven parquet teardown ERRORs were fixed (Robin storage `db_path`).

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

# Recommended: use --no-cov and --dist loadfile to avoid stall at 99% with parallel workers
SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python -m pytest tests/ --ignore=tests/archive -n 10 --dist loadfile -v --tb=short --no-cov 2>&1 | tee tests/robin_mode_test_results.txt
```

**Note:** Without `--no-cov`, the run can stall at ~99% when pytest-cov combines coverage from workers. Use `--no-cov` for reliable completion. For runs with coverage, use a process timeout (e.g. `bash tests/run_with_timeout.sh 1800 python -m pytest ...`).

---

## Conclusion

- Robin mode is active for the run: session creation uses the Robin backend and fixtures assert `backend_type == "robin"`, so no tests silently used the Polars backend.
- A large proportion of tests fail under Robin, consistent with the current Robin backend supporting only a subset of operations; the rest hit unsupported paths or fall back in ways that don’t match test expectations.
- This run had no ERRORs (parquet teardown fixed). Next steps for improving Robin mode pass rate: extend the Robin materializer and backend to support more operations and align behavior with the Polars backend where appropriate.
