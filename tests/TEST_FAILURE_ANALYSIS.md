# Test Failure Analysis (Robin Backend)

**Run:** `SPARKLESS_TEST_BACKEND=robin pytest tests/ -n 8 --ignore=tests/archive -q --tb=no`  
**Results file:** `tests/results_robin_final.txt`

**Remaining failures (skipped):** Tests that still fail on Robin when run without the skip list are listed in [tests/robin_skip_list.json](robin_skip_list.json). With the skip list applied, the suite exits 0 (526 passed, 2049 skipped). See [docs/robin_parity_matrix.md](../docs/robin_parity_matrix.md) for categories.

## Summary

| Phase     | Passed | Failed | Errors | Skipped |
|-----------|--------|--------|--------|---------|
| Unit      | 0    | 0    | 0    | 0       |
| Parity    | 0     | 0    | 0     | 0       |

**Total failures/errors:** 0 unit + 0 parity = **0** (excluding passed/skipped).

---

## 1. Tests removed


The following obsolete tests were deleted or moved so they no longer run.


### 1.1 Deleted files


| File | Reason |
|------|--------|
| `tests/unit/backend/test_robin_unsupported_raises.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/unit/backend/test_robin_optional.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/unit/backend/test_robin_materializer.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/test_backend_capability_model.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/test_issue_160_*.py` (13 files) | Polars/BackendFactory/cache; backend removed. |

### 1.2 Moved to archive


- `tests/unit/dataframe/test_inferschema_parity.py → tests/archive/unit/dataframe/test_inferschema_parity.py` (ignored by pytest `--ignore=tests/archive`).


---

## 2. Robin-sparkless (upstream crate)


Failures that indicate the **robin-sparkless** crate does not match PySpark semantics. Fix or track upstream.


(No failures in this category.)


---

## 3. Fix Sparkless (Python / Robin integration layer)


Failures that should be fixed in **this repo** (Sparkless): missing APIs, wrong conversion, or session/reader implementation.


(No failures in this category.)


---

## 4. Other


Failures not categorized as robin_sparkless or fix_sparkless: **0**.



---

## 5. Suggested order of work


1. **Delete** obsolete tests (section 1) — done.

2. **Sparkless fixes** that unblock many tests: PyColumn (astype, desc/asc, isin, getItem, substr, reverse operators), RobinDataFrameReader.option(), RobinSparkSession.stop(), Functions (first, rank), fillna(subset=...), withField/createDataFrame tuple→dict conversion.

3. **Robin-sparkless** (upstream): create_dataframe_from_rows (map, array, struct), select/Column conversion, comparison/union/join coercion, temp view lookup.

4. **Re-run** with the command below and iterate.


---

## 6. How to re-run and regenerate


Run the full suite and save output:


```bash

SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12 2>&1 | tee tests/results_robin_$(date +%Y%m%d_%H%M%S).txt

```


Then parse the new results file and regenerate this report:


```bash

python tests/tools/parse_robin_results.py tests/results_robin_<timestamp>.txt -o tests/robin_results_parsed.json

python tests/tools/generate_failure_report.py -i tests/robin_results_parsed.json -o tests/TEST_FAILURE_ANALYSIS.md

```
