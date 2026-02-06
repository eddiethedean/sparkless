# Robin Mode – Subset Test Report

**Date:** 2026-02-05  
**Configuration:** `SPARKLESS_TEST_BACKEND=robin`, `SPARKLESS_BACKEND=robin`  
**Scope:** Parity DataFrame subset (join, filter, aggregations, groupby, select, transformations, set operations, grouped_data_mean)  
**Command:**  
`SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin pytest tests/parity/dataframe/test_join.py tests/parity/dataframe/test_filter.py tests/parity/dataframe/test_aggregations.py tests/parity/dataframe/test_groupby.py tests/parity/dataframe/test_select.py tests/parity/dataframe/test_transformations.py tests/parity/dataframe/test_set_operations.py tests/parity/dataframe/test_grouped_data_mean_parity.py -v --tb=line`

---

## Summary

| Result   | Count |
|----------|-------|
| **PASSED**  | 22 |
| **FAILED**  | 17 |
| **SKIPPED** | 0  |
| **ERROR**   | 0  |
| **Total**   | **39** |

**Duration:** ~8.5 s

---

## Result file

Full output:

- **`tests/robin_mode_subset_results.txt`**

---

## Failed tests (17)

### Join (6)

| Test | Error |
|------|--------|
| `test_join.py::TestJoinParity::test_inner_join` | Row count mismatch: mock=0, expected=3 |
| `test_join.py::TestJoinParity::test_left_join` | Row count mismatch: mock=0, expected=4 |
| `test_join.py::TestJoinParity::test_right_join` | Row count mismatch: mock=0, expected=4 |
| `test_join.py::TestJoinParity::test_outer_join` | Row count mismatch: mock=0, expected=5 |
| `test_join.py::TestJoinParity::test_semi_join` | Row count mismatch: mock=0, expected=3 |
| `test_join.py::TestJoinParity::test_anti_join` | Row count mismatch: mock=0, expected=1 |

### Filter (5)

| Test | Error |
|------|--------|
| `test_filter.py::TestFilterParity::test_filter_operations` | Row count mismatch: mock=0, expected=2 |
| `test_filter.py::TestFilterParity::test_filter_with_boolean` | Row count mismatch: mock=0, expected=2 |
| `test_filter.py::TestFilterParity::test_filter_with_and_operator` | TypeError: '>' not supported between instances of 'builtins.Column' and 'builtins.Column' |
| `test_filter.py::TestFilterParity::test_filter_with_or_operator` | Same as above |
| `test_filter.py::TestFilterParity::test_filter_on_table_with_complex_schema` | Table read should return 3 rows (got 6) |

### Select (2)

| Test | Error |
|------|--------|
| `test_select.py::TestSelectParity::test_select_with_alias` | Row count mismatch: mock=0, expected=4 |
| `test_select.py::TestSelectParity::test_column_access` | Row count mismatch: mock=0, expected=4 |

### Transformations (4)

| Test | Error |
|------|--------|
| `test_transformations.py::TestTransformationsParity::test_with_column` | Row count mismatch: mock=0, expected=4 |
| `test_transformations.py::TestTransformationsParity::test_drop_column` | Row count mismatch: mock=0, expected=4 |
| `test_transformations.py::TestTransformationsParity::test_distinct` | Row count mismatch: mock=0, expected=3 |
| `test_transformations.py::TestTransformationsParity::test_order_by_desc` | Row count mismatch: mock=0, expected=4 |

---

## Passing areas (22 tests)

- **test_join.py:** `test_cross_join` (1)
- **test_aggregations.py:** all 11 (sum, avg, count, max, min, multiple aggs, groupby multiple columns, global aggregation, aggregation with nulls)
- **test_groupby.py:** `test_group_by`, `test_aggregation` (2)
- **test_grouped_data_mean_parity.py:** all 3 (single column, multiple columns, equals avg)
- **test_set_operations.py:** all 4 (union, unionByName, intersect, except)
- **test_select.py:** 1 other test passed (subset has 3 total, 2 failed)

---

## Failure themes

1. **Join returning 0 rows** – All join types (inner, left, right, outer, semi, anti) execute but Robin result has 0 rows; needs investigation in robin-sparkless `join`/`collect` or Sparkless Row conversion.
2. **Filter returning 0 rows or wrong count** – Simple filters (e.g. `age > 30`) yield mock=0; AND/OR tests hit `TypeError` (Column vs Column comparison in test/comparison path).
3. **Select/transformations returning 0 rows** – Select with alias, column access, withColumn, drop, distinct, orderBy desc all report mock=0.
4. **Column comparison TypeError** – Fixture or comparison code compares/orders `Column` objects; needs resolution to values or supported expressions (see [robin_improvement_plan.md](robin_improvement_plan.md)).

---

## How to reproduce

```bash
SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin pytest \
  tests/parity/dataframe/test_join.py \
  tests/parity/dataframe/test_filter.py \
  tests/parity/dataframe/test_aggregations.py \
  tests/parity/dataframe/test_groupby.py \
  tests/parity/dataframe/test_select.py \
  tests/parity/dataframe/test_transformations.py \
  tests/parity/dataframe/test_set_operations.py \
  tests/parity/dataframe/test_grouped_data_mean_parity.py \
  -v --tb=line 2>&1 | tee tests/robin_mode_subset_results.txt
```
