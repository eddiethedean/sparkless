# Improving Sparkless for Robin-Sparkless

This document lists concrete ways to improve Sparkless so it works better with the Robin backend (robin-sparkless). It is based on the [Robin mode failure analysis](robin_mode_failure_analysis.md) and [test report](robin_mode_test_report.md).

---

## 1. Summary

| Area | Priority | Owner | Status / notes |
|------|----------|--------|----------------|
| Join: support Column expression as `on` | High | Sparkless | **Done** – materializer accepts `col == col` and extracts join keys; Robin path no longer raises unsupported. Join result is 0 rows → see “Join result” below. |
| Join: fix 0-row result (inner/left/right/outer) | High | Sparkless / robin-sparkless | Robin materializer runs join but returns 0 rows; debug robin `join(on=..., how=...)` and/or our `collect()` → Row conversion. |
| Filter: support AND/OR and more expressions | High | Sparkless | **Done** – `_simple_filter_to_robin` handles `ColumnOperation("&", ...)` and `ColumnOperation("\|", ...)` recursively. |
| groupBy + agg in Robin materializer | High | Sparkless | Add `groupBy` (and agg) to `SUPPORTED_OPERATIONS` and translate to robin_sparkless `group_by` + GroupedData aggregations. |
| Semi/anti join support in Robin | Medium | Sparkless / robin-sparkless | **Done** – `_can_handle_join` and materialize accept `left_semi` / `left_anti`; robin may support or raise at runtime. |
| Column comparison / TypeError in fixtures | Medium | Sparkless | Avoid comparing or ordering raw `Column` objects in assertion/sort helpers; resolve to values or use supported expressions. |
| Parquet/table append semantics for Robin | Medium | Sparkless | If append/visibility under Robin storage is wrong, fix storage/session logic for the Robin delegate. |
| Test/fixture expectations (robin in backend list) | Low | Sparkless | **Done** – include `'robin'` where tests assume backends (e.g. unified infrastructure example). |
| Robin storage `db_path` | Low | Sparkless | **Done** – added so teardown no longer raises. |

---

## 2. Implemented

- **Robin storage `db_path`** – `RobinStorageManager` exposes `db_path`; parquet teardown ERRORs are fixed.
- **Arbitrary schema** – Materializer uses `create_dataframe_from_rows(data, schema)` and `_spark_type_to_robin_dtype` for any schema.
- **orderBy, withColumn, join, union** – Materializer supports these; join accepts both string/list and Column expression (`col == col`) via `_join_on_to_column_names()`.
- **Join on Column expression** – `_can_handle_join` and join materialization use `_join_on_to_column_names(on)` so `df.join(other, df.a == other.a, "inner")` is handled by Robin instead of raising unsupported.
- **Filter AND/OR** – `_simple_filter_to_robin` handles `ColumnOperation("&", left, right)` and `ColumnOperation("|", left, right)` recursively so combined conditions are translated to Robin.
- **Semi/anti join** – `_can_handle_join` accepts `left_semi`, `left_anti`, `semi`, `anti`; materialize passes them through to robin (robin may support or raise at runtime).
- **Join Row conversion** – Materializer uses `SchemaManager.project_schema_with_operations` to build a final schema and passes it to `Row(d, schema=final_schema)` so column order and duplicate names after join are correct.
- **Tests/fixtures** – Backend list includes `'robin'` where needed (e.g. unified infrastructure example).

---

## 3. Recommended next steps (in order)

### 3.1 Fix join result (0 rows)

Robin join path runs but returns 0 rows for inner/left/right/outer.

- **Sparkless:** Check how we convert `df.collect()` to `List[Row]` (column names, schema, duplicate names after join). Ensure we’re not dropping or mis-mapping columns.
- **robin-sparkless:** Verify `join(other, on=["dept_id"], how="inner")` with two DataFrames that both have `dept_id` returns the expected rows; if not, report upstream or adapt our call (e.g. left_on/right_on if the API supports it).

### 3.2 Extend filter translation (AND/OR)

Many failures are `SparkUnsupportedOperationError` for filter.

- In `sparkless/backend/robin/materializer.py`, extend `_simple_filter_to_robin` (or add a wrapper) to handle:
  - `ColumnOperation("&", left, right)` → `_simple_filter_to_robin(left) and _simple_filter_to_robin(right)` (and map to robin’s `&` or chained `.filter()`).
  - `ColumnOperation("|", left, right)` → same for OR.
- Recursively support nested AND/OR so more real-world filters are handled by Robin.

### 3.3 Add groupBy + agg to Robin materializer

- Add `"groupBy"` to `RobinMaterializer.SUPPORTED_OPERATIONS`.
- In the operations queue, groupBy is typically followed by a GroupedData agg; the payload may be `(group_by_columns, agg_exprs)` or similar (see how Polars materializer receives it).
- Translate to robin_sparkless: `df.group_by([...]).agg(...)` (or equivalent GroupedData API). Start with simple aggs (count, sum, min, max, avg).

### 3.4 Semi/anti join

- Check robin_sparkless API for left_semi / left_anti (or equivalent). If present, add `_can_handle_join` for `how in ("left_semi", "left_anti")` and translate in the join branch.
- If absent, keep raising unsupported for semi/anti or document as limitation.

---

## 4. Code locations

| Change | File(s) |
|--------|--------|
| Join on expression, filter AND/OR, groupBy | `sparkless/backend/robin/materializer.py` |
| Column comparison in tests | `tests/fixtures/comparison.py` (and any assertion helpers that compare Column objects) |
| Robin storage | `sparkless/backend/robin/storage.py` |

---

## 5. How to validate

- Run full suite in Robin mode and compare pass/fail counts to the last report:
  ```bash
  SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin pytest tests/ --ignore=tests/archive -n 10 --dist loadfile -v --tb=short 2>&1 | tee tests/robin_mode_test_results.txt
  ```
- Run only parity join tests to confirm join path and row counts:
  ```bash
  SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin pytest tests/parity/dataframe/test_join.py -v --tb=short
  ```
- After adding filter AND/OR or groupBy, run filter/groupby parity tests in Robin mode and check that they pass or fail with a clear, non-unsupported error.

---

## 6. robin-sparkless (upstream)

Robin-sparkless already exposes the APIs we need (arbitrary schema, filter, select, with_column, order_by, join, union, group_by, GroupedData). No upstream feature request is required for “more operations” or “flexible schema.” Concrete bugs (wrong result, wrong row count, missing behavior) should be reported upstream with a minimal repro; see [robin_sparkless_issues.md](robin_sparkless_issues.md).
