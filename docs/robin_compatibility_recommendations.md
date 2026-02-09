# Sparkless changes to work better with robin-sparkless

This document lists **concrete Sparkless-side changes** that improve Robin backend compatibility. It draws on [robin_sparkless_ownership_analysis.md](robin_sparkless_ownership_analysis.md), [robin_improvement_plan.md](robin_improvement_plan.md), and the worker-crash investigation.

---

## Already done

| Change | Notes |
|--------|--------|
| Filter: use method API (`.gt()`, `.lt()`, etc.) | `_simple_filter_to_robin()` uses robin Column methods instead of Python operators. |
| Filter: AND/OR | `ColumnOperation("&", ...)` and `ColumnOperation("\|", ...)` supported recursively. |
| Join: on= expression | `_join_on_to_column_names()` extracts keys from `col == col`; join supported. |
| Join: semi/anti | `can_handle_join` accepts left_semi, left_anti; materialize passes through. |
| Robin storage `db_path` | Parquet teardown ERRORs fixed. |
| Fewer workers default in Robin mode | `run_all_tests.sh` uses 4 workers when `BACKEND=robin` to reduce worker crashes. |
| Skip `test_regexp_extract_all_*` in Robin | Test skipped; robin-sparkless doesn’t support select with that expression (issue #176). |
| Polars: regex in closure | `regexp_extract_all` compiles regex inside closure to avoid hang under xdist. |

---

## Recommended Sparkless changes (by priority)

### High priority

1. **Add groupBy + agg to Robin materializer**  
   - **Status:** Not implemented. In the current design, `groupBy` never appears in a DataFrame’s `_operations_queue`; `GroupedData.agg()` materializes the parent DataFrame (via Robin when applicable) and then runs aggregation in Python. So typical `df.filter(...).groupBy("a").count()` already works with Robin (filter is materialized by Robin, then group+count run in Sparkless). Adding groupBy to the Robin materializer would only matter if a future path put groupBy in the queue.  
   - **What (if needed):** Add `"groupBy"` to `SUPPORTED_OPERATIONS` and in the materialize loop translate to robin_sparkless `df.group_by([...]).agg(...)` when the queue contains groupBy.

2. **Document Robin-supported operations and limits**  
   - **Status:** Done. [backend_selection.md](backend_selection.md) has a “Robin backend” section; [robin_compatibility_recommendations.md](robin_compatibility_recommendations.md) lists supported/unsupported ops and the worker-crash workaround.

### Medium priority

3. **Join result: 0 rows**  
   - **Status:** Not changed. Conversion from `collect()` to `List[Row]` (and `_right` merge) is in place; if Robin still returns 0 rows for some joins, the cause is likely in robin-sparkless or in the join keys we pass. Debug with a minimal repro and report upstream if needed.  
   - **Files:** `sparkless/backend/robin/materializer.py` (collect → merged → Row).

4. **Extend withColumn expression translation**  
   - **Status:** Done. `_expression_to_robin()` now supports: **alias** (e.g. `col.alias("x")`), **commutative ops** with `Literal` on the left (e.g. `lit(2) + col("x")`), and the same comparison/arithmetic ops as before.  
   - **Files:** `sparkless/backend/robin/materializer.py`.

### Low priority / optional

5. **Robin worker-crash note in backend_selection**  
   - **Why:** Users who see “node down” or INTERNALERROR in Robin mode need a quick pointer.  
   - **What:** In backend_selection “Troubleshooting” or “Robin backend”, add one line: “If you see worker crashes or INTERNALERROR when running with many workers, use fewer workers (e.g. `-n 4`) or serial (`-n 0`); see [robin_mode_worker_crash_investigation.md](robin_mode_worker_crash_investigation.md).”

6. **Pure Robin mode (no Polars fallback)**  
   - **Status:** Done. When Robin is selected, Sparkless runs in pure Robin mode only. Unsupported operations raise `SparkUnsupportedOperationError`. Use the Polars backend for full operation coverage.

---

## What to leave to robin-sparkless

- **select() with Column expressions** (e.g. `regexp_extract_all`) – robin-sparkless issue #176.  
- **Operator overloads on Column** (`col > lit`) – robin-sparkless already has an issue; we use method API.  
- **join(on="col")** – single-column string; we pass list; robin-sparkless issue exists.  
- **Worker crashes under pytest-xdist** – robin-sparkless issue #178 (fork-safety / stability).

No Sparkless code change is required for these; we work around or skip where needed and track upstream fixes.

---

## Quick reference: current Robin materializer support

| Operation | Supported | Notes |
|-----------|-----------|--------|
| filter | Yes | Simple col op literal; AND/OR; isin (translated to OR of equality). Method API (.gt, .lt, …). |
| select | Partial | Only list of column names (strings). No Column expressions. |
| limit | Yes | Non-negative int. |
| orderBy | Yes | Column names + optional ascending. |
| withColumn | Partial | Only expressions translatable by `_expression_to_robin`. |
| join | Yes | on= list or extracted from col==col; how= inner/left/right/outer/semi/anti. |
| union | Yes | |
| distinct | Yes | Deduplicate rows. |
| drop | Yes | Drop column(s) by name. |
| groupBy + agg | **No** | Recommended to add (high priority). |
