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
   - **Why:** Many parity and unit tests use `df.groupBy(...).agg(...)` or `count()`, `sum()`, etc. Robin materializer does not list `groupBy` in `SUPPORTED_OPERATIONS`, so those tests raise `SparkUnsupportedOperationError`.  
   - **What:** Add `"groupBy"` to `SUPPORTED_OPERATIONS`, and in the materialize loop handle the groupBy op plus the subsequent agg (see how the operations queue encodes groupBy + GroupedData.agg in lazy.py / Polars materializer). Translate to robin_sparkless `df.group_by([...]).agg(...)` (or equivalent).  
   - **Files:** `sparkless/backend/robin/materializer.py`, and possibly `logical_plan` / queue shape for groupBy+agg.

2. **Document Robin-supported operations and limits**  
   - **Why:** Users and contributors need to know what works in Robin mode and what fails fast.  
   - **What:** Add a short “Robin backend” section to [backend_selection.md](backend_selection.md) (or a dedicated `docs/robin_backend.md`) listing: supported ops (filter, select by name, limit, orderBy, withColumn, join, union), unsupported (select with expressions, groupBy until implemented, UDF, window, etc.), and the worker-crash workaround (use `-n 4` or `-n 0`).  
   - **Files:** `docs/backend_selection.md` and/or `docs/robin_backend.md`.

### Medium priority

3. **Join result: 0 rows**  
   - **Why:** Robin join path runs but some tests see 0 rows for inner/left/right/outer.  
   - **What:** Debug conversion from `df.collect()` to `List[Row]` (column names, schema, duplicate handling). If our conversion is correct, compare with robin-sparkless `join(on=..., how=...)` behavior and open or reference a robin-sparkless issue if the bug is upstream.  
   - **Files:** `sparkless/backend/robin/materializer.py` (collect → merged → Row), and robin-sparkless API usage.

4. **Extend withColumn expression translation**  
   - **Why:** `_expression_to_robin()` supports only a subset of expressions; more withColumn tests would pass if we translated more ops (e.g. arithmetic, simple functions).  
   - **What:** Incrementally extend `_expression_to_robin()` for expressions that robin-sparkless supports (check their API). Prefer clear “unsupported” for complex expressions rather than incorrect translation.

### Low priority / optional

5. **Robin worker-crash note in backend_selection**  
   - **Why:** Users who see “node down” or INTERNALERROR in Robin mode need a quick pointer.  
   - **What:** In backend_selection “Troubleshooting” or “Robin backend”, add one line: “If you see worker crashes or INTERNALERROR when running with many workers, use fewer workers (e.g. `-n 4`) or serial (`-n 0`); see [robin_mode_worker_crash_investigation.md](robin_mode_worker_crash_investigation.md).”

6. **Fallback to Polars when Robin can’t handle an op**  
   - **Why:** Tests could run (with Polars) instead of failing with SparkUnsupportedOperationError in Robin mode.  
   - **Trade-off:** Hides Robin gaps and changes semantics (result may differ from “pure” Robin). Prefer only behind a config flag (e.g. `spark.sparkless.robin.fallbackToPolars=true`) and document that it’s for development/CI, not for asserting Robin behavior.

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
| filter | Yes | Simple col op literal; AND/OR. Method API (.gt, .lt, …). |
| select | Partial | Only list of column names (strings). No Column expressions. |
| limit | Yes | Non-negative int. |
| orderBy | Yes | Column names + optional ascending. |
| withColumn | Partial | Only expressions translatable by `_expression_to_robin`. |
| join | Yes | on= list or extracted from col==col; how= inner/left/right/outer/semi/anti. |
| union | Yes | |
| groupBy + agg | **No** | Recommended to add (high priority). |
