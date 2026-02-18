# Robin (crate) unsupported expression ops and functions

**As of robin-sparkless 0.11.5** the issues described below are fixed; the skip markers that referenced this doc have been removed. The following is kept as historical reference.

This document listed expression operations and functions that the robin-sparkless crate did not yet support. Tests that rely on these are skipped when running with the Robin backend (`SPARKLESS_TEST_BACKEND=robin` or session `backend_type == "robin"`).

## Unsupported expression ops

| Op / shape | Crate error / behavior | Skip / notes |
|------------|------------------------|--------------|
| `create_map` | unsupported expression op: create_map | Tests using `F.create_map(...)` are skipped on Robin. |
| `getItem` | unsupported expression op: getItem | Tests using `col.getItem(...)` (e.g. array/map subscript) are skipped on Robin. |
| `regexp_extract` | unsupported expression op: regexp_extract | Tests using `F.regexp_extract` are skipped on Robin. |
| `startswith` | unsupported expression op: startswith | Tests using `F.col(...).startswith(...)` are skipped on Robin. |
| `is_null` / `is_not_null` | unsupported expression op: is_null (or unsupported function: is_null) | Tests that materialize plans containing `isnull()` / `isNotNull()` are skipped on Robin. |

## Unsupported function

| Function | Crate error / behavior | Skip / notes |
|----------|------------------------|--------------|
| `isin` | unsupported function: isin (or unsupported expression op when emitted as op) | We emit `isin` as op with left/right; if crate does not support it, tests using `col.isin(...)` are skipped on Robin. |

## Window expressions in select

| Shape | Crate error / behavior | Skip / notes |
|-------|------------------------|--------------|
| Window in select (e.g. `row_number().over(...) + 10`) | expression must have 'col', 'lit', 'op', or 'fn' | Crate does not accept `type: "window"` in expression tree. Plan adapter converts window to opaque; tests that use window functions in select (e.g. `tests/unit/test_window_arithmetic.py`, `tests/test_issue_336_window_function_comparison.py`) are skipped on Robin. |

## Python-side behavior

- **Plan adapter** (`sparkless/robin/plan_adapter.py`): Converts Sparkless logical plan expressions to Robin shape. Unsupported ops are passed through; the crate returns a clear error.
- **Skip markers**: Use `get_backend_type() == BackendType.ROBIN` or runtime check `getattr(spark, "backend_type", None) == "robin"` to skip tests that require these ops.
- **Parity**: See `docs/robin_parity_from_skipped_tests.md` for broader parity gaps (e.g. parquet empty-schema, window functions).

## Join and union row counts

When executing join or union via the crate, result row counts can be wrong (e.g. join returns 0 rows instead of 2, union returns only left side rows instead of left+right). Sparkless sends correct `other_data` and `other_schema` in the plan; the crate is responsible for merging. Tests that assert join/union row counts (e.g. `tests/unit/test_issue_270_tuple_dataframe.py::test_tuple_data_join_operations`, `test_tuple_data_union_operations`) are skipped on Robin.

## Case sensitivity and column-not-found

Errors like `not found: Column 'value'`, `not found: ID`, `Available columns: [id, name, value]` can occur when:

- The plan or a later step references a column name that does not match the schema exactly (e.g. case difference: `ID` vs `id`).
- Robin/crate may treat column names as case-sensitive; Sparkless preserves the exact names from the logical plan and schema and does not lower/upper when adapting.

**Recommendation:** Use consistent column name casing in schema and expressions. If you see column-not-found on Robin, verify the name matches the schema (including case). Tests that assert case-insensitive resolution (e.g. `tests/unit/test_column_case_variations.py`) are skipped on Robin.

## Adding support

When the crate gains support for an op or function, update the plan adapter if a different payload shape is required, remove the corresponding skip markers from tests, and update this doc.
