# v4 Behavior Changes and Known Differences

This document summarizes deliberate behavior changes in Sparkless v4 (Robin-only backend) and known differences between v3 and v4 / Robin engine semantics. It is the single source of truth for Phase 3 compatibility; see also [migration_v3_to_v4.md](migration_v3_to_v4.md) for user-facing migration.

## Deliberate behavior changes

- **Backend**: Only the Robin backend is supported. `SPARKLESS_BACKEND` and `backend_type` may not be set to `polars`, `memory`, `file`, or `duckdb`; they raise `ValueError`. Default is `robin`.
- **list_available_backends()**: Returns `["robin"]` only (no conditional exclusion of Robin when unavailable).
- **Schema inference (Reader)**: When no schema is provided (e.g. CSV with `inferSchema`), the v4 reader infers all columns as `StringType`. Numeric/boolean/date inference is not implemented (Phase 3 gap; was previously provided by Polars).
- **Storage and I/O**: Catalog and table persistence use file-based storage; Parquet/CSV/JSON use row-based and pandas where needed. Polars is not used.

## Known differences (Robin semantics)

These are cases where Robin-backed execution behaves differently from v3 (Polars) or where certain operations/expressions are not supported. Tests that rely on these may fail under the default (Robin) backend.

### Unsupported operation

- **Operations not in Robin materializer**: The Robin materializer supports a fixed set of operations (filter, select, limit, orderBy, withColumn, join, distinct, drop, union). Any other operation (e.g. aggregate in a way that is not groupBy+agg, or operations not yet implemented) causes `SparkUnsupportedOperationError` with "Backend 'robin' does not support these operations".

### Unsupported expression (select / withColumn / filter)

- **select**: If any selected expression cannot be translated to Robin (e.g. cast, CaseWhen, window function, getItem, map/array literals, compound expressions), `can_handle_select` is False or materialization raises `SparkUnsupportedOperationError` ("Operation 'Operations: select' is not supported").
- **filter**: Filter conditions that use expressions Robin does not support (e.g. `isin([])`, column-vs-column comparison in some forms, or complex expressions) cause "Operation 'Operations: filter' is not supported" or a Robin `RuntimeError`.
- **withColumn**: Same as select: unsupported expression types (cast, CaseWhen, window, map/array, etc.) cause unsupported operation or Robin runtime errors.

### Behavioral / semantic (Robin engine)

- **Type strictness**: Robin does not allow comparing string with numeric (e.g. "cannot compare string with numeric type (i32)"). PySpark/v3 may coerce; v4 with Robin does not.
- **map() / array()**: Robin engine may not expose `map()` or `array()` in the same way; "not found: map()" or "not found: array(...)" indicate missing or different API.
- **Row values**: `create_dataframe_from_rows` / plan execution expects row values to be None, int, float, bool, or str. Nested structs/lists/dicts may raise "row values must be None, int, float, bool, or str".
- **Column name case**: Robin may be case-sensitive where Sparkless/v3 allowed case-insensitive resolution ("not found: ID" / "not found: age").
- **Arithmetic / expressions**: Some expressions (e.g. "(id % 2)", "value1 * 2 + value2") may fail with "not found" or "arithmetic on string and numeric not allowed" when column types or names differ from what Robin expects.

## Phase 3 failure catalog (summary)

Unit test run with default backend (Robin) yields a large number of failures, grouped as follows:

| Category | Description | Representative test paths / error |
|----------|-------------|-----------------------------------|
| Unsupported expression (select) | select with cast, CaseWhen, window, getItem, map/array, etc. | test_column_astype, test_casewhen_windowfunction_cast, test_window_arithmetic, test_create_map, test_issues_225_231 (getItem), many others; "Operations: select is not supported" |
| Unsupported expression (filter) | filter with isin([]) or unsupported condition | test_issue_355, test_issues_225_231 (isin); "Operations: filter is not supported" |
| Unsupported expression (withColumn) | withColumn with window/compound/map/array | test_issue_355, test_create_map; "Operations: withColumn is not supported" or RuntimeError from Robin |
| Behavioral (type strictness) | string vs numeric comparison | test_issues_225_231 (string_eq_numeric, coercion); RuntimeError "cannot compare string with numeric type" |
| Behavioral (map/array/struct) | map(), array(), or nested row values | test_create_map, test_array_parameter_formats, test_withfield; "not found: map()" or "row values must be None, int, float, bool, or str" |
| Behavioral (column names / expr) | case sensitivity, expression resolution | test_issue_355 (unionByName + filter/expr), test_issues_225_231 (case insensitive); "not found: ID" / "not found: (id % 2)" |
| Backend config (fixed in Phase 3) | tests that requested polars | test_approx_count_distinct_rsd and test_logical_plan now use robin or skip (v4 Robin-only). |

Full failure list: see `tests/phase3_robin_unit_failures.txt` (generated by `pytest tests/unit/ -v --tb=line -m "not performance"` with default Robin backend).

## Decisions (shim vs accept-and-document)

- **Unsupported select/withColumn/filter expressions (cast, CaseWhen, window, getItem, map/array)**: **Accept and document**. No shim in Phase 3; documented above and in migration guide. Tests that exclusively exercise these may be skipped with reason referencing this doc.
- **Type strictness (string vs numeric)**: **Accept and document**. Robin semantics; no coercion shim in Phase 3.
- **map() / array() / nested row values**: **Accept and document**. Robin API/contract limitation.
- **Column name case / expression resolution**: **Accept and document**. Engine semantics.
- **Error message ("Consider using a different backend (e.g. polars)")**: **Shim**. Normalize to v4-appropriate message (Robin is the only backend; suggest checking docs for supported operations).

## References

- [sparkless_v4_roadmap.md](sparkless_v4_roadmap.md) §7.3, §7.3.1
- [migration_v3_to_v4.md](migration_v3_to_v4.md)
- [backend_selection.md](backend_selection.md)
