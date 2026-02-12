# v4 Behavior Changes and Known Differences

This document summarizes deliberate behavior changes in Sparkless v4 (Robin-only backend) and known differences between v3 and v4 / Robin engine semantics. It is the single source of truth for Phase 3 compatibility; see also [migration_v3_to_v4.md](migration_v3_to_v4.md) for user-facing migration.

## Deliberate behavior changes

- **Backend**: Only the Robin backend is supported. `SPARKLESS_BACKEND` and `backend_type` may not be set to `polars`, `memory`, `file`, or `duckdb`; they raise `ValueError`. Default is `robin`.
- **list_available_backends()**: Returns `["robin"]` only (no conditional exclusion of Robin when unavailable).
- **Schema inference (Reader)**: When no schema is provided (e.g. CSV with `inferSchema`), the v4 reader infers all columns as `StringType`. Numeric/boolean/date inference is not implemented (Phase 3 gap; was previously provided by Polars).
- **Storage and I/O**: Catalog and table persistence use file-based storage; Parquet/CSV/JSON use row-based and pandas where needed. Polars is not used.

## Known differences (Robin semantics)

These are cases where Robin-backed execution behaves differently from v3 (Polars) or where certain operations/expressions are not supported. Robin-sparkless's goal is to fully emulate PySpark (Rust-first); Sparkless fills Python-specific gaps. **Any difference vs PySpark (unsupported or different behavior) must be reported upstream** in [robin-sparkless issues](https://github.com/eddiethedean/robin-sparkless/issues); see [robin_sparkless_issues.md](robin_sparkless_issues.md) for division of responsibility, policy, and how to file. Tests that rely on these may fail under the default (Robin) backend.

### Unsupported operation

- **Operations not in Robin materializer**: The Robin materializer supports a fixed set of operations (filter, select, limit, orderBy, withColumn, join, distinct, drop, union). Any other operation (e.g. aggregate in a way that is not groupBy+agg, or operations not yet implemented) causes `SparkUnsupportedOperationError` with "Backend 'robin' does not support these operations".
- **Join (Robin)**: Join is executed via the op path only (the Robin plan path does not support join). Same-name join keys are supported: `on='col'`, `on=['c1','c2']`, or `left['id'] == right['id']`. Different-name join keys (e.g. `left['id'] == right['id_right']`) are supported via an internal temp join key that is dropped after the join. See [#473](https://github.com/eddiethedean/sparkless/issues/473) for earlier discussion.

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
- **Split**: Split can return a column whose length does not match DataFrame height, causing "lengths don't match: unable to add a column of length N to a DataFrame of height 1" when used in `withColumn`. Split with limit (third argument) is not supported (Robin 2-arg only); see [robin_sparkless_issues.md](robin_sparkless_issues.md) and issue #254. Parity tests that rely on split limit or split result shape may fail or need to be skipped for Robin.
- **Posexplode / explode (array columns)**: Robin's `create_dataframe_from_rows` does not accept array/list in the schema; array columns are sent as string type, so posexplode/explode raise "invalid series dtype: expected `List`, got `str`". Use array columns only when the engine supports list/array schema, or skip affected tests for Robin.

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
| Behavioral (split) | split return length ≠ row count; split with limit not supported | test_split_limit_parity, test_issue_328_split_limit; "lengths don't match" |
| Behavioral (posexplode/array) | array columns sent as string; posexplode expects List | test_issue_366_alias_posexplode, test_issue_429/430 posexplode; "expected List, got str" |
| Backend config (fixed in Phase 3) | tests that requested polars | test_approx_count_distinct_rsd and test_logical_plan now use robin or skip (v4 Robin-only). |

Full failure list: see `tests/phase3_robin_unit_failures.txt` (generated by `pytest tests/unit/ -v --tb=line -m "not performance"` with default Robin backend).

## Decisions (shim vs accept-and-document)

- **Unsupported select/withColumn/filter expressions (cast, CaseWhen, window, getItem, map/array)**: **Accept and document**. No shim in Phase 3; documented above and in migration guide. Tests that exclusively exercise these may be skipped with reason referencing this doc.
- **Type strictness (string vs numeric)**: **Accept and document**. Robin semantics; no coercion shim in Phase 3.
- **map() / array() / nested row values**: **Accept and document**. Robin API/contract limitation.
- **Column name case / expression resolution**: **Accept and document**. Engine semantics.
- **Error message ("Consider using a different backend (e.g. polars)")**: **Shim**. Normalize to v4-appropriate message (Robin is the only backend; suggest checking docs for supported operations).

## Phase 6: Skipped tests (Robin-only run)

When running unit tests with **Robin** as the backend (`SPARKLESS_TEST_BACKEND=robin`), a set of tests are **skipped** (not run) because they exercise operations or semantics that are out of scope for v4. This keeps the test run green (no failures) while documenting what is not yet supported.

- **Authoritative list**: [tests/unit/v4_robin_skip_list.txt](../tests/unit/v4_robin_skip_list.txt) — one pattern per line; each pattern is matched against the pytest test nodeid. A `pytest_collection_modifyitems` hook in [tests/conftest.py](../tests/conftest.py) applies the skips when `SPARKLESS_TEST_BACKEND=robin`.
- **Skip reason**: `"v4 Robin: out of scope (unsupported expression/operation); see docs/v4_behavior_changes_and_known_differences.md"`.

**Categories** (summary; see the skip list file and the Phase 3 failure catalog above for full coverage):

| Category | Description |
|----------|-------------|
| cast/astype | `test_column_astype.py` — cast in select/withColumn/filter not supported |
| substr/substring | `test_column_substr.py` — substring expression not supported |
| CaseWhen / window / cast | `test_casewhen_windowfunction_cast.py` — CaseWhen, window, cast |
| chained arithmetic | `test_chained_arithmetic.py` — expressions Robin cannot translate |
| inferSchema | `test_inferschema_parity.py` — v4 reader string-only inference |
| join/union type coercion | `test_join_type_coercion.py`, `test_union_type_coercion.py` — type strictness |
| logical plan (Polars/config) | Two tests in `test_logical_plan.py` (Phase 2/4 Polars-specific) |
| na_fill, string arithmetic | `test_na_fill.py`, `test_string_arithmetic.py` — coercion/expressions |
| nested struct/row | `test_withfield.py` — row values must be scalar types |
| window / UDF / SQL | Various functions and session tests — window, UDF, or SQL features |
| map/array/case/issue tests | `test_create_map.py`, `test_issues_225_231.py`, `test_issue_355.py`, etc. |
| struct / isnan parity | `test_issue_289_struct_function.py`, `test_issue_263_isnan_string.py` — struct result shape/field names differ; Robin does not support isnan on string columns (Polars raises; PySpark returns False). |

Adding or changing unit tests that rely on unsupported features may require updating the skip list so that `SPARKLESS_TEST_BACKEND=robin` runs remain green.

## Phase 7: Expression coverage (supported vs unsupported)

The Robin materializer (and, where noted, the plan path) was extended in Phase 7 to support additional expressions. The following summarizes what is supported and what remains a gap.

**Supported in Phase 7**

| Expression | Materializer | Plan path | Notes |
|------------|--------------|-----------|--------|
| **cast / astype** | Yes | Yes (alias + cast) | Type name mapped to Robin dtype; some `test_column_astype` tests un-skipped. |
| **substring / substr** | Yes | No | `F.substring` / `F.substr` fallback; `test_column_substr` skipped (result column naming may differ). |
| **getItem** | Yes | No | `column[key]` for array index or map key; `test_issues_225_231` still skipped (isin/coercion). |
| **struct** | Yes | No | `F.struct("a", "b")` and struct with string names translated; Robin struct result shape/field names may differ — tests skipped. |
| **isnan** | Yes | No | Expression and filter; Robin/Polars does not support isnan on string columns (raises); PySpark returns False — tests skipped. |

**Remaining unsupported (documented gaps)**

| Expression / feature | Notes |
|----------------------|--------|
| **CaseWhen (when/otherwise)** | Robin API not verified; tests remain skipped. |
| **Window in select** | Out of scope for Phase 7; document as gap. |
| **map() / array() / nested struct** | Robin row values must be scalar; see Phase 3 catalog. |
| **Chained arithmetic with unsupported sub-expressions** | e.g. "(a * 2)" resolution; high-value cases may be addressed later. |

Workarounds: use supported expressions only in select/withColumn/filter; for CaseWhen/window, consider pre-aggregation or Python-side logic; see [sparkless_v4_roadmap.md](sparkless_v4_roadmap.md) §7.7.1.

## References

- [sparkless_v4_roadmap.md](sparkless_v4_roadmap.md) §7.3, §7.3.1, §7.6, §7.6.1
- [migration_v3_to_v4.md](migration_v3_to_v4.md)
- [backend_selection.md](backend_selection.md)
