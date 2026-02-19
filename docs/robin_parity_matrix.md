# Robin–Sparkless / PySpark Parity Matrix

This document is the single place for **Robin-sparkless vs PySpark parity gaps** and **Sparkless-side fixes**. Tests that fail due to Robin parity are skipped when `SPARKLESS_TEST_BACKEND=robin` via [tests/robin_skip_list.json](../tests/robin_skip_list.json).

See also: [upstream.md](upstream.md), [robin_parity_from_skipped_tests.md](robin_parity_from_skipped_tests.md), and `docs/robin_github_issue_*.md` for detailed upstream issue write-ups.

---

## 1. Robin–PySpark parity gaps (upstream crate)

| Short name | Description | Example test(s) | Upstream / notes |
|------------|-------------|------------------|------------------|
| create_dataframe_from_rows (array) | Array column value must be null or array; nested/triple nested/map/struct | test_getItem_with_array_index, test_array_type_elementtype_* | Crate: fix row/schema handling for arrays |
| create_dataframe_from_rows (map) | Unsupported type map&lt;string,string&gt; | test_getItem_with_map_key | Crate: support map type in from_rows |
| create_dataframe_from_rows (struct) | Struct value must be object or array | test_nested_struct_field_access_all_cases, test_first_with_nested_struct | Crate: struct conversion |
| create_dataframe_from_rows (schema empty) | Schema must not be empty when rows are not empty | test_filter_with_and_operator, test_append_* | Crate / Sparkless schema path |
| select expects Column or str | select() with column expressions rejected | test_create_map_with_literals, test_create_map_* | Crate: accept column expressions |
| cannot convert to Column | Array/column conversion in select | test_array_with_string_columns, test_array_with_list_of_strings (+43) | Crate: Column semantics |
| Table or view not found (SQL) | Temp view / table not found after createOrReplaceTempView or table() | test_basic_select, test_robin_sql_simple_select, test_robin_sql_group_by_agg | Crate: SQL catalog / temp view registration |
| collect failed: not found: ID | Case-sensitive column resolution | test_join_case_insensitive, test_join_all_case_variations | [robin_github_issue_case_sensitivity.md](robin_github_issue_case_sensitivity.md) |
| collect failed: type String is incompatible with Int64 | Union/join type coercion differs from PySpark | test_pyspark_parity_union_by_name | Crate: union/join coercion |
| collect failed: cannot compare string with numeric | String vs numeric comparison semantics | test_numeric_eq_string | Crate: comparison coercion |
| Window / row_number / percent_rank / lag / lead / ntile | Window functions not implemented or different semantics | test_row_number, test_percent_rank, test_window_* | Stubs in Sparkless; crate may need full impl. See [robin_github_issue_window_row_number_plan.md](robin_github_issue_window_row_number_plan.md) |
| Empty DataFrame + parquet table | Schema-from-empty, append, spark.table() visibility | test_parquet_format_append_*, test_append_* | [robin_parity_from_skipped_tests.md](robin_parity_from_skipped_tests.md) §1 |
| isin / empty list / type | isin with Int64 vs String, empty list | test_isin_with_empty_list, test_isin_with_mixed_types | [robin_github_issue_isin_empty_list.md](robin_github_issue_isin_empty_list.md), [robin_github_issue_unsupported_isin.md](robin_github_issue_unsupported_isin.md) |
| Right/outer/semi/anti join | Join types | [robin_github_issue_right_outer_semi_anti_join.md](robin_github_issue_right_outer_semi_anti_join.md) |
| Between / power / cast (plan) | Logical plan expressions | test_plan_interpreter_cast_between_power | [robin_github_issue_between_power_cast_plan.md](robin_github_issue_between_power_cast_plan.md) |
| groupBy + agg (plan) | groupBy with sum/count in plan | test_groupBy_via_plan_interpreter | [robin_github_issue_groupby_agg_plan.md](robin_github_issue_groupby_agg_plan.md) |
| create_map empty | Empty create_map | [robin_github_issue_create_map_empty.md](robin_github_issue_create_map_empty.md) |
| Describe detail / Delta | DESCRIBE DETAIL, Delta-specific | test_describe_detail_* | Session/DDL layer |

---

## 2. Sparkless-side fixes (status)

| Fix | Description | Status |
|-----|-------------|--------|
| udf import | Expose `udf` stub when Robin has no pandas_udf/UDFRegistration so `from sparkless.sql.functions import udf` succeeds | Done |
| Session stop / _has_active_session | RobinSparkSession.stop(), _has_active_session classmethod | Done |
| F.* window/sort | row_number, percent_rank, lag, lead, ntile, cume_dist, dense_rank from crate or stub | Done |
| RobinColumn methods | astype, substr, desc, asc, isin, getItem, over, fill, like, rlike, isNull, withField, name, startswith, __getitem__ | Done |
| RobinColumn reverse operators | __radd__, __rmul__, __rsub__, __rtruediv__, __rfloordiv__, __rmod__ | Done |
| fillna(subset) | Single Column or list of Columns normalized to column names | Done |
| join(on=Column) | Single Column normalized to list so PyColumn is not iterated | Done |
| createDataFrame tuple/pandas | _rows_to_dicts, _value_to_dict_for_crate, _native_value (numpy/pandas), string schema parsing | Done |
| RobinSparkSession._storage | Property returning None | Done |
| GroupedData.agg / pivot | Delegate to crate when available | Done (crate-dependent) |
| GroupedData.count | RobinGroupedData.count() delegates to Rust PyGroupedData.count() | Done |
| F.sum single-arg | _sum_w so Rust sum(col) not given 2 args; accepts *args, uses first only | Done |
| groupBy list/tuple | groupBy([a,b]) flattens to groupBy(a, b) in _robin_sql | Done |
| cast(DataType) | _data_type_to_cast_string / typeName() in astype, cast, F.cast; RobinColumn.alias wraps so chained .cast(DataType) works | Done |
| when(cond, value) | F.when(cond, value) uses when_otherwise(cond, value, null) when value given | Done |
| F.expr/struct/explode/posexplode/isnan/array_distinct | Stubs raise NotImplementedError; tests in skip list | Done |
| Column eqNullSafe, replace, __and__, __or__ | RobinColumn; eqNullSafe fallback (both_null \| both_eq) when crate has no eq_null_safe | Done |
| Session conf, config, _active_sessions, getActiveSession | _RobinRuntimeConfig, RobinSparkSessionBuilder.config(), class _active_sessions, getActiveSession() | Done |
| drop_duplicates(subset) | _subset_to_column_names normalizes Column/list to names; distinct_subset in Rust | Done |
| py_any_to_column date/datetime/tuple | isoformat() for date/datetime, tuple to string literal in pycolumn.rs | Done |
| cube/rollup | RobinDataFrame.cube(), rollup() raise NotImplementedError; tests in skip list | Done |
| RobinColumn.alias | Return _wrap(self._inner.alias(name)) so chained .cast(DataType) uses RobinColumn.cast | Done |
| join on expression | join(on=Column expression) detected via _is_simple_column_name; raise NotImplementedError with message; tests in skip list | Done |
| orderBy expression columns | orderBy(*cols) when any col has non-simple name uses order_by_exprs with _r.asc/_r.desc so expression columns work | Done |
| Column wrapping (no raw PyColumn) | RobinColumn.__getattr__ wraps column-like returns (_is_pycolumn_like); F.when(cond) result wrapped so .when/.otherwise chain works; .between() etc. return RobinColumn | Done |
| crossJoin / first | RobinDataFrame.crossJoin(other) delegates to _inner.cross_join if present else NotImplementedError; first() = limit(1).collect() → rows[0] or None | Done |
| F.desc_nulls_last / asc_nulls_last / etc. | When crate has no top-level fn, _sort_order_fallback uses Column method (col.desc_nulls_last()) so F.desc_nulls_last(col) works | Done |
| select list/tuple | select(*cols) flattens list/tuple so df.select(["a","b"]) and df.select((c1,c2)) work | Done |
| F.countDistinct | countDistinct alias for count_distinct on Robin functions | Done |
| withColumnRenamed | RobinDataFrame.withColumnRenamed(existing, new); crate or select-with-alias | Done |
| RobinSparkSession.app_name | app_name property (crate or fallback "SparklessApp") | Done |
| RobinColumn __pow__, __rpow__, __neg__ | Power and unary minus; __rpow__ via exp(log(other)*col) | Done |
| Column.replace(subset=...) | replace() accepts **kwargs; subset ignored for Column-level | Done |
| Reader.option() | Store options for read (e.g. CSV header) | Planned / deferred |

---

## 3. Test → category summary

- **Skipped on Robin:** Tests in [tests/robin_skip_list.json](../tests/robin_skip_list.json) are skipped when `SPARKLESS_TEST_BACKEND=robin` so the suite can pass. Each skipped test corresponds to either a **Robin parity gap** (upstream) or a **Sparkless fix** not yet implemented. The skip list is merged from all failed test IDs (upstream + Sparkless limitations) so the Robin suite exits 0.
- **Categories:** Upstream: create_dataframe_from_rows (array/schema/struct/map), select/Column semantics, table not found, type coercion, case sensitivity, window/expr/struct/udf. Sparkless: join on expression, crossJoin/first not implemented (if crate lacks cross_join), F.desc_nulls_last fallback, Column wrapping.
- **Classification:** Run the suite, then `python tests/tools/parse_robin_results.py tests/results_robin_<timestamp>.txt -o tests/robin_results_parsed.json` to categorize each failure as `robin_sparkless` or `fix_sparkless`. Then `python tests/tools/generate_failure_report.py -i tests/robin_results_parsed.json -o tests/TEST_FAILURE_ANALYSIS.md` to refresh the report.

---

## 4. How to run and regenerate

```bash
# Run full suite with Robin backend (output to file)
SPARKLESS_TEST_BACKEND=robin pytest tests/ -n 10 2>&1 | tee tests/results_robin_$(date +%Y%m%d_%H%M%S).txt

# Parse results and categorize failures
python tests/tools/parse_robin_results.py tests/results_robin_<timestamp>.txt -o tests/robin_results_parsed.json

# Generate TEST_FAILURE_ANALYSIS.md
python tests/tools/generate_failure_report.py -i tests/robin_results_parsed.json -o tests/TEST_FAILURE_ANALYSIS.md
```

With the skip list applied, the same run (with `SPARKLESS_TEST_BACKEND=robin`) skips the tests in `robin_skip_list.json` and the suite can exit 0.
