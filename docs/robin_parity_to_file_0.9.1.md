# Robin parity issues to file (from test run 2026-02-13, robin-sparkless 0.9.1)

Classified from `test_run_20260213_194551.txt` (1940 failed, 687 passed). Verified with minimal Robin + PySpark repro scripts where indicated.

## Verified with repro scripts (created)

| # | Title | Link | Repro script |
|---|-------|------|--------------|
| 344 | [Parity] greatest() / least() should accept multiple arguments (variadic) | https://github.com/eddiethedean/robin-sparkless/issues/344 | `scripts/robin_parity_repros_0.9.1/greatest_least_variadic.py` |
| 345 | [Parity] coalesce() should accept multiple arguments (variadic) | https://github.com/eddiethedean/robin-sparkless/issues/345 | `scripts/robin_parity_repros_0.9.1/coalesce_variadic.py` |
| 346 | [Parity] GroupedData.avg() should accept multiple column names | https://github.com/eddiethedean/robin-sparkless/issues/346 | `scripts/robin_parity_repros_0.9.1/groupeddata_avg_multiple_cols.py` |
| 347 | [Parity] SQL: support CREATE SCHEMA / CREATE DATABASE (DDL) | https://github.com/eddiethedean/robin-sparkless/issues/347 | `scripts/robin_parity_repros_0.9.1/sql_ddl_not_supported.py` |

## All Robin parity categories (from classifier)

Full list: `tests/robin_parity_failures.txt`, `tests/robin_parity_failures_by_category.txt`.

| Category | Description (short) |
|----------|---------------------|
| aggregate_duplicate_column_alias | duplicate: column 'value' has more than one occurrence |
| argument_sequence_type | argument 'cols'/'partition_by' etc. cannot be converted to 'Sequence' |
| catalog_api | listDatabases, createDatabase, etc. missing on session |
| column_eqnullsafe | Column.eqNullSafe missing (verified fixed in 0.9.1 in direct repro) |
| column_isnull | Column.isNull / isNotNull missing |
| column_resolution | not found: Column 'X' not found; case sensitivity |
| dataframe_agg_global | DataFrame.agg() takes 1 positional argument but 3 given |
| describe_or_show_mode | 'function' object has no attribute 'mode' |
| filter_condition_type | condition must be a Column or literal bool |
| functions_variadic_coalesce | py_coalesce() takes 1 argument (verified with repro) |
| functions_variadic_greatest_least | py_greatest() / py_least() take 1 argument (verified with repro) |
| groupeddata_avg_multiple_cols | GroupedData.avg() multiple cols (verified with repro) |
| join_on_column_not_accepted | join 'on' must be str or list/tuple of str |
| lengths_dont_match | lengths don't match (e.g. withColumn / split) |
| order_by_ascending_type | argument 'ascending': bool cannot be converted to Sequence |
| order_by_pysortorder | Invalid column type: PySortOrder |
| result_parity_null_handling | DataFrames are not equivalent (isnull, isnotnull, nvl, nullif) |
| result_parity_schema_type | Expected TimestampType, got StringType |
| result_parity_type_coercion | assert '25' == 25 (type coercion) |
| sql_ddl_not_supported | SQL: only SELECT supported (verified with repro) |
| whenthen_when_chain | WhenThen has no attribute 'when' |
| window_partition_by_type | WindowSpec / partition_by type |
