# Robin test failure categories

Source: `/Users/odosmatthews/Documents/coding/sparkless/test_results_full.txt`
Total FAILED: 1031 | Excluded (non-Robin): 10 | Grouped: 1021

| Category ID | Failure key | Count | Representative tests |
|-------------|-------------|-------|----------------------|
| op_withColumn | Operations: withColumn | 372 | tests/test_issue_145_string_cast.py::test_string_cast_works_with_to_timestamp... |
| parity_assertion | AssertionError / DataFrames not equivalent | 115 | tests/parity/functions/test_string.py::TestStringFunctionsParity::test_sounde... |
| col_not_found | RuntimeError: Column not found (alias/schema) | 95 | tests/test_issue_286_aggregate_function_arithmetic.py::TestIssue286AggregateF... |
| op_filter | Operations: filter | 89 | tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe... |
| op_select | Operations: select | 76 | tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe... |
| op_join | Operations: join | 55 | tests/parity/functions/test_array_contains_join_parity.py::TestArrayContainsJ... |
| parity_assertion | assert / wrong result | 52 | tests/parity/dataframe/test_parquet_format_table_append.py::TestParquetFormat... |
| op_withColumn | Operations: withColumn, withColumn | 25 | tests/test_issue_151_to_timestamp_validation.py::TestIssue151ToTimestampValid... |
| other | TypeError: select() items must be str (column name | 18 | tests/test_issue_366_alias_posexplode.py::TestIssue366AliasPosexplode::test_p... |
| op_withColumn | Operations: withColumn, withColumn, withColumn | 15 | tests/test_issue_160_actual_bug_reproduction.py::test_bug_with_lazy_polars_ex... |
| other | RuntimeError: lengths don't match: unable to vstac | 13 | tests/test_issue_413_union_createDataFrame.py::TestIssue413UnionCreateDataFra... |
| other | RuntimeError: cannot compare 'date/datetime/time'  | 12 | tests/test_issue_259_datetime_string_comparison.py::TestIssue259DatetimeStrin... |
| other | RuntimeError: array requires at least one column | 12 | tests/test_issue_367_array_empty.py::TestIssue367ArrayEmpty::test_array_no_ar... |
| other | RuntimeError: lengths don't match: unable to add a | 11 | tests/parity/functions/test_split_limit_parity.py::TestSplitLimitParity::test... |
| other | RuntimeError: not found: name | 11 | tests/test_issue_297_join_different_case_select.py::TestIssue297JoinDifferent... |
| other | RuntimeError: cannot compare string with numeric t | 8 | tests/test_issue_336_window_function_comparison.py::TestIssue336WindowFunctio... |
| other | TypeError: '<' not supported between instances of  | 6 | tests/test_issue_406_aggregate_cast_decimal_drop.py::TestIssue406AggregateCas... |
| other | ValueError: Unsupported materializer type: polars. | 4 | tests/test_issue_160_cache_key_reproduction.py::test_cache_key_includes_colum... |
| op_join | Operations: join, withColumn | 2 | tests/test_issue_331_array_contains_join.py::TestIssue331ArrayContainsJoin::t... |
| other | RuntimeError: casting from null to double not supp | 2 | tests/test_delta_lake_schema_evolution.py::TestDeltaLakeSchemaEvolution::test... |
| other | TypeError: unsupported operand type(s) for -: 'Non | 2 | tests/parity/functions/test_log_float_constant_parity.py::TestLogFloatConstan... |
| other | RuntimeError: lengths don't match: unable to add a | 2 | tests/parity/functions/test_split_limit_parity.py::TestSplitLimitParity::test... |
| other | RuntimeError: lengths don't match: unable to add a | 2 | tests/test_issue_328_split_limit.py::TestIssue328SplitLimit::test_split_with_... |
| other | RuntimeError: lengths don't match: unable to add a | 2 | tests/test_issue_328_split_limit.py::TestIssue328SplitLimit::test_split_conse... |
| op_filter | Operations: filter, filter | 1 | tests/test_issue_445_between_string_column_numeric_bounds.py::test_between_st... |
| op_join | Operations: withColumn, join | 1 | tests/test_issue_290_udf_multiple_arguments.py::TestIssue290UdfMultipleArgume... |
| op_join | Operations: join, join | 1 | tests/test_issue_374_join_aliased_columns.py::TestIssue374JoinAliasedColumns:... |
| op_select | Operations: select, filter | 1 | tests/test_issue_436_concat_cast_string.py::test_concat_filter_after |
| op_withColumn | Operations: withColumn, filter | 1 | tests/test_issue_336_window_function_comparison.py::TestIssue336WindowFunctio... |
| op_withColumn | Operations: withColumn, withColumn, withColumn, wi | 1 | tests/test_issue_398_withfield_window.py::TestIssue398WithFieldWindow::test_w... |
| other | RuntimeError: casting from null to date not suppor | 1 | tests/test_delta_lake_schema_evolution.py::TestDeltaLakeSchemaEvolution::test... |
| other | sparkless.core.exceptions.execution.QueryExecution | 1 | tests/parity/sql/test_dml.py::TestSQLDMLParity::test_insert_from_select |
| other | RuntimeError: not found: NaMe | 1 | tests/test_issue_297_join_different_case_select.py::TestIssue297JoinDifferent... |
| other | RuntimeError: datatypes of join keys don't match - | 1 | tests/test_issue_332_cast_alias_select.py::TestIssue332CastAliasSelect::test_... |
| other | TypeError: object of type 'NoneType' has no len() | 1 | tests/test_issue_328_split_limit.py::TestIssue328SplitLimit::test_split_in_se... |
| other | RuntimeError: lengths don't match: unable to add a | 1 | tests/test_issue_328_split_limit.py::TestIssue328SplitLimit::test_split_very_... |
| other | RuntimeError: conversion from `str` to `datetime[μ | 1 | tests/test_issue_432_cast_datetime_date_noop.py::test_cast_date_only_string_t... |
| other | KeyError: 3 | 1 | tests/test_issue_414_row_number_over_descending.py::TestIssue414RowNumberOver... |
| other | KeyError: ('a', 3) | 1 | tests/test_issue_414_row_number_over_descending.py::TestIssue414RowNumberOver... |
| other | RuntimeError: to_timestamp: invalid series dtype:  | 1 | tests/test_to_timestamp_compatibility.py::TestToTimestampCompatibility::test_... |
| other | RuntimeError: conversion from `str` to `datetime[μ | 1 | tests/test_to_timestamp_compatibility.py::TestToTimestampCompatibility::test_... |
| other | RuntimeError: conversion from `str` to `datetime[μ | 1 | tests/unit/backend/test_robin_materializer.py::TestRobinMaterializerExpressio... |
| other | RuntimeError: conversion from `str` to `datetime[μ | 1 | tests/test_type_strictness.py::TestTypeStrictness::test_to_timestamp_accepts_... |
| other | RuntimeError: not found: ID | 1 | tests/integration/test_case_sensitivity.py::TestCaseSensitivityConfiguration:... |

## Example errors

### op_withColumn (Operations: withColumn)

```
sparkless.core.exceptions.operation.SparkUnsupportedOperationError: Operation 'Operations: withColumn' is not supported
```

### parity_assertion (AssertionError / DataFrames not equivalent)

```
AssertionError: DataFrames are not equivalent:
```

### col_not_found (RuntimeError: Column not found (alias/schema))

```
RuntimeError: not found: Column 'Charlie' not found. Available columns: [Name, Value]. Check spelling and case sensitivity (spark.sql.caseSensitive).
```

### op_filter (Operations: filter)

```
sparkless.core.exceptions.operation.SparkUnsupportedOperationError: Operation 'Operations: filter' is not supported
```

### op_select (Operations: select)

```
sparkless.core.exceptions.operation.SparkUnsupportedOperationError: Operation 'Operations: select' is not supported
```

### op_join (Operations: join)

```
sparkless.core.exceptions.operation.SparkUnsupportedOperationError: Operation 'Operations: join' is not supported
```

### parity_assertion (assert / wrong result)

```
assert 0 == 2
```

### op_withColumn (Operations: withColumn, withColumn)

```
sparkless.core.exceptions.operation.SparkUnsupportedOperationError: Operation 'Operations: withColumn, withColumn' is not supported
```

### other (TypeError: select() items must be str (column name) or Column (expression))

```
TypeError: select() items must be str (column name) or Column (expression)
```

### op_withColumn (Operations: withColumn, withColumn, withColumn)

```
sparkless.core.exceptions.operation.SparkUnsupportedOperationError: Operation 'Operations: withColumn, withColumn, withColumn' is not supported
```

### other (RuntimeError: lengths don't match: unable to vstack, column names don't match: ")

```
RuntimeError: lengths don't match: unable to vstack, column names don't match: "id" and "x"
```

### other (RuntimeError: cannot compare 'date/datetime/time' to a string value)

```
RuntimeError: cannot compare 'date/datetime/time' to a string value
```

### other (RuntimeError: array requires at least one column)

```
RuntimeError: array requires at least one column
```

### other (RuntimeError: lengths don't match: unable to add a column of length 4 to a DataF)

```
RuntimeError: lengths don't match: unable to add a column of length 4 to a DataFrame of height 1
```

### other (RuntimeError: not found: name)

```
RuntimeError: not found: name
```

### other (RuntimeError: cannot compare string with numeric type (i64))

```
RuntimeError: cannot compare string with numeric type (i64)
```

### other (TypeError: '<' not supported between instances of 'NoneType' and 'NoneType')

```
TypeError: '<' not supported between instances of 'NoneType' and 'NoneType'
```

### other (ValueError: Unsupported materializer type: polars. Sparkless v4 supports only th)

```
ValueError: Unsupported materializer type: polars. Sparkless v4 supports only the Robin backend (robin-sparkless).
```

### op_join (Operations: join, withColumn)

```
sparkless.core.exceptions.operation.SparkUnsupportedOperationError: Operation 'Operations: join, withColumn' is not supported
```

### other (RuntimeError: casting from null to double not supported)

```
RuntimeError: casting from null to double not supported
```
