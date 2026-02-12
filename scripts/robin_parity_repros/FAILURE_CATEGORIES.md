# Robin test failure categories

Source: `test_results_full.txt`
Total FAILED: 910 | Excluded (non-Robin): 1 | Grouped: 909

| Category ID | Failure key | Count | Representative tests |
|-------------|-------------|-------|----------------------|
| op_withColumn | Operations: withColumn | 311 | tests/test_issue_145_string_cast.py::test_string_cast_works_with_to_timestamp... |
| parity_assertion | AssertionError / DataFrames not equivalent | 132 | tests/parity/functions/test_string.py::TestStringFunctionsParity::test_base64... |
| col_not_found | RuntimeError: Column not found (alias/schema) | 101 | tests/test_issue_203_filter_with_string.py::TestIssue203FilterWithString::tes... |
| parity_assertion | assert / wrong result | 71 | tests/test_issue_261_between.py::TestIssue261Between::test_between_in_select_... |
| op_filter | Operations: filter | 51 | tests/test_issue_290_udf_multiple_arguments.py::TestIssue290UdfMultipleArgume... |
| op_select | Operations: select | 47 | tests/test_issue_290_udf_multiple_arguments.py::TestIssue290UdfMultipleArgume... |
| op_join | Operations: join | 32 | tests/parity/functions/test_array_contains_join_parity.py::TestArrayContainsJ... |
| op_withColumn | Operations: withColumn, withColumn | 18 | tests/test_issue_160_dropped_column_execution_plan.py::TestIssue160DroppedCol... |
| other | RuntimeError: invalid series dtype: expected `List | 18 | tests/test_issue_366_alias_posexplode.py::TestIssue366AliasPosexplode::test_p... |
| other | RuntimeError: cannot compare 'date/datetime/time'  | 13 | tests/test_issue_139_datetime_validation_compatibility.py::TestIssue139Dateti... |
| other | RuntimeError: lengths don't match: unable to vstac | 13 | tests/test_issue_413_union_createDataFrame.py::TestIssue413UnionCreateDataFra... |
| other | RuntimeError: array requires at least one column | 12 | tests/test_issue_367_array_empty.py::TestIssue367ArrayEmpty::test_array_no_ar... |
| other | RuntimeError: lengths don't match: unable to add a | 11 | tests/parity/functions/test_split_limit_parity.py::TestSplitLimitParity::test... |
| other | RuntimeError: not found: name | 11 | tests/test_issue_297_join_different_case_select.py::TestIssue297JoinDifferent... |
| op_withColumn | Operations: withColumn, withColumn, withColumn | 9 | tests/test_issue_294_hour_minute_second_string_timestamps.py::TestIssue294Hou... |
| other | RuntimeError: cannot compare string with numeric t | 9 | tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe... |
| other | RuntimeError: round can only be used on numeric ty | 9 | tests/test_issue_373_round_string.py::TestIssue373RoundString::test_round_str... |
| other | RuntimeError: datatypes of join keys don't match - | 7 | tests/test_issue_152_sql_column_aliases.py::TestIssue152SQLColumnAliases::tes... |
| other | TypeError: '<' not supported between instances of  | 6 | tests/test_issue_406_aggregate_cast_decimal_drop.py::TestIssue406AggregateCas... |
| op_join | Operations: join, withColumn | 2 | tests/test_issue_331_array_contains_join.py::TestIssue331ArrayContainsJoin::t... |
| other | TypeError: unsupported operand type(s) for -: 'Non | 2 | tests/parity/functions/test_log_float_constant_parity.py::TestLogFloatConstan... |
| other | RuntimeError: casting from null to double not supp | 2 | tests/test_delta_lake_schema_evolution.py::TestDeltaLakeSchemaEvolution::test... |
| other | RuntimeError: lengths don't match: unable to add a | 2 | tests/parity/functions/test_split_limit_parity.py::TestSplitLimitParity::test... |
| other | RuntimeError: lengths don't match: unable to add a | 2 | tests/test_issue_328_split_limit.py::TestIssue328SplitLimit::test_split_conse... |
| other | RuntimeError: lengths don't match: unable to add a | 2 | tests/test_issue_328_split_limit.py::TestIssue328SplitLimit::test_split_unico... |
| op_select | Operations: select, filter | 1 | tests/test_issue_436_concat_cast_string.py::test_concat_filter_after |
| op_withColumn | Operations: withColumn, filter | 1 | tests/test_issue_336_window_function_comparison.py::TestIssue336WindowFunctio... |
| other | sparkless.core.exceptions.execution.QueryExecution | 1 | tests/parity/sql/test_dml.py::TestSQLDMLParity::test_insert_from_select |
| other | RuntimeError: casting from null to date not suppor | 1 | tests/test_delta_lake_schema_evolution.py::TestDeltaLakeSchemaEvolution::test... |
| other | RuntimeError: lengths don't match: unable to add a | 1 | tests/test_issue_328_split_limit.py::TestIssue328SplitLimit::test_split_very_... |
| other | RuntimeError: datatypes of join keys don't match - | 1 | tests/test_issue_332_cast_alias_select.py::TestIssue332CastAliasSelect::test_... |
| other | TypeError: object of type 'NoneType' has no len() | 1 | tests/test_issue_328_split_limit.py::TestIssue328SplitLimit::test_split_in_se... |
| other | RuntimeError: not found: NaMe | 1 | tests/test_issue_297_join_different_case_select.py::TestIssue297JoinDifferent... |
| other | KeyError: 3 | 1 | tests/test_issue_414_row_number_over_descending.py::TestIssue414RowNumberOver... |
| other | KeyError: ('a', 3) | 1 | tests/test_issue_414_row_number_over_descending.py::TestIssue414RowNumberOver... |
| other | RuntimeError: to_timestamp: invalid series dtype:  | 1 | tests/test_to_timestamp_compatibility.py::TestToTimestampCompatibility::test_... |
| other | RuntimeError: conversion from `str` to `datetime[μ | 1 | tests/test_to_timestamp_compatibility.py::TestToTimestampCompatibility::test_... |
| other | RuntimeError: conversion from `str` to `datetime[μ | 1 | tests/test_type_strictness.py::TestTypeStrictness::test_to_timestamp_accepts_... |
| other | RuntimeError: cannot compare string with numeric t | 1 | tests/test_issue_445_between_string_column_numeric_bounds.py::test_between_st... |
| other | RuntimeError: conversion from `str` to `datetime[μ | 1 | tests/test_issue_432_cast_datetime_date_noop.py::test_cast_date_only_string_t... |
| other | RuntimeError: conversion from `str` to `datetime[μ | 1 | tests/unit/backend/test_robin_materializer.py::TestRobinMaterializerExpressio... |

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
RuntimeError: not found: Column 'IT' not found. Available columns: [dept, name, salary]. Check spelling and case sensitivity (spark.sql.caseSensitive).
```

### parity_assertion (assert / wrong result)

```
assert None is True
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

### op_withColumn (Operations: withColumn, withColumn)

```
sparkless.core.exceptions.operation.SparkUnsupportedOperationError: Operation 'Operations: withColumn, withColumn' is not supported
```

### other (RuntimeError: invalid series dtype: expected `List`, got `str`)

```
RuntimeError: invalid series dtype: expected `List`, got `str`
```

### other (RuntimeError: cannot compare 'date/datetime/time' to a string value)

```
RuntimeError: cannot compare 'date/datetime/time' to a string value
```

### other (RuntimeError: lengths don't match: unable to vstack, column names don't match: ")

```
RuntimeError: lengths don't match: unable to vstack, column names don't match: "id" and "x"
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

### op_withColumn (Operations: withColumn, withColumn, withColumn)

```
sparkless.core.exceptions.operation.SparkUnsupportedOperationError: Operation 'Operations: withColumn, withColumn, withColumn' is not supported
```

### other (RuntimeError: cannot compare string with numeric type (i64))

```
RuntimeError: cannot compare string with numeric type (i64)
```

### other (RuntimeError: round can only be used on numeric types)

```
RuntimeError: round can only be used on numeric types
```

### other (RuntimeError: datatypes of join keys don't match - `__sparkless_join_key__`: str)

```
RuntimeError: datatypes of join keys don't match - `__sparkless_join_key__`: str on left does not match `__sparkless_join_key__`: i64 on right
```

### other (TypeError: '<' not supported between instances of 'NoneType' and 'NoneType')

```
TypeError: '<' not supported between instances of 'NoneType' and 'NoneType'
```

### op_join (Operations: join, withColumn)

```
sparkless.core.exceptions.operation.SparkUnsupportedOperationError: Operation 'Operations: join, withColumn' is not supported
```
