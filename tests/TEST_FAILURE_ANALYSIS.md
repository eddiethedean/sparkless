# Test Failure Analysis (Robin Backend)

**Run:** `SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12`  
**Results file:** `sparkless/tests/robin_run_no_skip2.txt`

## Summary

| Phase     | Passed | Failed | Errors | Skipped |
|-----------|--------|--------|--------|---------|
| Unit      | 690    | 1863    | 0    | 22       |
| Parity    | 0     | 0    | 0     | 0       |

**Total failures/errors:** 1863 unit + 0 parity = **1863** (excluding passed/skipped).

---

## 1. Tests removed


The following obsolete tests were deleted or moved so they no longer run.


### 1.1 Deleted files


| File | Reason |
|------|--------|
| `tests/unit/backend/test_robin_unsupported_raises.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/unit/backend/test_robin_optional.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/unit/backend/test_robin_materializer.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/test_backend_capability_model.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/test_issue_160_*.py` (13 files) | Polars/BackendFactory/cache; backend removed. |

### 1.2 Moved to archive


- `tests/unit/dataframe/test_inferschema_parity.py → tests/archive/unit/dataframe/test_inferschema_parity.py` (ignored by pytest `--ignore=tests/archive`).


---

## 2. Robin-sparkless (upstream crate)


Failures that indicate the **robin-sparkless** crate does not match PySpark semantics. Fix or track upstream.


| Error pattern | Example tests | Action |
|---------------|----------------|--------|
| cannot convert to Column | test_to_timestamp_with_filter_isnotnull, test_to_timestamp_with_filter_isnull, test_to_timestamp_with_multiple_filters, test_to_timestamp_with_multiple_operations_and_filter (+260) | Crate: fix semantics or track upstream. |
| select expects Column or str | test_inner_join_then_groupby, test_three_column_join_then_groupby, test_join_then_aggregate_with_join_keys, test_sum_over_window (+105) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'St... | test_struct_field_with_alias_parity, test_struct_field_with_alias_multiple_fields_parity, test_struct_field_with_alias_and_other_columns_parity, test_struct_field_with_alias (+45) | Crate: fix semantics or track upstream. |
| collect failed: filter predicate must be of type `Boolean`, got `String` | test_filter_with_string_equals, test_filter_with_string_and_condition, test_filter_and_string_and_is_null, test_filter_and_literal_contains_and (+39) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: struct value must be object (by field name... | test_getfield_struct_field_by_name, test_withfield_with_conditional_expression, test_withfield_with_cast_operation, test_withfield_replace_with_different_type (+31) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: array element ... | test_getfield_nested_array_access, test_getfield_chained_access, test_posexplode_nested_arrays, test_array_type_elementtype_with_date_type (+6) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'm'... | test_map_column_subscript_multiple_in_select, test_map_column_subscript_orderby_result, test_map_column_subscript_when_otherwise, test_map_column_subscript_chained_with_columns (+4) | Crate: fix semantics or track upstream. |
| assert False | test_pyspark_parity_int64_string_inner, test_pyspark_parity_double_precision_string, test_column_methods_exist, test_first_ignorenulls_type_preservation (+3) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported string f... | test_array_contains_join_empty_arrays, test_posexplode_alias_two_names_empty_array, test_posexplode_empty_array, test_posexplode_alias_empty_array | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported boolean ... | test_explode_with_booleans, test_array_distinct_boolean_arrays, test_array_type_elementtype_with_all_primitive_types | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'me... | test_drop_duplicates_with_dict_column_mock_only, test_describe_detail_complex_schema, test_array_type_elementtype_in_complex_schema | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `datetime[μs]` failed in column 's' ... | test_cast_string_to_timestamp_still_works, test_cast_date_only_string_to_timestamp | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `i32` failed in column 'text' for va... | test_astype_invalid_string_to_int, test_astype_empty_string_handling | Crate: fix semantics or track upstream. |
| table(test_schema.test_table) failed: Table or view 'test_schema.test_table' ... | test_parquet_format_append_detached_df_visible_to_active_session, test_parquet_format_append_detached_df_visible_to_multiple_sessions | Crate: fix semantics or track upstream. |
| assert ('At least one column' in 'cannot convert to Column' or 'must be speci... | test_window_orderby_list_empty_list_error | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Ou... | test_column_subscript_with_nested_struct | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Le... | test_column_subscript_deeply_nested_struct | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `datetime[μs]` failed in column 'Dat... | test_exact_scenario_from_issue_432 | Crate: fix semantics or track upstream. |
| collect failed: cannot compare string with numeric type (i32) | test_between_string_column_in_select_expression | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported long for... | test_array_distinct_with_nulls_in_array | Crate: fix semantics or track upstream. |
| collect failed: casting from f64 to i32 not supported | test_astype_double_to_int | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported int for ... | test_drop_duplicates_with_nulls_in_array | Crate: fix semantics or track upstream. |
| collect failed: casting from string to boolean failed for value '' | test_astype_string_to_boolean | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Ma... | test_map_column_subscript_with_column_key_exact_issue_441 | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'da... | test_map_column_subscript_in_select | Crate: fix semantics or track upstream. |
| collect failed: cannot compare string with numeric type (i64) | test_numeric_eq_string | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'ma... | test_getItem_with_map_key | Crate: fix semantics or track upstream. |
| collect failed: 'is_in' cannot check for List(Int64) values in String data | test_isin_with_mixed_types | Crate: fix semantics or track upstream. |

---

## 3. Fix Sparkless (Python / Robin integration layer)


Failures that should be fixed in **this repo** (Sparkless): missing APIs, wrong conversion, or session/reader implementation.


### 3.1 PyColumn (missing methods)


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'builtins.PyColumn' object has no attribute '__mul__' | test_with_column, test_join_then_withcolumn_on_join_key, test_struct_with_expressions (+70) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'mode' | test_get_table_in_database, test_cache_table, test_uncache_table (+65) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'otherwise' | test_casewhen_subtraction, test_casewhen_addition, test_casewhen_multiplication (+34) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute '__truediv__' | test_avg_divide, test_fillna_float_subset_calculated_column, test_fillna_float_multiple_calculated_columns (+21) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'format' | test_merge_schema_append, test_merge_schema_bidirectional, test_describe_detail_basic (+19) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'drop' | test_na_drop_with_subset_exact_issue_scenario, test_na_drop_no_subset_drops_any_null, test_na_drop_subset_as_string (+19) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute '__add__' | test_count_plus_one, test_arithmetic_with_nulls, test_mixed_aggregate_functions (+18) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'isNotNull' | test_eqnullsafe_example_from_issue_260, test_eqnullsafe_literal_semantics[None-None-True], test_eqnullsafe_literal_semantics[None-x-False] (+17) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'flatMap' | test_rdd_flatmap_words, test_rdd_flatmap_empty_iterable, test_rdd_flatmap_then_map (+9) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute '__sub__' | test_count_star_arithmetic, test_power_negative_exponent_exact_issue, test_power_negative_exponent_multiple_values (+7) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'when' | test_casewhen_multiple_when_conditions, test_between_string_column_in_when_otherwise, test_when_comparison_with_none_exact_issue (+2) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'getField' | test_getfield_array_index, test_getfield_equivalent_to_getitem, test_getfield_negative_index | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute '__mod__' | test_reverse_modulo, test_reverse_operations_modulo_by_zero, test_string_modulo_with_numeric | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'contains' | test_concat_filter_after, test_string_operations_in_filters | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'over' | test_first_value | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute '__invert__' | test_logical_operations_in_filters | Implement or fix in Sparkless (see current analysis). |

### 3.2 Session / lifecycle


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| Cannot perform mean aggregate function: No active SparkSession foun... | test_cast_alias_select_basic, test_cast_alias_select_multiple_aggregations, test_cast_alias_select_different_cast_types (+15) | Implement or fix in Sparkless (see current analysis). |
| Cannot perform sum aggregate function: No active SparkSession found... | test_window_orderby_list_with_rows_between, test_window_orderby_list_with_range_between, test_window_function_comparison_with_rowsBetween (+3) | Implement or fix in Sparkless (see current analysis). |
| Cannot perform count aggregate function: No active SparkSession fou... | test_functions_are_static_methods, test_function_signatures_match_pyspark, test_cast_alias_select_all_aggregation_functions (+1) | Implement or fix in Sparkless (see current analysis). |
| Cannot perform max aggregate function: No active SparkSession found... | test_grouped_data_mean_with_multiple_aggregations, test_window_function_comparison_with_max, test_create_map_in_groupby_agg | Implement or fix in Sparkless (see current analysis). |
| Cannot perform countDistinct aggregate function: No active SparkSes... | test_window_function_comparison_with_countDistinct, test_window_orderby_list_with_count_distinct | Implement or fix in Sparkless (see current analysis). |
| Cannot perform avg aggregate function: No active SparkSession found... | test_window_orderby_list_with_aggregation_functions, test_window_function_comparison_with_avg | Implement or fix in Sparkless (see current analysis). |
| Cannot perform stddev aggregate function: No active SparkSession fo... | test_window_orderby_list_with_stddev_variance | Implement or fix in Sparkless (see current analysis). |
| Cannot perform min aggregate function: No active SparkSession found... | test_window_function_comparison_with_min | Implement or fix in Sparkless (see current analysis). |

### 3.3 Schema / NoneType


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'NoneType' object has no attribute 'fields' | test_unionByName_all_case_variations, test_select_all_case_variations, test_case_insensitive_unionByName | Implement or fix in Sparkless (see current analysis). |

### 3.4 Error message parity


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| createDataFrame requires schema when data is list of tuples | test_createDataFrame_from_pandas, test_createDataFrame_from_pandas_with_nulls | Implement or fix in Sparkless (see current analysis). |

### 3.5 Other (fix_sparkless)


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'str' object cannot be converted to 'PyDict' | test_createDataFrame_from_pandas_empty, test_createDataFrame_from_pandas_with_schema | Implement or fix in Sparkless (see current analysis). |

---

## 4. Other


Failures not categorized as robin_sparkless or fix_sparkless: **940**.


Sample (first 15):


- `tests/test_issue_287_na_replace.py::TestIssue287NAReplace::test_na_replace_multiple_chained_operations` — Column.replace is not implemented for the Robin backend. ...

- `tests/test_issue_263_isnan_string.py::TestIssue263IsnanString::test_isnan_on_string_column_filter_does_not_error_and_returns_empty` — isnan is not implemented for the Robin backend. See docs/...

- `tests/test_groupby_rollup_cube_with_list.py::test_rollup_with_tuple` — rollup() is not implemented for the Robin backend. See do...

- `tests/parity/functions/test_math.py::TestMathFunctionsParity::test_math_sqrt` — DataFrames are not equivalent:

- `tests/test_issue_287_na_replace.py::TestIssue287NAReplace::test_na_replace_with_mixed_types_in_column` — Column.replace is not implemented for the Robin backend. ...

- `tests/test_groupby_rollup_cube_with_list.py::test_rollup_backward_compatibility` — rollup() is not implemented for the Robin backend. See do...

- `tests/test_issue_263_isnan_string.py::TestIssue263IsnanString::test_isnan_on_numeric_column_true_only_for_nan` — isnan is not implemented for the Robin backend. See docs/...

- `tests/parity/functions/test_math.py::TestMathFunctionsParity::test_math_pow` — DataFrames are not equivalent:

- `tests/test_groupby_rollup_cube_with_list.py::test_cube_with_list` — cube() is not implemented for the Robin backend. See docs...

- `tests/test_issue_287_na_replace.py::TestIssue287NAReplace::test_na_replace_large_dataframe` — Column.replace is not implemented for the Robin backend. ...

- `tests/test_issue_287_na_replace.py::TestIssue287NAReplace::test_na_replace_preserves_other_columns` — Column.replace is not implemented for the Robin backend. ...

- `tests/test_issue_263_isnan_string.py::TestIssue263IsnanString::test_isnan_literal_matches_python_math` — isnan is not implemented for the Robin backend. See docs/...

- `tests/test_groupby_rollup_cube_with_list.py::test_cube_with_tuple` — cube() is not implemented for the Robin backend. See docs...

- `tests/test_groupby_rollup_cube_with_list.py::test_cube_backward_compatibility` — cube() is not implemented for the Robin backend. See docs...

- `tests/test_issue_287_na_replace.py::TestIssue287NAReplace::test_na_replace_case_insensitive_column_name` — Column.replace is not implemented for the Robin backend. ...


… and 925 more.


---

## 5. Suggested order of work


1. **Delete** obsolete tests (section 1) — done.

2. **Sparkless fixes** that unblock many tests: PyColumn (astype, desc/asc, isin, getItem, substr, reverse operators), RobinDataFrameReader.option(), RobinSparkSession.stop(), Functions (first, rank), fillna(subset=...), withField/createDataFrame tuple→dict conversion.

3. **Robin-sparkless** (upstream): create_dataframe_from_rows (map, array, struct), select/Column conversion, comparison/union/join coercion, temp view lookup.

4. **Re-run** with the command below and iterate.


---

## 6. How to re-run and regenerate


Run the full suite and save output:


```bash

SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12 2>&1 | tee tests/results_robin_$(date +%Y%m%d_%H%M%S).txt

```


Then parse the new results file and regenerate this report:


```bash

python tests/tools/parse_robin_results.py tests/results_robin_<timestamp>.txt -o tests/robin_results_parsed.json

python tests/tools/generate_failure_report.py -i tests/robin_results_parsed.json -o tests/TEST_FAILURE_ANALYSIS.md

```
