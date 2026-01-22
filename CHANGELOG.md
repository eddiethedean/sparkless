# Changelog

## 3.27.0 — Unreleased

### Fixed
- **Window function alias extraction in Python evaluation path**
  - Fixed issue where Python-evaluated window functions (e.g., `percent_rank()`, `ntile()`) in `apply_select` were using default aliases (e.g., `percent_rank_window`) instead of user-defined aliases (e.g., `percentile`)
  - Updated alias extraction in Python evaluation path to use `original_col_for_alias` instead of the processed `col`, matching the non-Python path behavior
  - Fixes `test_window_function_multiply`, `test_window_function_rmul`, `test_window_function_chained_operations`, `test_ntile_with_arithmetic`, and `test_multiple_window_functions_with_arithmetic` tests that were returning `None` values
  - Ensures user-defined aliases are preserved when window functions fall back to Python evaluation

- **UnboundLocalError in apply_select**
  - Fixed `UnboundLocalError: cannot access local variable 'had_python_window_functions'` that occurred when `apply_select` was called without Python window functions
  - Initialized `had_python_window_functions` before the conditional block to ensure it's always defined
  - Fixes 317 test failures that were caused by this error

- **Issue #297** - Fixed column name resolution after join when columns differ only by case
  - Fixed `AnalysisException: Ambiguous column name` when selecting columns after a join where columns differ only by case (e.g., "name" vs "Name")
  - Updated `ColumnResolver.resolve_column_name()` to return the first matching column in case-insensitive scenarios instead of raising an exception, matching PySpark behavior
  - Modified `TransformationService.select()` to preserve the original requested column name (e.g., "NaMe") rather than replacing it with the resolved canonical name
  - Updated `SchemaManager._handle_select_operation()` to dynamically determine output column name based on ambiguity:
    - If multiple case-insensitive matches exist, use the requested column name (e.g., "NaMe")
    - If only a single case-insensitive match exists, use the original column name from the schema (e.g., "Name" if "name" was requested)
  - Updated `PolarsOperationExecutor.apply_select()` to correctly alias columns when ambiguity exists
  - Modified `PolarsMaterializer` to pass original requested column names to `apply_select` for proper resolution and aliasing
  - Fixes `KeyError` when accessing Row objects with the requested column name after a join and select operation

- **Issue #295** - Fixed `withColumnRenamed` to treat non-existent columns as no-op (matching PySpark behavior)
  - Fixed `SparkColumnNotFoundError` when trying to rename a non-existent column - now treated as a no-op, matching PySpark behavior
  - Modified `TransformationService.withColumnRenamed()` to return the DataFrame unchanged when the column doesn't exist
  - Modified `TransformationService.withColumnsRenamed()` to skip non-existent columns instead of raising an error (only renames existing columns)
  - Comprehensive test coverage: 27 tests covering edge cases including empty DataFrames, null values, different data types, special characters, unicode, very long column names, and integration with all DataFrame operations (joins, groupBy, select, orderBy, union, distinct, withColumn, drop)
  - All tests passing in both Sparkless and PySpark modes, confirming full compatibility
  - Fixes issue where `df.withColumnRenamed("Does-Not-Exist", "New-Name")` would raise an error instead of silently ignoring the operation

- **Issue #296** - Fixed UDF decorator interface support (`@udf(DataType())` pattern)
  - Fixed `AttributeError: 'function' object has no attribute 'name'` when using `@udf(T.StringType())` decorator pattern
  - Updated `Functions.udf()` to correctly detect when a DataType instance is passed as the first positional argument (decorator pattern) and treat it as `returnType`
  - When `@udf(T.StringType())` is used, the DataType instance is now correctly recognized as `returnType` instead of being treated as the function parameter
  - Maintains backward compatibility with function interface (`F.udf(lambda x: x.upper(), T.StringType())`)
  - Comprehensive test coverage: 37 tests covering all decorator patterns including:
    - Decorator with return type: `@udf(T.StringType())`
    - Decorator without return type: `@udf()` (defaults to StringType)
    - Different return types (String, Integer, Double, Boolean, Date, Timestamp, Array)
    - Multiple arguments (2, 3+ parameters)
    - Various DataFrame operations (withColumn, select, filter, groupBy, join, union, distinct, orderBy, drop)
    - Edge cases: empty DataFrames, null values, special characters, unicode, very long strings
    - Complex scenarios: conditional logic, exception handling, nested calls, chained UDFs, idempotent behavior
  - All tests passing in both Sparkless and PySpark modes, confirming full compatibility
  - Fixes issue where `@udf(T.StringType())` decorator would raise an error instead of working correctly

- **Issue #286** - Added arithmetic operators to `AggregateFunction` class
  - Added support for arithmetic operations on aggregate functions (e.g., `F.countDistinct("Value") - 1`), matching PySpark behavior
  - Implemented `__add__`, `__sub__`, `__mul__`, `__truediv__`, `__mod__` and their reverse counterparts (`__radd__`, `__rsub__`, `__rmul__`, `__rtruediv__`, `__rmod__`) on `AggregateFunction` class
  - Updated `GroupedData._evaluate_column_expression()` to handle arithmetic operations on aggregate functions
  - Supports both `AggregateFunction` and `ColumnOperation` wrapping aggregate functions
  - Supports forward operations (e.g., `F.countDistinct() - 1`) and reverse operations (e.g., `10 - F.countDistinct()`)
  - Supports chained arithmetic operations (e.g., `(F.countDistinct() - 1) * 2`)
  - Properly handles division and modulo by zero (returns `None`, matching PySpark behavior)
  - Works with all aggregate functions: `count`, `sum`, `avg`, `max`, `min`, `countDistinct`, `stddev`, `variance`, etc.

- **Issue #287** - Added `replace` method to `NAHandler` class
  - Added `df.na.replace()` method to match PySpark's `NAHandler.replace()` API
  - Supports dict mapping for value replacements (e.g., `{"A": "TypeA", "B": "TypeB"}`)
  - Supports single value replacement (e.g., `df.na.replace(1, 99)`)
  - Supports list replacements (e.g., `df.na.replace([1, 2], 99)` or `df.na.replace([1, 2], [10, 20])`)
  - Supports `subset` parameter as string, tuple, or list to limit replacement to specific columns
  - Properly handles case-insensitive column name resolution
  - Preserves columns not in the subset during replacement
  - Validates column existence and raises appropriate errors for invalid columns
  - Validates list length matching and raises errors for mismatched lengths
  - Handles edge cases: None values, booleans, empty dicts/lists, special characters, unicode

- **Issue #288** - Added arithmetic and logical operators to `CaseWhen` class
  - Added support for arithmetic operations on `CaseWhen` expressions (e.g., `F.when(...).otherwise(...) - F.when(...).otherwise(...)`), matching PySpark behavior
  - Implemented arithmetic operators: `__add__`, `__sub__`, `__mul__`, `__truediv__`, `__mod__` and their reverse counterparts (`__radd__`, `__rsub__`, `__rmul__`, `__rtruediv__`, `__rmod__`)
  - Implemented logical operators: `__or__` (bitwise OR), `__and__` (bitwise AND), `__invert__` (bitwise NOT)
  - Fixed bitwise NOT (`~`) operator support in Polars backend expression translator
  - Supports forward operations (e.g., `case_when1 - case_when2`)
  - Supports reverse operations (e.g., `100 - case_when`)
  - Supports chained arithmetic operations (e.g., `(case_when1 - case_when2) * 2`)
  - Properly handles division and modulo by zero (returns `None`, matching PySpark behavior)
  - Works with multiple WHEN conditions and nested CaseWhen expressions
  - Supports operations in groupBy aggregation contexts
  - Works with floating point numbers, zero, negative numbers, and large numbers
  - Properly handles null values in operations

- **Issue #289** - Added `struct` function support in Polars backend
  - Added support for `F.struct()` function to create struct-type columns from multiple columns, matching PySpark behavior
  - Implemented struct function translation in `PolarsExpressionTranslator._translate_function_call()`
  - Handles multiple columns (string names and Column objects)
  - Supports case-insensitive column name resolution
  - Creates Polars struct with proper field names using `pl.struct()`
  - Supports struct creation with computed expressions, literals, and aliased columns
  - Works in various contexts: `withColumn`, `select`, `groupBy().agg()`, joins
  - Supports nested structs (struct within struct)
  - Works with arrays, conditional expressions, string functions, and math operations
  - Properly handles null values and empty DataFrames
  - Fixes `ValueError: Unsupported function: struct` error

- **Issue #290** - Added support for UDFs with multiple arguments
  - Added support for UDFs (User Defined Functions) with multiple positional arguments, matching PySpark behavior
  - Modified `Functions.udf()` wrapper to accept `*cols` instead of single `col` parameter
  - Store all column arguments in `op._udf_cols` for backend processing
  - Generate proper UDF name with all column names (e.g., `udf(col1, col2)`)
  - Backend already supported multiple UDF columns via `_udf_cols` - no backend changes needed
  - Maintains backward compatibility with single-argument UDFs
  - Supports UDFs with 2, 3, 4, 5, 6, and 10+ arguments
  - Works with different data types (integers, floats, booleans, strings, dates)
  - Works in various contexts: `withColumn`, `select`, `filter`, `groupBy().agg()`, `orderBy`, joins
  - Supports computed column expressions, conditional logic, and chained operations
  - Properly handles null values, empty DataFrames, and mixed string/Column object inputs
  - Fixes `TypeError: apply_udf() takes 1 positional argument but 2 were given` error

- **Issue #291** - Added support for power operator (**) between floats and Column/ColumnOperation
  - Added `__pow__` method to `ColumnOperatorMixin` for forward power operation (e.g., `col ** 2`)
  - Added `__rpow__` method to `ColumnOperatorMixin` for reverse power operation (e.g., `3.0 ** col` or `2 ** col`)
  - Added "**" to binary operators list in `PolarsExpressionTranslator` to route it correctly
  - Added "**" to arithmetic operations handling in `_coerce_for_arithmetic()`
  - Uses Polars `pow()` function for power operations with proper null/infinity handling
  - Supports power operations with integers, floats, Column objects, and ColumnOperations
  - Works in various contexts: `withColumn`, `select`, `filter`, `groupBy().agg()`, `orderBy`, union
  - Supports nested expressions, chained operations, and mixed numeric types
  - Properly handles null values, zero base/exponent, and negative exponents
  - Supports fractional exponents (square root, cube root, etc.)
  - Supports string column coercion to numeric for power operations
  - Works with conditional expressions, multiple columns, and aliases
  - Fixes `TypeError: unsupported operand type(s) for ** or pow(): 'float' and 'Column'` error

- **Issue #292** - Added support for look-around regex patterns in `rlike()` and related functions
  - Added look-around pattern detection for `rlike`, `regexp`, and `regexp_like` operations
  - Uses Python `re` module fallback when Polars doesn't support look-ahead/look-behind assertions
  - Detects patterns containing `(?=...)`, `(?!...)`, `(?<=...)`, `(?<!...)` assertions
  - Falls back to Python evaluation when Polars raises ComputeError about look-around not being supported
  - Supports negative lookahead (e.g., `(?!.*(Alice\sCat))`), positive lookahead, lookbehind, and negative lookbehind
  - Works with case-insensitive flags, multiple lookaheads, and complex nested patterns
  - Maintains backward compatibility with regular patterns (no look-around)
  - Works in various contexts: `filter`, `select`, `withColumn`, chained operations
  - Properly handles null values and empty DataFrames
  - Fixes `polars.exceptions.ComputeError: regex error: look-around, including look-ahead and look-behind, is not supported` error

- **Issue #293** - Fixed `explode()` and `explode_outer()` functions to properly explode arrays/lists into multiple rows
  - Fixed `F.explode()` to correctly expand array or map columns into new rows, matching PySpark behavior
  - Fixed `F.explode_outer()` to expand arrays while preserving rows with null/empty arrays (unlike regular `explode`)
  - Updated `PolarsOperationExecutor.apply_with_column()` to properly handle `explode` operations in `withColumn` and `select`
  - For regular `explode`, rows with null/empty arrays are dropped (matching PySpark behavior)
  - For `explode_outer`, rows with null/empty arrays are preserved with `None` values (matching PySpark behavior)
  - Properly resolves source column names from `Column` objects and `ColumnOperation` expressions
  - Works with integer, float, boolean, and string arrays
  - Supports homogeneous array types (Polars requirement) - mixed types are handled by converting to strings
  - Works in various contexts: `withColumn`, `select`, `filter`, `groupBy().agg()`, `orderBy`, `distinct`, `union`, `join`
  - Supports chained operations, conditional expressions (`F.when().otherwise()`), `cast()`, string operations
  - Supports multiple `explode` operations on the same DataFrame
  - Properly handles single-element arrays, large arrays, empty arrays, and null arrays
  - Fixes issue where `explode` was not exploding lists as expected

- **Issue #294** - Fixed `hour()`, `minute()`, and `second()` functions to correctly extract time components from string columns containing timestamp values
  - Fixed `F.hour()`, `F.minute()`, and `F.second()` to properly parse string timestamps and extract time components
  - Enhanced `_extract_datetime_part()` in `PolarsExpressionTranslator` to handle various timestamp string formats
  - Added support for timezone formats: `+0000` (normalized to `+00:00`), `-0500`, `Z` format, and timezone-less formats
  - Added support for various timestamp formats: ISO format (`2023-02-07T04:00:01.730+0000`), space-separated, with/without microseconds, date-only
  - Properly handles null timestamp values (returns `None` for all time components)
  - Works in various contexts: `withColumn`, `select`, `filter`, `groupBy().agg()`
  - Fixes issue where `hour()`, `minute()`, and `second()` returned `None` for string timestamp columns

### Testing
- Added comprehensive test suite for issue #297 (`tests/test_issue_297_join_different_case_select.py`)
  - Tests for different join types (inner, left, right, outer)
  - Tests for multiple ambiguous columns
  - Tests for chained operations (filter, orderBy, groupBy)
  - Tests for edge cases (empty DataFrames, null values)
  - Tests for different case variations (NaMe, nAmE, NAME, etc.)
  - Tests for operations after select (withColumn, drop)
  - Verification of single-match vs. multiple-match behavior
- Added comprehensive test suite for issue #286 (`tests/test_issue_286_aggregate_function_arithmetic.py`)
  - 26 test cases covering all arithmetic operations (+, -, *, /, %)
  - Tests for forward and reverse operations
  - Tests for chained arithmetic operations
  - Tests for null handling, floats, negative numbers, zero
  - Tests for division/modulo by zero (returns None)
  - Tests for min, stddev, variance aggregate functions
  - Tests for complex nested operations
  - Tests for count(*), empty groups, large numbers
  - Tests for mixed aggregate functions
  - Tests for aliases and operator precedence
  - All tests pass in both Sparkless (mock) and PySpark backends
- Added comprehensive test suite for issue #287 (`tests/test_issue_287_na_replace.py`)
  - 31 test cases covering all `df.na.replace()` functionality
  - Tests for dict mapping with and without subset
  - Tests for single value and list replacements
  - Tests for different subset formats (string, tuple, list)
  - Tests for multiple columns, numeric values, and edge cases
  - Tests for None/null value handling (replacing None and replacing with None)
  - Tests for boolean values, type coercion, special characters, and unicode
  - Tests for zero and negative numbers, empty dicts/lists
  - Tests for error handling (invalid columns, mismatched list lengths, None value errors)
  - Tests for chained operations, large DataFrames, and column preservation
  - Tests for case-insensitive column name resolution
  - All tests pass in both Sparkless (mock) and PySpark backends
- Added comprehensive test suite for issue #288 (`tests/test_issue_288_casewhen_operators.py`)
  - 27 test cases covering all arithmetic and logical operators on `CaseWhen` expressions
  - Tests for all arithmetic operations (+, -, *, /, %)
  - Tests for bitwise operations (|, &, ~)
  - Tests for forward and reverse operations
  - Tests for chained arithmetic operations
  - Tests for multiple WHEN conditions and nested CaseWhen expressions
  - Tests for division/modulo by zero (returns None)
  - Tests for floating point, zero, negative numbers, and large numbers
  - Tests for null value handling
  - Tests for groupBy aggregation contexts
  - Tests for operator precedence
  - Tests for empty DataFrames, aliases, and mixed operations with regular columns
  - All tests pass in both Sparkless (mock) and PySpark backends
- Added comprehensive test suite for issue #289 (`tests/test_issue_289_struct_function.py`)
  - 20 test cases covering all `F.struct()` functionality
  - Tests for basic struct creation with string column names and `F.col()`
  - Tests for single column, multiple columns, and different data types
  - Tests for null value handling and empty DataFrames
  - Tests for struct with computed expressions, literals, and aliased columns
  - Tests for struct in groupBy aggregation contexts
  - Tests for struct field access verification
  - Tests for nested structs (struct within struct)
  - Tests for struct with arrays
  - Tests for struct in join operations
  - Tests for struct with conditional expressions (when/otherwise)
  - Tests for struct with string functions and math operations
  - Tests for large number of fields (8+ columns)
  - Tests for multiple chained operations
  - All tests pass in both Sparkless (mock) and PySpark backends
- Added comprehensive test suite for issue #290 (`tests/test_issue_290_udf_multiple_arguments.py`)
  - 29 test cases covering all UDF multiple arguments functionality
  - Tests for 2, 3, 4, 5, 6, and 10+ argument UDFs
  - Tests for different data types (integers, floats, booleans, strings, dates)
  - Tests for string names, Column objects, and mixed inputs
  - Tests for null value handling and empty DataFrames
  - Tests for UDF in various contexts: `withColumn`, `select`, `filter`, `groupBy().agg()`, `orderBy`, joins
  - Tests for computed column expressions and nested arithmetic
  - Tests for conditional logic, string functions, and chained operations
  - Tests for decorator pattern and backward compatibility with single-argument UDFs
  - Tests for edge cases: all null arguments, large number of columns, mixed types
  - All tests pass in both Sparkless (mock) and PySpark backends
- Added comprehensive test suite for issue #291 (`tests/test_issue_291_power_operator_float_column.py`)
  - 33 test cases covering all power operator (**) functionality
  - Tests for float ** Column and float ** ColumnOperation (from issue examples)
  - Tests for Column ** number (forward power operation)
  - Tests for integer ** Column, Column ** Column
  - Tests for nested expressions, chained operations, and mixed types
  - Tests for null handling, zero base/exponent, negative exponents
  - Tests for power in select, filter, orderBy, groupBy, and union contexts
  - Tests for fractional exponents (square root, cube root, etc.)
  - Tests for large and small numbers
  - Tests for string column coercion to numeric
  - Tests for complex nested expressions and arithmetic combinations
  - Tests for conditional expressions (when/otherwise)
  - Tests for multiple columns, aliases, and multiple withColumn operations
  - Tests for edge cases: one base/exponent, decimal base/exponent, very large exponents
  - Tests for empty DataFrames
  - All tests pass in both Sparkless (mock) and PySpark backends
- Added comprehensive test suite for issue #292 (`tests/test_issue_292_rlike_lookaround.py`)
  - 36 test cases covering all look-around regex functionality
  - Tests for negative lookahead (from issue example)
  - Tests for positive lookahead, lookbehind, and negative lookbehind
  - Tests for complex look-around patterns and multiple lookaheads
  - Tests for case-insensitive look-around patterns
  - Tests for rlike with and without look-around (backward compatibility)
  - Tests for look-around in filter, select, withColumn, and chained operations
  - Tests for null handling and empty DataFrames
  - Tests for anchored lookaheads, nested lookaheads, lookbehind with digits, combined lookahead/lookbehind
  - Tests for multiple negative lookaheads, lookahead/lookbehind with word boundaries, quantifiers
  - Tests for alternation, capture groups, unicode, escaped characters, large datasets, and case sensitivity
  - Tests for fixed-width lookbehind patterns (Python re module limitation)
  - All tests pass in both Sparkless (mock) and PySpark backends
- Added comprehensive test suite for issue #293 (`tests/test_issue_293_explode_withcolumn.py`)
  - 29 test cases covering all `explode` and `explode_outer` functionality
  - Tests for basic `explode` in `withColumn` and `select` contexts
  - Tests for `explode` with integer, float, boolean, and string arrays
  - Tests for `explode` with empty and null arrays (verifying PySpark's drop behavior for `explode` and retain behavior for `explode_outer`)
  - Tests for `explode` with single-element and large arrays
  - Tests for `explode` with `groupBy().agg()`, `orderBy`, `distinct`, `union`, `join`
  - Tests for `explode` with chained `withColumn` operations, `F.when().otherwise()`, `cast()`, string operations
  - Tests for multiple `explode` operations on the same DataFrame
  - Tests for `explode_outer` with empty arrays
  - Tests for `explode` with aliases and filter operations before/after
  - Tests for homogeneous array types (Polars requirement) and mixed-type handling
  - All tests pass in both Sparkless (mock) and PySpark backends
- Fixed flaky test `test_column_power_number` in `tests/test_issue_291_power_operator_float_column.py`
  - Changed test to find rows by `Value` column instead of relying on row order
  - Added materialization between `withColumn` operations to prevent race conditions in parallel test execution
  - Test now passes consistently in parallel test runs (`-n 10`)
- Added comprehensive test suite for issue #294 (`tests/test_issue_294_hour_minute_second_string_timestamps.py`)
  - 7 test cases covering all `hour()`, `minute()`, and `second()` functionality with string timestamps
  - Tests for exact issue example format (`2023-02-07T04:00:01.730+0000`)
  - Tests for various timezone formats (`+0000`, `-0500`, `Z`, no timezone)
  - Tests for different timestamp formats (ISO, space-separated, with/without microseconds, date-only)
  - Tests for `hour/minute/second` in `select`, `filter`, and `groupBy().agg()` contexts
  - Tests for null timestamp values (verifying `None` return behavior)
  - All tests pass in both Sparkless (mock) and PySpark backends
- Fixed additional flaky tests in `tests/test_issue_291_power_operator_float_column.py`
  - Fixed `test_power_in_multiple_withcolumns`: Added materialization between `withColumn` operations
  - Fixed `test_power_fractional_exponent`: Changed to find rows by `Value` instead of positional indexing, added materialization
  - Tests now pass consistently in parallel test runs (`-n 10`)
  - Tests for `hour/minute/second` in `select`, `filter`, and `groupBy().agg()` contexts
  - Tests for null timestamp values (verifying `None` return behavior)
  - All tests pass in both Sparkless (mock) and PySpark backends

## 3.31.0 — Unreleased

### Added
- **Issue #189** - Implemented missing string and JSON functions for improved PySpark compatibility
  - Added `soundex()` function for phonetic string matching (Soundex algorithm)
  - Added `translate()` function for character-by-character string translation
  - Added `levenshtein()` function for calculating edit distance between strings
  - Added `crc32()` function for CRC32 checksum calculation
  - Added `xxhash64()` function for XXHash64 hashing (deterministic, seed=42, matches PySpark)
  - Added `regexp_extract_all()` function for extracting all regex matches as an array
  - Added `get_json_object()` function for JSONPath-based JSON value extraction
  - Added `json_tuple()` function for extracting multiple JSON fields into separate columns (c0, c1, ...)
  - Added `substring_index()` function for substring extraction based on delimiter occurrences
  - All functions include comprehensive edge case handling (nulls, empty strings, invalid JSON, etc.)
  - All functions match PySpark behavior exactly, including `xxhash64(NULL)` returning seed value (42)
- **Issue #267** - Added aggregate function convenience methods to `PivotGroupedData` class
  - Added `sum()`, `avg()`, `mean()`, `count()`, `max()`, `min()` methods
  - Added `count_distinct()`, `collect_list()`, `collect_set()` methods
  - Added `first()`, `last()`, `stddev()`, `variance()` methods
  - All methods match the `GroupedData` API for consistency with PySpark

### Fixed
- **Issue #279** - Added support for executing Python UDFs in the Polars backend
  - Fixed `ValueError: Unsupported function: udf` when applying `F.udf(...)` via `withColumn`
  - Supports single-argument and multi-argument UDFs
- **Issue #280** - Fixed `join(..., on=[...])` followed by `groupBy([...])` raising ambiguous column errors
  - Join schema projection now avoids duplicate column names for column-name joins
  - Prevents downstream failures like `AnalysisException: Ambiguous column name` and duplicate output columns in chained joins
- **Issue #281** - Fixed `ValueError: dictionary update sequence element ...` when joining DataFrames with unmaterialized operations
  - Join materialization now correctly converts collected rows to dictionaries when the right DataFrame has pending operations (e.g., `withColumn`, `withColumnRenamed`, `drop`, `select`, `filter`)
  - Handles Sparkless Row objects, dicts, and sequence-like rows with schema fallback
- **Issue #267** - Fixed `PivotGroupedData` column naming to match PySpark behavior
  - Single aggregate expression without alias: uses pivot value as column name (e.g., `A`, `B`)
  - Single aggregate expression with alias: uses alias as column name
  - Multiple aggregate expressions: uses `{pivot_value}_{alias}` or `{pivot_value}_{function_name}` format
- Enhanced `PivotGroupedData._evaluate_aggregate_function()` to support all aggregate functions from `GroupedData`
- Enhanced `PivotGroupedData._evaluate_column_expression()` to support all column operations
- Fixed handling of empty pivot groups (returns `None` instead of `0`)

### Testing
- Added comprehensive tests for issue #189 string/JSON functions
  - 8 new parity tests in `tests/parity/functions/test_string.py` for PySpark compatibility validation
  - 1 unit test in `tests/unit/functions/test_regexp_extract_all_189.py` for regex extraction
  - 9 robust edge case tests in `tests/unit/functions/test_issue_189_string_functions_robust.py`
  - Tests cover null handling, empty strings, invalid JSON, missing paths/fields, delimiter edge cases, and multi-match scenarios
  - All tests pass in both Sparkless (mock) and PySpark backends
- Added regression and comprehensive tests for issues #279, #280, and #281
  - UDF regression + comprehensive UDF coverage
  - Join-then-groupBy scenarios across join types, join keys, and follow-on operations
  - Join with unmaterialized operations: multiple pending ops, select+filter, both-sides operations, empty DataFrames
- Added 16 unit tests covering all convenience methods, column naming, and edge cases
- Added 6 PySpark parity tests including the exact example from issue #267
- All tests verify that column naming matches PySpark exactly

## 3.30.0 — 2025-01-21

### Added
- **Issue #266** - Added `rsd` parameter support to `approx_count_distinct()` function
  - Added optional `rsd` (relative standard deviation) parameter to `approx_count_distinct()` function
  - Matches PySpark API: `approx_count_distinct(column, rsd=0.01)`
  - `rsd` parameter controls approximation accuracy (lower values = better accuracy, more memory)
  - Default value is `None`, which uses PySpark's default of 0.05 (5% relative error) when in PySpark mode
  - Function name generation includes `rsd` parameter when provided: `approx_count_distinct(column, rsd=0.01)`

### Fixed
- **Issue #266** - Fixed `approx_count_distinct()` returning `None` in Window functions
  - Added `approx_count_distinct` support to Window function handler
  - Window functions now correctly compute distinct counts instead of returning `None`
  - Fixes the issue where `F.approx_count_distinct("value", rsd=0.01).over(window)` returned `None`

### Testing
- Added 7 unit tests covering backward compatibility, rsd parameter, different values, groupBy, and Window functions
- Added 6 PySpark parity tests including the exact example from issue #266
- All tests verify that Window functions no longer return `None` for `approx_count_distinct`

### Technical Details
- Updated `AggregateFunction` class to store `rsd` attribute (similar to `ord_column`, `ignorenulls`)
- Updated `AggregateFunctions.approx_count_distinct()` and `Functions.approx_count_distinct()` to accept `rsd` parameter
- Enhanced function name generation in `_generate_name()` to include `rsd` when provided
- Added `approx_count_distinct` case to `WindowFunctionHandler._apply_aggregate_to_partition()`
- For mock implementation, `rsd` is accepted for API compatibility but exact counting is used (more accurate than approximation)

## 3.29.0 — 2025-01-21

### Added
- **Issue #265** - Implemented `cast()` method for `AggregateFunction` objects
  - Added `cast()` method to `AggregateFunction` class, enabling type casting of aggregate function results
  - Supports casting aggregate results to different data types (string, int, long, double, float, boolean)
  - Works with all aggregate functions: `sum()`, `avg()`, `mean()`, `max()`, `min()`, `count()`, `countDistinct()`, `stddev()`, `variance()`, etc.
  - Example usage: `df.groupby("type").agg(F.mean(F.col("value")).cast("string"))`
  - Generates PySpark-compatible column names: `CAST(avg(value) AS STRING)`
  - Properly handles nested `ColumnOperation` structures when aggregate functions are wrapped
  - Cast operations are evaluated after aggregate computation, ensuring correct type conversion

### Fixed
- Fixed `GroupedData.agg()` to correctly handle cast operations on aggregate functions
  - Detects when a `ColumnOperation` with "cast" operation wraps an `AggregateFunction`
  - Evaluates the aggregate function first, then applies the cast transformation
  - Uses `TypeConverter` for proper type conversion between different data types
  - Handles both string type names (e.g., "string", "int") and `DataType` objects

### Testing
- Added 7 unit tests covering basic functionality, return types, multiple aggregates, and null handling
- Added 11 PySpark parity tests ensuring exact compatibility with PySpark behavior
- Tests cover various aggregate functions, cast types, null values, empty groups, and chained operations
- All tests work in both normal and PySpark modes via `MOCK_SPARK_TEST_BACKEND` environment variable

### Technical Details
- Updated `AggregateFunction.cast()` to return a `ColumnOperation` wrapping the aggregate and target type
- Enhanced `GroupedData.agg()` evaluation logic to detect and handle cast-wrapped aggregates
- Improved type narrowing in `GroupedData.agg()` for better mypy compliance
- All code quality checks passing (ruff format, ruff check, mypy type checking)

## 3.25.0 — 2025-01-20

### Fixed
- **Issue #270** - Fixed `createDataFrame` with tuple-based data parameter to convert tuples to dictionaries
  - Fixed `AttributeError: 'tuple' object has no attribute 'keys'` when calling `.show()` with tuple-based data
  - Fixed `AttributeError: 'tuple' object has no attribute 'get'` in operations that use `.get()` on rows
  - Fixed `AttributeError: 'tuple' object has no attribute 'items'` in transformation operations
  - Fixed `AttributeError: 'tuple' object has no attribute 'copy'` in misc operations
  - When `createDataFrame` is called with tuple-based data (e.g., `[('Alice', 1), ('Bob', 2)]`) and an explicit `StructType` schema, tuples are now converted to dictionaries using schema field names in order
  - Added strict length validation matching PySpark behavior: raises `IllegalArgumentException` with `LENGTH_SHOULD_BE_THE_SAME` error when tuple length doesn't match schema field count (matching PySpark's `PySparkValueError`)
  - All downstream operations now work correctly with tuple-based data: `.show()`, `.unionByName()`, `.union()`, `.fillna()`, `.replace()`, `.dropna()`, `.groupBy()`, `.join()`, `.select()`, `.filter()`, `.orderBy()`, `.distinct()`, etc.
  - Supports both tuple and list data (e.g., `[(1, 2), (3, 4)]` or `[[1, 2], [3, 4]]`)
  - Handles mixed tuple/dict/Row data correctly
  - Preserves field order as specified in schema
  - Comprehensive test coverage: 23 unit tests covering tuple/list data, None values, various data types, mixed data, edge cases (single row, empty DataFrame, long schemas, complex types), error scenarios, and PySpark parity validation
  - All tests pass in both Sparkless and PySpark modes, confirming full PySpark compatibility
- **Case-Sensitive Mode Enforcement** - Fixed case-sensitive mode (`spark.sql.caseSensitive = true`) to properly enforce exact case matching
  - Fixed attribute access (`df.columnName`) to correctly fail when wrong case is used in case-sensitive mode
  - Fixed `DataFrameAttributeHandler` to use DataFrame's `_is_case_sensitive()` method for proper configuration retrieval
  - Case-sensitive mode now correctly rejects column references with wrong casing across all operations
  - Added comprehensive tests for case-sensitive mode: `withColumn`, `filter`, `select`, `groupBy`, `join`, attribute access, SQL queries
  - All tests pass in both Sparkless and PySpark modes, confirming full PySpark compatibility
  - **Issue #264** - Fixed case-insensitive column resolution in `withColumn` with `F.col()`, specifically when column names differ by case
- **fillna After Join** - Fixed `fillna()` to properly materialize lazy DataFrames before processing
  - Ensures all columns are present after joins before filling null values
  - Prevents missing columns from being incorrectly filled as None
  - Removed xfail marker from `test_na_fill_after_join` (now passing)
- **Issue #263** - Fixed `isnan()` on string columns to match PySpark behavior
  - Prevents Polars backend error: `polars.exceptions.InvalidOperationError: is_nan operation not supported for dtype str`
  - `isnan()` now returns `False` for string values (strings are never NaN in PySpark)
  - `isnan(NULL)` returns `False` (PySpark behavior)
- **Issue #262** - Fixed `ArrayType` initialization with positional arguments
  - Fixed `TypeError: Cannot specify both 'elementType' and 'element_type'` when using positional arguments like `ArrayType(DoubleType(), True)`
  - Added detection for when `elementType` parameter is incorrectly matched as a bool from positional arguments
  - When `elementType` is a bool, it's now correctly treated as the `nullable` parameter instead
  - Maintains full backward compatibility with all existing `ArrayType` initialization patterns:
    - `ArrayType(elementType=StringType())` (PySpark keyword convention)
    - `ArrayType(element_type=StringType())` (snake_case keyword)
    - `ArrayType(StringType())` (positional element_type)
    - `ArrayType(StringType(), nullable=False)` (positional with nullable)
  - Added comprehensive test suite (4 test cases) covering the issue reproduction, all initialization patterns, DataFrame usage, and PySpark parity
- **CI Linting and Type Checking** - Fixed all CI failures related to code quality checks
  - Fixed redundant casts in `misc_service.py` fillna method
  - Fixed return type annotation in `attribute_handler.py` to allow `NAHandler` return type
  - Fixed Row to dict conversion in `set_operations.py` using `Row.asDict()` method
  - Fixed dynamic attribute access in `lazy.py` for window functions using `setattr`/`getattr`
  - Removed unused `type: ignore` comments in `operation_executor.py` decorators
  - Added appropriate `type: ignore` comments where needed for mypy full codebase checks
  - Cleaned up mypy warnings and decorator typing around the Polars `operation_executor` used by `eqNullSafe`
- **PySpark Parity Test Environment** - Ensured PySpark driver and workers use the same Python executable during parity tests
  - Updated `tests/fixtures/spark_backend.py` to set `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` and corresponding Spark config keys (`spark.pyspark.python`, `spark.pyspark.driver.python`)
  - Allows running `MOCK_SPARK_TEST_BACKEND=pyspark` tests for `eqNullSafe` without Python version mismatch errors

### Added
- **Case-Insensitive Column Names Refactor** - Complete refactoring of column name resolution to use centralized `ColumnResolver` system
  - Added `spark.sql.caseSensitive` configuration (default: `false`, case-insensitive, matching PySpark)
  - Added `Configuration.is_case_sensitive()` method for checking case sensitivity setting
  - Created `sparkless.core.column_resolver.ColumnResolver` for centralized column name resolution
  - All column resolution now goes through `ColumnResolver.resolve_column_name()` respecting session configuration
  - Ambiguity detection: raises `AnalysisException` when multiple columns differ only by case (in case-insensitive mode)
  - Updated all DataFrame operations (select, filter, groupBy, join, etc.) to use centralized resolver
  - Updated Polars backend (operation executor, expression translator, materializer) to use resolver
  - Updated SchemaManager, JoinService, AggregationService, and all validation logic
  - Comprehensive test coverage: 34 unit tests for case variations, 17 integration tests for case sensitivity configuration
- **Issue #247** - Added `elementType` keyword argument support to `ArrayType` for PySpark compatibility
  - `ArrayType(elementType=StringType())` now works (PySpark convention)
  - Maintains backward compatibility with positional `element_type` parameter
  - Added comprehensive test suite (32 tests) covering edge cases and PySpark parity
- **Issue #260** - Implemented `Column.eqNullSafe` for null-safe equality comparisons
  - Added `eqNullSafe` method to the `Column` and `Literal` APIs, matching PySpark semantics (treats `NULL` <=> `NULL` as `True`)
  - Updated Polars backend comparison coercion and join handling to support null-safe equality alongside existing numeric and datetime coercion
  - Added focused regression tests (including string, integer, float, date, and timestamp columns) plus optional PySpark parity tests for column–column and column–literal comparisons
  - Documented `eqNullSafe` behavior and usage in `api_reference.md`, `getting_started.md`, and `function_api_audit.md`
- **Issue #261** - Implemented full support for `Column.between()` API
  - Added `between` operation translation in Polars backend using `is_between()` with inclusive bounds (`closed="both"`)
  - Implemented Python fallback evaluator `_func_between` for row-wise evaluation when Polars backend cannot handle the operation
  - Added comprehensive test suite (13 test cases) covering:
    - Basic between functionality with inclusive bounds
    - Various data types (int, float, string, date)
    - Null handling (PySpark behavior: returns None for NULL values)
    - Literal bounds using `F.lit()`
    - Column-based bounds (per-row evaluation)
    - Usage in select expressions and when/otherwise constructs
    - PySpark parity verification
  - Documented `between` behavior and usage in `api_reference.md` with examples
  - Implementation matches PySpark's behavior: `between` is inclusive on both ends (`lower <= value <= upper`)

### Changed
- **Code Quality** - All CI checks now passing (ruff format, ruff check, mypy)
  - Improved type annotations for better mypy compliance
  - Cleaned up unused imports and type ignore comments
  - Enhanced type safety in DataFrame operations
  - Applied `ruff format`/`ruff check` and mypy cleanups for new `eqNullSafe` tests and supporting code

### Testing
- Added 34 unit tests for case-insensitive column resolution covering all DataFrame operations
- Added 17 integration tests for case sensitivity configuration (case-insensitive and case-sensitive modes)
- Added 32 new tests for ArrayType elementType support
  - Basic keyword argument tests (10 tests)
  - Robust tests covering all primitive types, nested arrays, complex types, and DataFrame operations (22 tests)
- Added 13 new tests for `between` functionality
- Added 4 new tests for `ArrayType` positional arguments
- Added regression tests covering `isnan()` on string columns, numeric NaN behavior, and `NULL` handling
- All tests pass in both Sparkless and PySpark modes for full compatibility validation
- All 1309 tests passing (up from 1105), 12 skipped, 0 xfailed (down from 1)

### Technical Details
- Updated `ArrayType.__init__()` to accept both `elementType` (camelCase, PySpark) and `element_type` (snake_case, backward compat) keyword arguments, and to detect and handle bool values incorrectly matched to `elementType` parameter
- Enhanced `fillna()` in `MiscService` to materialize lazy DataFrames before processing rows
- Updated `PolarsExpressionTranslator._translate_operation()` to handle `between` operation with tuple bounds `(lower, upper)`
- Added support for translating various bound types: ColumnOperation, Column, Literal, and direct values (int, float, bool, str, datetime, date)
- Enhanced `ExpressionEvaluator._func_between()` to handle row-wise evaluation with proper null handling
- Added `between` to the list of special operations that should not be routed to generic function call translation
- Fixed type annotations across multiple modules for improved type safety
- All code quality checks passing (ruff format, ruff check, mypy type checking)

## 3.23.0 — 2025-01-14

### Fixed
- **Issue #225** - Fixed string-to-numeric type coercion for comparison operations (==, !=, <, <=, >, >=), matching PySpark behavior
- **Issue #226** - Fixed `isin()` method to support `*values` arguments for backward compatibility, with improved type coercion for mixed types
- **Issue #227** - Fixed `getItem()` method to properly handle out-of-bounds array and map access, returning `None` instead of raising errors (PySpark compatibility)
- **Issue #228** - Fixed `regexp_extract()` to include fallback support for regex patterns with look-ahead and look-behind assertions using Python's `re` module when Polars native support is unavailable
- **Issue #229** - Fixed Pandas DataFrame support: `createDataFrame()` now properly recognizes real pandas DataFrames using duck typing, bypassing the mock pandas module when needed
- **Issue #230** - Fixed case-insensitive column name matching across all DataFrame operations (select, filter, withColumn, groupBy, orderBy, etc.), matching PySpark behavior
- **Issue #231** - Fixed `simpleString()` method implementation for all DataType classes (StringType, IntegerType, ArrayType, MapType, StructType, etc.), returning PySpark-compatible string representations
- **SQL JOIN Parsing** - Fixed SQL JOIN condition parsing to correctly extract and validate join column names
- **select() Validation** - Fixed `select()` validation to skip ColumnOperation expressions (like `F.size(col)`, `F.abs(col)`), preventing false column not found errors

### Changed
- **Code Quality** - Applied ruff formatting and fixed all linting issues
- **Type Safety** - Fixed mypy type errors in transformation_service, join_service, and executor
- **Test Coverage** - All 50 tests in `test_issues_225_231.py` now passing, including 4 pandas DataFrame support tests

### Testing
- All 572 tests passing, 4 skipped
- Comprehensive test coverage for all fixed issues
- All CI checks passing (ruff format, ruff lint, mypy type checking)

### Technical Details
- Updated `PolarsExpressionTranslator` to handle `isin` type coercion and `getItem` out-of-bounds access
- Enhanced `dataframe_factory.py` to recognize real pandas DataFrames using duck typing
- Improved `TransformationService.select()` to validate ColumnOperation expressions correctly
- Fixed SQL JOIN parsing in `SQLExecutor` to properly extract join column names
- Updated `ColumnValidator` to support case-insensitive column matching across all operations

## 3.21.0 — 2025-01-10

### Fixed
- **Issue #212** - Fixed `DataFrame.select()` to properly handle lists of `psf.col()` statements, matching PySpark's behavior
- **Issue #213** - Fixed `createDataFrame()` to support single `DataType` schemas with `toDF()` syntax (e.g., `createDataFrame([date1, date2], DateType()).toDF("dates")`)
- **Issue #214** - Fixed `df.sort()` and `df.orderBy()` to properly handle list parameters (e.g., `df.sort(["col1", "col2"])`)
- **Issue #215** - Fixed `sparkless.sql.Row` to support kwargs-style initialization (e.g., `Row(name="Alice", age=30)`)
- **List Unpacking** - Fixed `groupBy()`, `rollup()`, and `cube()` methods to properly unpack list/tuple arguments, matching PySpark compatibility
- **Type System** - Fixed Python 3.8 compatibility by replacing `|` union syntax with `Union` type hints and using `typing` module generics
- **Type Mapping** - Fixed unsigned integer type mapping (`UInt32`, `UInt64`, `UInt16`, `UInt8`) to convert to signed equivalents (`IntegerType`, `LongType`, `ShortType`, `ByteType`)
- **Function Return Types** - Fixed `size()`, `length()`, `bit_length()`, and `octet_length()` functions to return `Int64` instead of `UInt32` for PySpark compatibility
- **Date/Time Type Preservation** - Fixed Polars materializer to preserve `datetime.date` and `datetime.datetime` objects when converting back to Row objects (Polars `to_dicts()` converts them to strings)
- **Schema Inference** - Fixed schema inference to correctly identify `DateType` for `datetime.date` objects
- **LongType.typeName()** - Fixed `LongType.typeName()` to return `"long"` instead of `"bigint"` for PySpark compatibility

### Changed
- **Code Formatting** - Applied ruff code formatting across all files, ensuring consistent code style
- **Type Annotations** - Improved type annotations for better Python 3.8 compatibility and mypy validation
- **Row Object Handling** - Enhanced `createDataFrame()` to properly handle Row objects initialized with kwargs
- **DataFrame Factory** - Improved data validation to accept Row objects, lists, tuples, and dictionaries as positional rows

### Testing
- Added comprehensive test coverage for issues #212, #213, #214, #215
- Added test coverage for `groupBy()`, `rollup()`, and `cube()` with list parameters
- All tests passing with full test suite execution
- All CI checks passing (ruff format, ruff lint, mypy type checking)

### Technical Details
- Updated `select()`, `sort()`, `orderBy()` methods in both `transformation_service.py` and `transformations/operations.py` to unpack list arguments
- Enhanced `dataframe_factory.py` to handle single `DataType` schemas and Row object conversion
- Modified `schema_inference.py` to correctly infer `DateType` for `datetime.date` objects
- Updated `polars/materializer.py` to preserve date/timestamp types when converting Polars DataFrames to Row objects
- Added type mapper support for unsigned integer types from Polars
- Enhanced expression translator to cast `size()`, `length()`, `bit_length()`, and `octet_length()` results to `Int64`

### Release
- Released version 3.21.0 to PyPI: https://pypi.org/project/sparkless/3.21.0/
- Package available for installation: `pip install sparkless==3.21.0`

## 3.14.0 — 2025-01-XX

### Fixed
- **Drop Operation Improvements** - Enhanced drop operation to match PySpark behavior
  - Fixed handling of non-existent columns: drop operation now silently ignores non-existent columns (matching PySpark behavior)
  - Fixed dropping all columns: row count is now preserved when all columns are dropped, matching PySpark's behavior
  - Improved drop operation integration with lazy evaluation system
- **Union Operation Type Handling** - Fixed type compatibility issues in union operations
  - Fixed union operations to use correct dtype when adding missing columns (prevents `SchemaError` with Null types)
  - Union operations now properly handle type promotion (e.g., Int64, Float64) when concatenating DataFrames
- **DataFrame Materialization** - Improved support for tuple/list format data
  - Materializer now properly handles both dict and tuple/list formats when creating Polars DataFrames
  - Fixed schema inference when data is provided in tuple format with explicit schema
- **Type System Improvements** - Fixed all mypy type checking errors
  - Fixed `ISession` protocol attribute access issues by using proper `getattr()` fallback patterns
  - Fixed type narrowing issues in `GroupedData.agg()` method for union types
  - Fixed attribute access errors in `Catalog`, `DataFrameReader`, and `SQLExecutor` classes
  - Removed duplicate method definition (`_add_error_rule`) in `SparkSession`
  - Added proper type annotations and ignores for complex union type scenarios
  - All 173 source files now pass mypy type checking with Python 3.11

### Changed
- **Code Quality** - Comprehensive code quality improvements
  - Removed all debug logging from materializer and lazy evaluation engine
  - Improved code formatting and linting (all files pass ruff format and check)
  - Enhanced type safety across the codebase
  - Improved error handling for edge cases in drop and union operations

### Testing
- Fixed `test_drop_all_columns` and `test_drop_nonexistent_column` tests
- Fixed `test_union_with_compatible_numeric_types_succeeds` and `test_union_with_float_and_double_types_succeeds` tests
- Skipped `test_withcolumn_drop_withcolumn_chain` test due to known Polars schema dtype mismatch limitation
- All 1340+ tests passing (38 skipped)
- All files pass mypy type checking with Python 3.11
- All files pass ruff format and lint checks
- Code coverage: 51% overall

### Technical Details
- Updated `PolarsMaterializer` to handle tuple/list data formats with proper schema field mapping
- Enhanced `apply_union` in `PolarsOperationExecutor` to use correct dtypes when adding missing columns
- Improved drop operation logic to filter out non-existent columns and preserve row count when all columns are dropped
- Added type ignore comments for legitimate type narrowing scenarios where mypy cannot infer types after isinstance() checks
- Fixed storage access patterns to work with `ISession` protocol by using `getattr()` with fallback to `catalog._storage`

## 3.13.0 — 2025-12-XX

### Removed
- **Removed PySpark Alias Import Feature** - Removed the `from pyspark.sql import ...` namespace package feature
  - Deleted `sparkless/pyspark/` namespace package directory and all related files
  - Removed pyspark namespace registration from `sparkless/__init__.py`
  - Removed `"pyspark*"` from package includes in `pyproject.toml`
  - Deleted test files: `test_pyspark_namespace_imports.py` and `test_pyspark_drop_in_replacement_comprehensive.py`
  - Removed pyspark namespace-specific test methods from compatibility test files
  - Note: `from sparkless.sql import ...` imports continue to work as before
  - Note: `getActiveSession()` and `createDatabase()` improvements remain, just without pyspark namespace support

### Testing
- All 1330+ tests passing (40 skipped)
- All files pass mypy type checking with Python 3.11
- All files pass ruff format and lint checks
- Code coverage: 51% overall

## 3.12.0 — 2025-12-XX

### Added
- **PySpark Drop-in Replacement Improvements** - Comprehensive compatibility enhancements to ensure sparkless behaves exactly like PySpark in testing scenarios
  - String concatenation with `+` operator now returns `None` when DataFrame is cached, matching PySpark behavior
  - Empty DataFrame validation now requires explicit schema (raises `ValueError` if schema not provided)
  - Union operations now enforce strict schema compatibility (column count, names, and types must match)
  - Type system compatibility: Sparkless types now inherit from PySpark types when available for better compatibility
  - SQL expression parsing for `F.expr()` with proper SQL syntax support (e.g., `"id IS NOT NULL"`, `"age > 18"`)
  - Py4J error compatibility layer (`MockPy4JJavaError`) for error handling compatibility
  - Performance mode support (`fast`/`realistic`) for JVM overhead simulation in SparkSession
  - Enhanced catalog API compatibility with proper Database object attributes
- **New Modules**
  - `sparkless.core.exceptions.py4j_compat` - Py4J error compatibility layer
  - `sparkless.functions.core.sql_expr_parser` - SQL expression parser for `F.expr()`
- **Comprehensive Test Suite**
  - New test file `test_pyspark_drop_in_replacement.py` covering all compatibility improvements
  - Tests for caching behavior, empty DataFrames, union operations, SQL parsing, type compatibility
  - Tests for performance mode, catalog API, and error handling compatibility

### Changed
- **Caching Behavior**: DataFrame caching now properly tracks cached state and applies post-processing for string concatenation
- **Type System**: All data types (StringType, IntegerType, etc.) now inherit from PySpark DataType when available
- **SQL Expression Parsing**: `F.expr()` now parses SQL expressions instead of storing raw strings, with fallback for backward compatibility
- **Empty DataFrame Handling**: Empty DataFrames now require explicit schema to match PySpark behavior
- **Union Operations**: Enhanced validation to check column count, names, and type compatibility
- **Expression Evaluation**: Improved condition evaluator and expression evaluator with DataFrame context awareness
- **Type Validation**: `to_timestamp()` now strictly requires StringType input (with explicit cast support)
- Fixed `IntegerType.typeName()` to return `"int"` instead of `"integer"` for PySpark compatibility

### Fixed
- Fixed string concatenation with `+` operator to return `None` when DataFrame is cached (PySpark compatibility)
- Fixed empty DataFrame creation to require explicit schema when data is empty
- Fixed union operations to properly validate schema compatibility
- Fixed `to_timestamp()` type validation to accept only StringType (with cast support)
- Fixed condition evaluator and expression evaluator to properly track DataFrame context
- Fixed type system to properly inherit from PySpark types when available
- Fixed `IntegerType.typeName()` return value for PySpark compatibility

### Removed
- Removed `array_distinct` function feature due to complex materialization issues with chained operations
  - Function implementation remains in codebase but is not exported
  - All `array_distinct` tests are now skipped

### Testing
- All 1330+ tests passing (40 skipped)
- All files pass mypy type checking with Python 3.11
- All files pass ruff format and lint checks
- Code coverage: 51% overall
- Comprehensive compatibility test suite added for PySpark drop-in replacement scenarios

## 3.11.0 — 2025-12-10

### Added
- Lazy evaluation for session-aware functions (`current_database()`, `current_schema()`, `current_user()`, `current_catalog()`)
  - Literals now resolve session state at evaluation time, not creation time, matching PySpark behavior
  - Session-aware functions properly reflect the active session's catalog state during DataFrame operations
- Session validation for all functions requiring an active SparkSession
  - Functions now validate session availability at creation time (matching PySpark error behavior)
  - Improved error messages for missing session scenarios
- Comprehensive test coverage for session isolation and validation
  - `test_sparkcontext_validation.py` - validates session dependency requirements
  - `test_column_availability.py` - tests column materialization behavior
  - `test_fixture_compatibility.py` - verifies fixture/setup compatibility
  - `test_function_api_compatibility.py` - validates function API signatures
  - `test_type_strictness.py` - tests strict type checking for datetime functions

### Changed
- Session-aware functions now use lazy literal resolution via resolver functions
- Expression evaluator and Polars translator updated to resolve lazy literals during evaluation
- Improved type annotations: replaced `callable` with `Callable[[], Any]` for better mypy compatibility
- Enhanced `SparkColumnNotFoundError` with optional custom message support

### Fixed
- Fixed `test_current_helpers_are_session_isolated` to properly capture original session before `newSession()`
- Fixed type checking issues in `TransformationOperations` mixin for `_validate_operation_types` method
- Fixed strict type validation for `to_timestamp()` and `to_date()` to accept both StringType and native types (TimestampType/DateType)
- Fixed column availability tracking to correctly update after DataFrame materialization
- Fixed lazy literal evaluation to resolve session state dynamically at evaluation time

### Testing
- All 1209 tests passing (46 skipped)
- All files pass mypy type checking with Python 3.11
- All files pass ruff format and lint checks
- Code coverage: 50% overall

## 3.10.0 — 2025-01-XX

### Added
- Comprehensive type safety improvements across the codebase
- Improved protocol type definitions with Union types instead of Any
- Enhanced type annotations for better IDE support and static analysis

### Changed
- Aligned mypy.ini configuration with pyproject.toml settings
- Replaced `Any` type aliases with proper Union types for `ColumnExpression`, `AggregateExpression`
- Improved protocol method signatures to use `ColumnExpression` instead of `Any`
- Enhanced TypeConverter return types from `Any` to Union types
- Removed module-level error ignoring for `display.operations`, `joins.operations`, and `operations.misc`

### Fixed
- Fixed type ignore comments with proper type narrowing in datetime functions
- Fixed SQLAlchemy helper function return types
- Fixed PySpark compatibility layer typing issues
- Fixed `timestamp_seconds()` to preserve Literal objects correctly
- Improved type safety in display, join, and misc operations modules

### Testing
- All tests passing (1095 passed, 47 skipped)
- All modified files pass mypy type checking
- Code formatted and linted with ruff

## 3.9.1 — 2025-01-XX

### Fixed
- Fixed timezone handling in `from_unixtime()` and `timestamp_seconds()` functions to interpret Unix timestamps as UTC and convert to local timezone, matching PySpark behavior.
- Fixed CI performance test job to handle cases where no performance tests are found (exit code 5).

### Changed
- Improved CI test execution with parallel test runs using pytest-xdist, significantly reducing CI execution time.
- Added `pytest-xdist>=3.0.0` to dev dependencies for parallel test execution.

### Testing
- All compatibility tests now passing with proper timezone handling.
- CI tests now run in parallel across 4 test groups (unit, compatibility, performance, documentation).

## 3.9.0 — 2025-12-02

### Added
- Complete implementation of all 11 window functions with proper partitioning and ordering support:
  - `row_number()`, `rank()`, `dense_rank()` - ranking functions
  - `cume_dist()`, `percent_rank()` - distribution functions
  - `lag()`, `lead()` - offset functions
  - `first_value()`, `last_value()`, `nth_value()` - value functions
  - `ntile()` - bucket function
- Python fallback mechanism for window functions not natively supported in Polars backend (cume_dist, percent_rank, nth_value, ntile).
- Enhanced window function evaluation with proper tie handling for rank-based calculations.

### Fixed
- Fixed `nth_value()` to return NULL for rows before the nth position, matching PySpark behavior.
- Fixed `cume_dist()` and `percent_rank()` calculations to correctly handle ties using rank-based calculations.
- Fixed window function results alignment when DataFrame is sorted after evaluation.
- Fixed mypy type error in `MiscellaneousOperations` by accessing columns via schema instead of direct property access.
- Fixed syntax errors in `window_execution.py` that prevented proper module import.

### Changed
- Window functions now use Python evaluation fallback when Polars backend doesn't support them, ensuring correct PySpark-compatible behavior.
- Improved window function partitioning and ordering logic to handle edge cases (single-row partitions, ties, etc.).

### Testing
- All 11 window function compatibility tests now passing (previously 7 passing, 4 skipped).
- Full test suite: 1088 tests passing with 47 expected skips.

## 3.7.0 — 2025-01-XX

### Added
- Full SQL DDL/DML support: `CREATE TABLE`, `DROP TABLE`, `INSERT INTO`, `UPDATE`, and `DELETE FROM` statements are now fully implemented in the SQL executor.
- Enhanced SQL parser with comprehensive support for DDL statements including column definitions, `IF NOT EXISTS`, and `IF EXISTS` clauses.
- Support for `INSERT INTO ... VALUES (...)` with multiple rows and `INSERT INTO ... SELECT ...` sub-queries.
- `UPDATE ... SET ... WHERE ...` statements with Python-based expression evaluation for WHERE conditions and SET clauses.
- `DELETE FROM ... WHERE ...` statements with Python-based condition evaluation.

### Changed
- SQL executor now handles DDL/DML operations by directly interacting with the storage backend, bypassing DataFrame expression translation for complex SQL operations.
- Improved error handling in SQL operations with proper exception types and messages.

### Fixed
- Fixed recursion error in `DataFrame._project_schema_with_operations` by using `_schema` directly instead of the `schema` property.
- Fixed `UnboundLocalError` in SQL executor by removing shadowing local imports of `StructType`.
- Removed unused imports and improved code quality with ruff linting fixes.

### Documentation
- Updated SQL executor docstrings to reflect full DDL/DML implementation status.
- README "Recent Updates" highlights the new SQL DDL/DML capabilities.

## 3.6.0 — 2025-11-13

### Added
- Feature-flagged profiling utilities in `sparkless.utils.profiling`, with Polars execution and
  expression hot paths instrumented via lightweight decorators.
- Optional native pandas backend selection through `MOCK_SPARK_PANDAS_MODE`, including a benchmarking
  harness at `scripts/benchmark_pandas_fallback.py`.

### Changed
- The query optimizer now supports adaptive execution simulation, inserting configurable
  `REPARTITION` operations when skew metrics indicate imbalanced workloads.

### Documentation
- Published performance guides covering hot-path profiling (`docs/performance/profiling.md`) and
  pandas fallback benchmarking (`docs/performance/pandas_fallback.md`).
- README “Recent Updates” highlights the profiling, adaptive execution, and pandas backend features.

## 3.5.0 — 2025-11-13

### Added
- Session-aware helper functions in `sparkless.functions`: `current_catalog`, `current_database`,
  `current_schema`, and `current_user`, plus a dynamic `call_function` dispatcher.
- Regression tests covering the new helpers and dynamic dispatch, ensuring PySpark-compatible error
  handling.
- Pure-Python statistical fallbacks (`percentile`, `covariance`) to remove the dependency on native
  wheels when running documentation and compatibility suites.

### Changed
- The Polars storage backend and `UnifiedStorageManager` now track the active schema so
  `setCurrentDatabase` updates propagate end-to-end.
- `SparkContext.sparkUser()` mirrors PySpark’s context helper, allowing the new literal functions to
  surface the current user.

### Documentation
- README and quick-start docs updated for version 3.5.0 and the session-aware catalogue features.
- Internal upgrade summary documents the stability improvements and successful full-suite run.

## 3.4.0 — 2025-11-12

### Changed
- Standardised local workflows around `bash tests/run_all_tests.sh`, Ruff, and MyPy via updated Makefile targets and `install.sh`.
- Introduced GitHub Actions CI that enforces linting, type-checking, and full-suite coverage on every push and pull request.
- Refreshed development docs to reflect the consolidated tooling commands.

### Added
- Published `plans/typing_delta_roadmap.md`, outlining phased mypy cleanup and Delta feature milestones for the next release cycle.

### Documentation
- README “Recent Updates” highlights the 3.4.0 workflow improvements and roadmap visibility.

## 3.3.0 — 2025-11-12

### Added
- Consolidated release metadata so `pyproject.toml`, `sparkless/__init__.py`, and published wheels all advertise version `3.3.0`.
- Documented the renumbering from the legacy 3.x preview series to the semantic 0.x roadmap, keeping downstream consumers aligned with public messaging.
- Updated README badges and compatibility tables to reflect the curated 396-test suite and PySpark 3.2–3.5 coverage.

### Changed
- Finalised the migration to Python 3.8-compatible typing throughout the Polars executor,
  DataFrame reader/writer, schema manager, and Delta helpers so that `mypy sparkless`
  now completes without suppressions.
- Consolidated type-only imports behind `TYPE_CHECKING` guards, reducing import
  overhead while keeping tooling visibility intact.

### Fixed
- Ensured Python-evaluated projection columns always materialise with string aliases,
  preventing accidental `None` column names when fallback expressions run outside Polars.
- Normalised optional alias handling inside the Delta merge builder, avoiding runtime
  `None` lookups when accessing assignment metadata.

### Documentation
- README “Recent Updates” highlights the metadata realignment for 3.3.0 and the clean `mypy` status.
- Refreshed version references to 3.3.0 across project metadata.

## 3.2.0 — 2025-11-12

### Changed
- Lowered the minimum supported Python version to 3.8 and aligned Black, Ruff, and mypy
  targets so local tooling matches the published wheel.
- Added `typing_extensions` dependency for Python 3.8 compatibility and used `from __future__ import annotations`
  for deferred type evaluation.
- Standardised type hints on `typing` module generics (`List[str]`, `Dict[str, Any]`, `Tuple[...]`) and
  `typing` protocols across the codebase for Python 3.8 compatibility.
- Adopted `ruff format` as the canonical formatter, bringing the entire repository in line with
  the Ruff style guide.

### Documentation
- Updated the README to call out the Python 3.8 baseline and refreshed the "Recent Updates"
  section with the typing/tooling improvements delivered in 3.2.0.

## 3.1.0 — 2025-11-07

### Added
- Schema reconciliation for Delta `mergeSchema=true` appends on the Polars backend,
  preventing null-type collisions while preserving legacy data.
- Datetime compatibility helpers in `sparkless.compat.datetime` for producing
  stable string outputs when downstream code expects substrings.
- Configurable backend selection via constructor overrides, the
  `SPARKLESS_BACKEND` environment variable, or `SparkSession.builder.config`.
- Regression tests covering schema evolution, datetime normalisation, backend
  selection, and compatibility helpers.
- Protocol-based DataFrame mixins (`SupportsDataFrameOps`) enabling structural typing and
  a clean mypy run across 260 modules.
- Ruff lint configuration and cast/typing cleanups so that `ruff check` passes repository-wide.

## 3.0.0 — 2025-09-12

### Added
- Polars backend as the new default execution engine, delivering thread-safe, high-performance
  DataFrame operations without JVM dependencies.
- Parquet-based table persistence with `saveAsTable`, including catalog synchronisation and
  cross-session durability via `db_path`.
- Comprehensive backend selection via environment variables, builder configuration, and constructor overrides.
- New documentation covering backend architecture, migration guidance from v2.x, and configuration options.

### Changed
- Migrated window functions, joins, aggregations, and lazy evaluation to Polars-powered implementations
  while maintaining PySpark-compatible APIs.
- Updated test harness and CI scripts to exercise the Polars backend, increasing the regression suite to
  600+ passing tests.

### Removed
- Legacy DuckDB-backed SQL translation layer (`sqlglot` dependency, Mock* prefixed classes) in favour of
  the unified protocol-based backend architecture.

### Documentation
- Introduced `docs/backend_selection.md` describing backend options, environment
  overrides, and troubleshooting tips.
- Documented merge-schema limitations and datetime helper usage in
  `docs/known_issues.md`.

### Known Issues
- Documentation example tests invoke the globally installed `sparkless`
  distribution. When a different version is installed in `site-packages`, the
  example scripts exit early with `ImportError`. Align the executable path or
  install the local wheel before running documentation fixtures.

