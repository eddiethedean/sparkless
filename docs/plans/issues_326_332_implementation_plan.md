# Implementation Plan: Issues 326-332

**Created:** 2025-01-23  
**Status:** In Progress  
**Target Version:** 3.28.0

## Overview

This document outlines the implementation plan for fixing 7 PySpark compatibility issues (GitHub issues #326-332) reported by Zarimax. All issues are related to missing API features or incorrect behavior compared to PySpark.

## Progress Summary

**Overall Status:** 4 of 5 PRs completed (80%)

| PR | Status | Issues | Completion Date |
|----|--------|--------|------------------|
| PR 1 | ✅ Completed | #326, #328 | 2025-01-23 (PR #333) |
| PR 2 | ✅ Completed | #329 | 2025-01-23 (PR #334) |
| PR 3 | ✅ Completed | #327 | 2025-01-23 (PR #338) |
| PR 4 | ✅ Completed | #330, #332 | 2025-01-23 (PR #340) |
| PR 5 | ⏳ Pending | #331 | - |

## Issues Summary

| Issue | Title | Category | Priority | Status |
|-------|-------|----------|----------|--------|
| #326 | Missing `format_string` function | String Functions | Medium | ✅ Fixed |
| #327 | `orderBy()` missing `ascending` parameter | DataFrame Methods | Low | ✅ Fixed |
| #328 | `split()` missing limit parameter | String Functions | Medium | ✅ Fixed |
| #329 | `log()` doesn't handle float constants | Math Functions | Low | ✅ Fixed |
| #330 | Struct field selection with alias fails | Column Resolution | High | ✅ Fixed |
| #331 | Join doesn't support `array_contains()` condition | Join Operations | Medium | ⏳ Pending |
| #332 | Column resolution fails with cast+alias+select | Column Resolution | High | ✅ Fixed |

## PR Grouping Strategy

The issues have been grouped into 5 logical pull requests based on:
- **Related functionality** - Issues affecting similar code paths
- **Dependencies** - Issues that may share implementation details
- **Complexity** - Balancing PR size and reviewability
- **Testability** - Grouping issues that can be tested together

---

## PR 1: String Function Enhancements ✅ COMPLETED

**Title:** `Add format_string function and split() limit parameter support`

**Issues:** #326, #328  
**Priority:** Medium  
**Estimated Complexity:** Medium  
**Dependencies:** None  
**Status:** ✅ Merged (PR #333)  
**Completed:** 2025-01-23

### Issue #326: Missing `format_string` function

**Problem:**
- `F.format_string()` is defined in `functions.py` but not implemented in the Polars backend
- Raises `ValueError: Unsupported function: format_string`

**Expected Behavior:**
```python
df.withColumn("NewValue", F.format_string(
    "%s-%s",
    F.col("StringValue"),
    F.col("IntegerValue")
))
# Should produce: "abc-123", "def-456"
```

**Implementation Tasks:**
1. ✅ Verify `format_string` exists in `sparkless/functions/functions.py` (line 306)
2. ✅ Add `format_string` Python evaluation fallback in `PolarsOperationExecutor`
3. ✅ Implement Python fallback (using `ExpressionEvaluator` which already had support)
4. ✅ Fixed `ExpressionEvaluator._evaluate_format_string()` to include first column value
5. ✅ Handle variable number of column arguments
6. ✅ Support format specifiers: `%s`, `%d`, `%f`, `%x`, `%o`, etc.
7. ✅ Fixed null handling: PySpark converts None to "null" string

**Files Modified:**
- ✅ `sparkless/backend/polars/expression_translator.py` - Added format_string fallback
- ✅ `sparkless/backend/polars/operation_executor.py` - Added format_string to Python fallback list
- ✅ `sparkless/dataframe/evaluation/expression_evaluator.py` - Fixed format_string evaluation

**Test Cases:**
- ✅ Basic format_string with string and integer columns
- ✅ Multiple format specifiers (%s, %d, %f, %x, %o)
- ✅ Null value handling (None -> "null" string)
- ✅ Edge cases: empty strings, special characters, Unicode, very long strings
- ✅ PySpark parity tests (3 tests)
- ✅ Comprehensive test coverage: 15 unit tests total

---

### Issue #328: `split()` missing limit parameter

**Problem:**
- `split()` only accepts 2 parameters (column, delimiter)
- PySpark supports optional 3rd parameter (limit) to control number of splits
- Raises `TypeError: Functions.split() takes 2 positional arguments but 3 were given`

**Expected Behavior:**
```python
F.split(F.col("StringValue"), ",", 3)
# "A,B,C,D,E,F" -> ["A", "B", "C,D,E,F"]  # Only 3 splits
```

**Implementation Tasks:**
1. ✅ Update `StringFunctions.split()` signature to accept optional `limit` parameter
2. ✅ Update `Functions.split()` wrapper
3. ✅ Implement Python fallback for split with limit (Polars doesn't support limit natively)
4. ✅ Update function name generation to include limit when provided
5. ✅ Handle default behavior (limit=-1 means no limit, limit=0 also means no limit, limit=1 means no split)
6. ✅ Fixed limit logic: PySpark's limit means "maximum number of parts", not "number of splits"

**Files Modified:**
- ✅ `sparkless/functions/string.py` - Updated `split()` method signature
- ✅ `sparkless/functions/functions.py` - Updated wrapper
- ✅ `sparkless/backend/polars/expression_translator.py` - Implemented Python fallback for limit

**Test Cases:**
- ✅ Basic split with limit
- ✅ Limit = 1, 2, 3, 0, -1, etc.
- ✅ Limit larger than actual splits
- ✅ Limit = -1 (no limit, default behavior)
- ✅ Limit = 0 (treated as no limit in PySpark)
- ✅ Null value handling
- ✅ Empty strings
- ✅ Multi-character delimiters, special regex chars, Unicode
- ✅ Leading/trailing delimiters, consecutive delimiters
- ✅ PySpark parity tests (4 tests)
- ✅ Comprehensive test coverage: 21 unit tests total

---

## PR 2: Math Function Fix ✅ COMPLETED

**Title:** `Fix log() function to support float constants as base argument`

**Issues:** #329  
**Priority:** Low  
**Estimated Complexity:** Low  
**Dependencies:** None  
**Status:** ✅ Merged (PR #334)  
**Completed:** 2025-01-23

### Issue #329: `log()` doesn't handle float constants

**Problem:**
- `log()` assumes base argument is always a Column object
- Fails with `AttributeError: 'float' object has no attribute 'name'` when float is passed
- PySpark supports both Column and constant values

**Expected Behavior:**
```python
F.log(10.0, F.col("Value"))  # Base 10 logarithm
# Should work with float constant as base
```

**Implementation Tasks:**
1. ✅ Updated `MathFunctions.log()` to handle PySpark's two signatures: `log(column)` and `log(base, column)`
2. ✅ Updated function name generation to handle constant bases
3. ✅ Added `_log_expr()` helper in `PolarsExpressionTranslator` to handle base parameter
4. ✅ Fixed Polars translation to compute log_base(value) = log(value) / log(base) for constant bases
5. ✅ Used `pl.lit()` for constant bases and column translation for Column bases

**Files Modified:**
- ✅ `sparkless/functions/math.py` - Fixed `log()` method to handle both signatures
- ✅ `sparkless/functions/functions.py` - Updated wrapper signature
- ✅ `sparkless/backend/polars/expression_translator.py` - Added `_log_expr()` helper method

**Test Cases:**
- ✅ `F.log(10.0, F.col("Value"))` - Float constant base
- ✅ `F.log(2, F.col("Value"))` - Integer constant base
- ✅ `F.log(F.col("Value"))` - Natural logarithm (backward compatibility)
- ✅ `F.log(F.col("base"), F.col("Value"))` - Column base (Sparkless extension)
- ✅ Null value handling
- ✅ Edge cases: different bases (2, 3, 10), usage in `withColumn` context
- ✅ PySpark parity tests (3 tests, 1 skipped for column base as PySpark doesn't support it)
- ✅ Comprehensive test coverage: 8 unit tests + 3 PySpark parity tests

---

## PR 3: DataFrame Method Enhancement ✅ COMPLETED

**Title:** `Add ascending parameter support to orderBy() method`

**Issues:** #327  
**Priority:** Low  
**Estimated Complexity:** Low  
**Dependencies:** None  
**Status:** ✅ Open (PR #338)  
**Completed:** 2025-01-23

### Issue #327: `orderBy()` missing `ascending` parameter

**Problem:**
- `orderBy()` doesn't accept `ascending` keyword argument
- Raises `TypeError: DataFrame.orderBy() got an unexpected keyword argument 'ascending'`
- PySpark supports `df.orderBy("col", ascending=True/False)`

**Expected Behavior:**
```python
df.orderBy("StringValue", ascending=True)   # Ascending order
df.orderBy("StringValue", ascending=False) # Descending order
```

**Implementation Tasks:**
1. ✅ Updated `orderBy()` signature in `TransformationOperations`, `TransformationService`, and `DataFrame` to accept `ascending` parameter (default: `True`)
2. ✅ Updated `sort()` alias to extract `ascending` from `**kwargs` and pass to `orderBy()`
3. ✅ Fixed `sort()` to unpack list/tuple arguments before calling `orderBy()` (maintains backward compatibility)
4. ✅ Modified `PolarsMaterializer` to extract `ascending` from orderBy payload
5. ✅ Payload format: `(columns, ascending)` tuple for new format, backward compatible with old format
6. ✅ Maintains compatibility with existing `F.asc()` and `F.desc()` usage

**Files Modified:**
- ✅ `sparkless/dataframe/transformations/operations.py` - Updated `orderBy()` and `sort()` signatures
- ✅ `sparkless/dataframe/services/transformation_service.py` - Updated `orderBy()` and `sort()` with list unpacking
- ✅ `sparkless/dataframe/dataframe.py` - Updated `orderBy()` signature
- ✅ `sparkless/backend/polars/materializer.py` - Handle `ascending` parameter in orderBy payload

**Test Cases:**
- ✅ `orderBy("col", ascending=True)` - Ascending order
- ✅ `orderBy("col", ascending=False)` - Descending order
- ✅ `orderBy("col")` - Default behavior (ascending)
- ✅ Multiple columns with ascending parameter
- ✅ Numeric and string columns
- ✅ Column objects
- ✅ `sort()` alias with ascending parameter
- ✅ Null value handling (nulls last)
- ✅ Backward compatibility
- ✅ Edge cases: empty DataFrame, single row, all nulls, mixed nulls, negative numbers, floating point, boolean columns, Unicode strings, special characters, very long strings
- ✅ Integration: chained operations, multiple orderBy calls, with limit, case-insensitive columns, three columns, duplicate values
- ✅ PySpark parity tests (3 tests)
- ✅ Comprehensive test coverage: 27 unit tests + 3 PySpark parity tests

---

## PR 4: Column Resolution & Schema Fixes ✅ COMPLETED

**Title:** `Fix column resolution for struct fields with aliases and cast+alias+select operations`

**Issues:** #330, #332  
**Priority:** High  
**Estimated Complexity:** Medium-High  
**Dependencies:** None (but both affect column resolution)  
**Status:** ✅ Merged (PR #340)  
**Completed:** 2025-01-23

### Issue #330: Struct field selection with alias fails

**Problem:**
- Selecting struct fields with alias doesn't work
- Raises `polars.exceptions.ColumnNotFoundError: unable to find column "StructValue.E1"`
- Works without alias: `F.col("StructValue.E1")` works, but `.alias("E1-Extract")` fails

**Expected Behavior:**
```python
df.select(F.col("StructValue.E1").alias("E1-Extract"))
# Should extract E1 from struct and alias it as "E1-Extract"
```

**Implementation Tasks:**
1. ✅ Fixed struct field extraction in `apply_select()` when alias is present
2. ✅ Ensured struct field path is resolved before alias is applied by checking `_original_column._name`
3. ✅ Updated column name tracking to handle aliased struct fields for both Column and ColumnOperation objects
4. ✅ Added None checks for type safety (mypy compliance)
5. ✅ Ensured Polars expression correctly extracts struct field before aliasing

**Files Modified:**
- ✅ `sparkless/backend/polars/operation_executor.py` - Fixed `apply_select()` for struct fields with aliases
  - Added logic to check `col._original_column._name` for struct field paths when column is aliased
  - Handles both Column and ColumnOperation objects with proper None checks
  - Struct field extraction happens before alias is applied

**Test Cases:**
- ✅ `F.col("StructValue.E1").alias("E1-Extract")` - Basic struct field with alias
- ✅ Multiple struct fields with aliases
- ✅ Struct fields in `withColumn` with aliases
- ✅ Struct field with alias combined with other columns
- ✅ Null struct values (all nulls, mixed nulls)
- ✅ Different data types (int, string, float, bool, null)
- ✅ Case sensitivity handling
- ✅ Special characters in field names
- ✅ Integration with joins, unions, groupBy, window functions
- ✅ Multiple select operations
- ✅ Schema verification
- ✅ Empty DataFrame handling
- ✅ Backward compatibility (without alias)
- ✅ Chained operations
- ✅ Comprehensive test coverage: 20 unit tests + 3 PySpark parity tests

---

### Issue #332: Column resolution fails with cast+alias+select

**Problem:**
- Column resolution fails when combining aggregation, cast, alias, and select
- Raises `SparkColumnNotFoundError: cannot resolve 'AvgValue'`
- Schema shows: `CAST(avg(Value) AS DOUBLETYPE(NULLABLE=TRUE))` instead of `AvgValue`
- Works without cast: `F.mean("Value").alias("AvgValue")` works

**Expected Behavior:**
```python
df.groupBy("Name").agg(
    F.mean("Value").cast(T.DoubleType()).alias("AvgValue")
).select("Name", "AvgValue")
# Should resolve "AvgValue" column name correctly
```

**Implementation Tasks:**
1. ✅ Fixed column name resolution in `GroupedData.agg()` to handle cast+alias combinations
2. ✅ Ensured schema projection tracks aliased column names correctly after cast operations
3. ✅ Updated `GroupedData.agg()` to check for alias name (`_alias_name` or `expr.name`) before generating CAST expression
4. ✅ Fixed column name tracking in aggregation operations with cast+alias
5. ✅ Ensured Polars backend preserves alias names through cast operations
6. ✅ Maintained backward compatibility: cast operations without alias still generate CAST expression format

**Files Modified:**
- ✅ `sparkless/dataframe/grouped/base.py` - Fixed aggregation column name tracking
  - Updated cast operation handling to check for alias before generating CAST expression name
  - Uses `expr._alias_name` or `expr.name` (which returns alias if present) as the result key
  - Distinguishes between alias names and column names to maintain backward compatibility

**Test Cases:**
- ✅ `F.mean("Value").cast(T.DoubleType()).alias("AvgValue")` then select "AvgValue"
- ✅ Multiple aggregations with cast+alias
- ✅ Different cast types (StringType, IntegerType, DoubleType)
- ✅ Cast+alias in `withColumn` then select
- ✅ Nested operations: cast+alias in groupBy then select
- ✅ All major aggregation functions (count, sum, avg, min, max) with cast+alias
- ✅ Integration with joins, unions, window functions, filters, orderBy, limit, distinct
- ✅ Multiple casts on same column with different aliases
- ✅ Schema verification (ensures alias name, not CAST expression)
- ✅ Empty DataFrame handling
- ✅ All null values, mixed null values
- ✅ Complex nested operations
- ✅ Backward compatibility (aggregation+alias without cast)
- ✅ Comprehensive test coverage: 20 unit tests + 3 PySpark parity tests

---

## PR 5: Join Condition Enhancement

**Title:** `Add support for array_contains() as join condition`

**Issues:** #331  
**Priority:** Medium  
**Estimated Complexity:** Medium  
**Dependencies:** None

### Issue #331: Join doesn't support `array_contains()` condition

**Problem:**
- Join operations only support column name(s) or simple ColumnOperation
- Doesn't support expression-based join conditions like `F.array_contains()`
- Raises `ValueError: Join keys must be column name(s) or a ColumnOperation`

**Expected Behavior:**
```python
df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
# Should join where df2.ID is contained in df1.IDs array
```

**Implementation Tasks:**
1. Update join validation to accept expression-based conditions
2. Implement expression evaluation for join conditions in Polars backend
3. Handle `array_contains` expression in join context
4. Support other expression-based join conditions (future-proofing)
5. Ensure proper null handling in expression-based joins
6. Update join materialization to evaluate expressions

**Files to Modify:**
- `sparkless/dataframe/services/join_service.py` - Accept expression-based join conditions
- `sparkless/backend/polars/operation_executor.py` - Implement `apply_join()` with expression evaluation
- `sparkless/core/condition_evaluator.py` - Evaluate `array_contains` in join context
- `sparkless/backend/polars/expression_translator.py` - Support `array_contains` in join conditions

**Test Cases:**
- `join(on=F.array_contains(df1.IDs, df2.ID))` - Basic array_contains join
- Different join types (inner, left, right, outer) with array_contains
- Multiple matches (array contains multiple matching IDs)
- No matches
- Null arrays and null IDs
- Complex expressions in join conditions
- PySpark parity test

---

## Implementation Order & Timeline

### Recommended Order:
1. ✅ **PR 1** (String Functions) - COMPLETED - Medium complexity, good foundation
2. **PR 2** (Math Function Fix) - Simplest, can be done next
3. **PR 3** (DataFrame Method) - Simple, independent
4. **PR 4** (Column Resolution) - Most complex, requires careful testing
5. **PR 5** (Join Enhancement) - Medium complexity, can be done in parallel with PR 4

### Estimated Timeline:
- ✅ **PR 1**: 3-4 hours (COMPLETED - Actual: ~4 hours)
- **PR 2**: 1-2 hours
- **PR 3**: 1-2 hours
- **PR 4**: 4-6 hours
- **PR 5**: 3-4 hours

**Total Estimated Time:** 12-18 hours  
**Time Remaining:** 8-14 hours

---

## Testing Strategy

### Unit Tests
Each PR should include:
- Basic functionality tests
- Edge case tests (nulls, empty values, etc.)
- Error handling tests
- Integration with existing features

### Parity Tests
All PRs should include PySpark parity tests:
- Test exact same code in both Sparkless and PySpark
- Compare outputs to ensure identical behavior
- Use `MOCK_SPARK_TEST_BACKEND` environment variable for switching

### Test File Structure
Create test files following existing patterns:
- ✅ `tests/test_issue_326_format_string.py` (15 tests)
- ✅ `tests/test_issue_328_split_limit.py` (21 tests)
- `tests/test_issue_329_log_float_constant.py`
- `tests/test_issue_327_orderby_ascending.py`
- `tests/test_issue_330_struct_field_alias.py`
- `tests/test_issue_332_cast_alias_select.py`
- `tests/test_issue_331_join_array_contains.py`

Parity tests:
- ✅ `tests/parity/functions/test_format_string_parity.py` (3 tests)
- ✅ `tests/parity/functions/test_split_limit_parity.py` (4 tests)
- `tests/parity/functions/test_log_float_constant_parity.py`
- `tests/parity/dataframe/test_orderby_ascending_parity.py`
- `tests/parity/dataframe/test_struct_field_alias_parity.py`
- `tests/parity/dataframe/test_cast_alias_select_parity.py`
- `tests/parity/dataframe/test_join_array_contains_parity.py`

---

## Code Quality Requirements

### Before Submitting PRs:
- ✅ All tests passing (unit + parity)
- ✅ Code formatted with `ruff format`
- ✅ Linting passes with `ruff check`
- ✅ Type checking passes with `mypy`
- ✅ Documentation updated if API changes
- ✅ CHANGELOG.md updated with fixes

### Code Review Checklist:
- [ ] Implementation matches PySpark behavior exactly
- [ ] Edge cases handled (nulls, empty values, etc.)
- [ ] Error messages match PySpark when possible
- [ ] Performance considerations addressed
- [ ] No breaking changes to existing API
- [ ] Tests cover all scenarios from issue descriptions

---

## Risk Assessment

### Low Risk:
- **PR 2** (log() fix) - Isolated change, minimal impact
- **PR 3** (orderBy ascending) - Simple parameter addition

### Medium Risk:
- **PR 1** (String functions) - New functionality, but well-contained
- **PR 5** (Join enhancement) - Affects join logic, but isolated

### High Risk:
- **PR 4** (Column resolution) - Affects core column resolution logic, could impact many operations
  - **Mitigation**: Extensive testing, especially regression tests for existing functionality

---

## Success Criteria

Each PR is considered complete when:
1. ✅ All issue reproduction tests pass
2. ✅ PySpark parity tests pass
3. ✅ No regressions in existing test suite
4. ✅ Code quality checks pass
5. ✅ Documentation updated
6. ✅ CHANGELOG updated

### PR 1 Completion Summary:
- ✅ All 36 tests passing (15 format_string + 21 split)
- ✅ All tests pass in both Sparkless and PySpark modes
- ✅ Code formatted with ruff
- ✅ Passes mypy type checking
- ✅ CHANGELOG.md updated
- ✅ PR #333 merged successfully
- ✅ All CI checks passed (lint-and-type, test-compatibility, test-documentation, test-parity, test-performance, test-unit)

---

## Notes

- All issues reported by Zarimax using `sparkless==3.27.0-dev` (Commit 5120eae)
- All issues are OPEN and have no comments yet
- Issues follow consistent format with example code and expected results
- All fixes should maintain backward compatibility

---

## Related Documentation

- [PySpark Function Matrix](../../PYSPARK_FUNCTION_MATRIX.md)
- [API Reference](../api_reference.md)
- [Testing Patterns](../testing_patterns.md)
- [Migration Guide](../migration_from_pyspark.md)

---

**Last Updated:** 2025-01-23  
**PR 1 Status:** ✅ Completed and merged (PR #333)  
**Next Review:** After PR 2 completion
