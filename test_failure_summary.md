# Test Failure Summary (pytest -n 12)

**Run:** 1904 failed, 729 passed, 67 skipped, 6 errors (~50s)

Tests run with default backend (v4 Robin). Below are the main **causes** of failures, grouped by root issue.

---

## 1. **Robin expects column names (str), Sparkless passes Column** (~220 failures)

- **`argument 'col': 'str' object cannot be converted to 'Column'`** (179)  
  Robin APIs (e.g. `groupBy`, `agg`, `orderBy`) expect **column names (strings)**. Sparkless often passes **Column expressions**. The compat layer must resolve `Column` → name where Robin only accepts str, or convert the call to an expression-based API where Robin supports it.

- **`argument 'on': join 'on' must be str or list/tuple of str`** (103)  
  Robin’s `join(on=...)` expects string column names. Sparkless sometimes passes Column expressions or other types. Compat must pass only str or list/tuple of str for `on`.

- **`avg() column names must be str: 'Column' object cannot be converted to 'PyString'`** (41)  
  Same idea: grouped/agg APIs expect column names as strings, not Column objects.

- **`'Column' object cannot be converted to 'PyString'`** (15), **`argument 'cols': 'Column' object cannot be converted to 'PyString'`** (12)  
  Other places where Robin expects string(s) and receives Column(s).

**Root cause:** Mismatch between “expression-based” usage (PySpark/Sparkless) and “name-based” APIs in Robin. Fix: in compat, detect Robin calls that take column names and pass `expr.name` or resolved names instead of Column objects where appropriate.

---

## 2. **Expression conversion: Sparkless → Robin Column** (~250 failures)

- **`argument 'expr': 'ColumnOperation' object cannot be converted to 'Column'`** (106)  
  Sparkless builds `ColumnOperation` (e.g. cast, alias, binary ops, UDFs). Not all op shapes are handled in `_to_robin_column`, so some expressions reach Robin unchanged and are rejected.

- **`argument 'expr': 'WindowFunction' object cannot be converted to 'Column'`** (58)  
  Window expressions (e.g. `rank().over(...)`) are not converted to Robin’s window API; tests that don’t skip for Robin hit this.

- **`argument 'expr': 'CaseWhen' object cannot be converted to 'Column'`** (57)  
  CaseWhen (when/otherwise) is not translated to Robin’s equivalent.

- **`argument 'exprs': 'ColumnOperation' object cannot be converted to 'Column'`** (24)  
  Same as above for multi-expression APIs (e.g. agg with multiple exprs).

- **`argument 'value': 'int'/'str' object cannot be converted to 'Column'`** (5+7)  
  Literals or wrong types passed where Robin expects a Column; need to run through `_to_robin_column` (e.g. `lit()` for scalars).

**Root cause:** Incomplete coverage in `_to_robin_column` and in call sites that pass expressions to Robin (UDFs, window, case/when, some agg/select paths).

---

## 3. **Robin Column/API differences** (~120 failures)

- **`'builtins.Column' object has no attribute 'name'`** (51)  
  Sparkless code assumes Column has `.name`. Robin’s Column may not expose it (or differently). Either stop relying on `.name` on Robin Columns or add a compat shim.

- **`'builtins.Column' object has no attribute 'eqNullSafe'`** (21)  
  Robin’s Column doesn’t implement `eqNullSafe`. Needs compat or a different expression in Robin.

- **`'builtins.Column' object has no attribute 'getItem'`** (5), **`no attribute 'getField'`** (6), **`no attribute 'withField'`** (38)  
  Struct/array/map access or withField not present or named differently in Robin. Requires mapping to Robin’s API or skipping.

- **`'builtins.Column' object is not subscriptable`** (11)  
  PySpark-style `col["key"]` or `col[0]` not supported on Robin’s Column; needs conversion to Robin’s accessors.

**Root cause:** PySpark/Sparkless assume a Column API (name, eqNullSafe, getItem, getField, withField, subscript). Robin’s Column type and API differ; compat layer or tests must account for that.

---

## 4. **Session / Spark API not implemented or different** (~90 failures)

- **`'function' object has no attribute 'mode'`** (47)  
  SparkSession or config access (e.g. `.mode`) is a function in Robin instead of an object with attributes. Test or compat expects PySpark-style session/config API.

- **`'builtin_function_or_method' object has no attribute 'option'`** (21)  
  Builder-style `.option()` not available on what’s exposed for session building.

- **`'builtin_function_or_method' object has no attribute 'listDatabases'`** (4), **`createDatabase`** (6)  
  Catalog API (listDatabases, createDatabase, etc.) missing or different on Robin’s session/catalog.

- **`'function' object has no attribute 'flatMap'`** (12), **`'function' object has no attribute 'format'`** (10)  
  DataFrame or SQL context methods (flatMap, format) exposed as plain functions or with different names.

- **`'builtins.GroupedData' object has no attribute 'pivot'`** (23)  
  Pivot not implemented or named differently in Robin.

- **`'builtins.DataFrame' object has no attribute 'alias'`** (7)  
  DataFrame alias for subqueries not supported or different.

**Root cause:** Session, catalog, config, and some DataFrame/GroupedData methods differ from PySpark; tests or compat assume PySpark’s object model.

---

## 5. **Window / WindowSpec** (~55 failures)

- **`descriptor 'orderBy' for 'builtins.Window' objects doesn't apply to a 'str' object`** (20)  
  Robin’s Window API expects different types for partitionBy/orderBy (e.g. Column or special sort order, not plain str). Sparkless passes strings; needs conversion or different Window usage.

- **`No constructor defined for Window`** (15)  
  Tests or compat use `Window()` or WindowSpec in a way Robin doesn’t support (e.g. no default constructor).

- **`select() items must be str (column name) or Column (expression)`** (22)  
  Some select() calls pass WindowFunction or other non-Column/non-str (e.g. after window conversion fails), so Robin receives an invalid type.

**Root cause:** Window and WindowSpec construction/usage differ from PySpark; expression conversion for window functions is not implemented, and Window API shape differs.

---

## 6. **Union / DataFrame type** (57 failures)

- **`argument 'other': '_PySparkCompatDataFrame' object cannot be converted to 'DataFrame'`** (57)  
  Robin’s union (or similar) expects its own `DataFrame` type. Compat wraps Robin DataFrame in `_PySparkCompatDataFrame`; that wrapper is not accepted where Robin expects a native DataFrame. Compat must unwrap or use Robin’s API with native frames.

**Root cause:** Union (and possibly other binary ops) are called with the compat wrapper instead of the underlying Robin DataFrame.

---

## 7. **Intentional / documented gaps** (~43 failures)

- **`na.replace(...) not available on Robin backend`** (30)  
  Documented: na.replace not implemented for Robin; use na.fill().

- **`dropDuplicates(subset=...) not supported when Robin only provides distinct()`** (4)  
  Robin doesn’t support subset in dropDuplicates.

**Root cause:** By design; either skip these tests for Robin or extend Robin/compat later.

---

## 8. **SQL / DDL not supported** (16 failures + 6 errors)

- **`SQL: only SELECT and CREATE SCHEMA/DATABASE are supported, got Drop { object_type: Table, ... }`** (6 ERROR + 10 FAILED)  
  Robin’s SQL layer doesn’t support DROP TABLE (and likely other DDL). Table lifecycle tests fail.

**Root cause:** Robin engine limitation; tests that rely on DROP TABLE (or similar DDL) need to be skipped or run only on backends that support it.

---

## 9. **Struct / type conversion** (~50 failures)

- **`py_struct() takes 1 positional arguments but 2/3 were given`** (12+6)  
  Robin’s struct() has a different signature (e.g. one list of columns vs multiple columns). Sparkless passes multiple columns; compat must adapt.

- **`'IntegerType'/'StringType' object cannot be converted to 'PyString'`** (19+15)  
  Robin expects type names as strings; Sparkless passes DataType instances. Convert type to string (e.g. `type_name()`) before calling Robin.

- **`'str' object has no attribute 'fieldNames'`** (21)  
  Sparkless code calls `.fieldNames()` on something that is a string (e.g. type name) in this path; likely schema/type handling assuming a StructType.

- **`unknown type name: Decimal(10,0)`** (5)  
  Robin may not support Decimal or uses a different type name.

**Root cause:** Struct construction and type-name handling differ between Sparkless (PySpark types) and Robin (string type names, different struct API).

---

## 10. **Row / result shape and types** (~40 failures)

- **`Key 'map_col' not found in row`** (15)  
  After select/withColumn, result row doesn’t have the expected alias (e.g. `map_col`). Robin may use different column names in result rows (e.g. expression string vs alias).

- **`duplicate: column with name 'value' has more than one occurrence`** (8)  
  Robin rejects duplicate output column names in agg/select; Sparkless test produces duplicate “value” columns.

- **`lengths don't match: unable to add a column of length N to a DataFrame of height 1`** (several)  
  Broadcast/join or scalar expansion behavior differs; column length doesn’t match DataFrame height.

**Root cause:** Result schema (column names, aliasing) and aggregation/join result shape differ from PySpark expectations.

---

## 11. **Regex / string / comparison semantics** (~25 failures)

- **`look-around, including look-ahead and look-behind, is not supported`** (3)  
  Robin’s regex engine doesn’t support lookahead/lookbehind. Parity tests for regexp_extract with these patterns fail.

- **`cannot compare string with numeric type`**, **`cannot compare 'date/datetime/time' to a string value`** (10+10)  
  Robin is stricter about types in comparisons; Sparkless/PySpark may coerce. Tests assume coercion or different semantics.

- **`replace: to_replace and value must be None, int, float, bool, or str`** (10)  
  Robin’s replace has stricter argument types.

**Root cause:** Robin has stricter or different semantics for regex, comparisons, and replace; tests written for PySpark behavior.

---

## 12. **Miscellaneous** (remaining failures)

- **AssertionError: DataFrames are not equivalent** (46), **assert False** (20), **assert 0 == 1** (18), etc.  
  Result data or schema doesn’t match expected (types, values, nulls, order). Often downstream of one of the issues above (e.g. wrong type from Robin, or wrong column name in row).

- **AssertionError: assert '25' == 25** (and similar)  
  Type coercion: Robin returns string where test expects int (or vice versa); common with createDataFrame or after operations.

- **AssertionError: assert [] == {}** (8)  
  Empty map: Robin returns list `[]` where PySpark returns dict `{}` (or vice versa); known Robin–PySpark difference.

- **Case sensitivity / catalog**  
  Tests for case-insensitive matching, unionByName with different case, etc., fail because Robin treats names differently or doesn’t support the same options.

**Root cause:** Mix of type/schema/result shape differences and stricter or different semantics; some are follow-on effects of the categories above.

---

## Summary table (by root cause)

| Category                              | Approx. count | Main fix direction |
|--------------------------------------|----------------|---------------------|
| Robin expects str, we pass Column    | ~220           | Resolve Column→name or use expr-based API where Robin allows |
| Expression conversion (ColumnOp etc.)| ~250           | Extend `_to_robin_column` and expression-taking call sites |
| Robin Column API (name, eqNullSafe…)| ~120           | Compat shims or avoid relying on PySpark-only Column API |
| Session/catalog/API not implemented | ~90            | Skip for Robin or implement compat for session/catalog/DF |
| Window / WindowSpec                 | ~55            | Align Window usage with Robin; convert or skip window tests |
| Union compat wrapper                | 57             | Unwrap to Robin DataFrame when calling Robin union |
| Intentional (na.replace, etc.)      | ~43            | Skip or document |
| SQL/DDL (e.g. DROP TABLE)          | 16             | Skip or backend-specific test |
| Struct / type names                 | ~50            | Adapt struct() and DataType→string in compat |
| Row/schema/result shape             | ~40            | Align aliasing and result handling with Robin |
| Regex/string/comparison semantics   | ~25            | Skip or relax tests for Robin semantics |
| Misc assertions / types             | rest           | Fix underlying conversion/semantics above |

---

*Generated from `test_results.txt` (pytest -n 12 run).*
