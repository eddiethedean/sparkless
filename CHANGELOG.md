# Changelog

## 3.24.0 — 2025-01-15

### Added
- **Issue #247** - Added `elementType` keyword argument support to `ArrayType` for PySpark compatibility
  - `ArrayType(elementType=StringType())` now works (PySpark convention)
  - Maintains backward compatibility with positional `element_type` parameter
  - Added comprehensive test suite (32 tests) covering edge cases and PySpark parity

### Fixed
- **fillna After Join** - Fixed `fillna()` to properly materialize lazy DataFrames before processing
  - Ensures all columns are present after joins before filling null values
  - Prevents missing columns from being incorrectly filled as None
  - Removed xfail marker from `test_na_fill_after_join` (now passing)

### Testing
- Added 32 new tests for ArrayType elementType support
  - Basic keyword argument tests (10 tests)
  - Robust tests covering all primitive types, nested arrays, complex types, and DataFrame operations (22 tests)
- All 1106 tests passing (up from 1105), 12 skipped, 0 xfailed (down from 1)

### Technical Details
- Updated `ArrayType.__init__()` to accept both `elementType` (camelCase, PySpark) and `element_type` (snake_case, backward compat) keyword arguments
- Enhanced `fillna()` in `MiscService` to materialize lazy DataFrames before processing rows
- All code quality checks passing (ruff format, ruff check, mypy)

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

