# Sparkless

<div align="center">

**🚀 Test PySpark code at lightning speed—no JVM required**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark 3.2-3.5](https://img.shields.io/badge/pyspark-3.2--3.5-orange.svg)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://badge.fury.io/py/sparkless.svg)](https://badge.fury.io/py/sparkless)
[![Tests](https://img.shields.io/badge/tests-650+%20passing%20%7C%200%20failing-brightgreen.svg)](https://github.com/eddiethedean/sparkless)
[![Type Checked](https://img.shields.io/badge/mypy-260%20files%20clean-blue.svg)](https://github.com/python/mypy)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

*⚡ 10x faster tests • 🎯 Drop-in PySpark replacement • 📦 Zero JVM overhead • 🧵 Thread-safe Polars backend*

</div>

---

## Why Sparkless?

**Tired of waiting 30+ seconds for Spark to initialize in every test?**

Sparkless is a lightweight PySpark replacement that runs your tests **10x faster** by eliminating JVM overhead. Your existing PySpark code works unchanged—just swap the import.

```python
# Before
from pyspark.sql import SparkSession

# After  
from sparkless.sql import SparkSession
```

### Key Benefits

| Feature | Description |
|---------|-------------|
| ⚡ **10x Faster** | No JVM startup (30s → 0.1s) |
| 🎯 **Drop-in Replacement** | Use existing PySpark code unchanged |
| 📦 **Zero Java** | Pure Python with Polars backend (thread-safe, no SQL required) |
| 🧪 **100% Compatible** | Full PySpark 3.2-3.5 API support |
| 🔄 **Lazy Evaluation** | Mirrors PySpark's execution model |
| 🏭 **Production Ready** | 650+ passing tests, 100% mypy typed |
| 🧵 **Thread-Safe** | Polars backend designed for parallel execution |
| 🔧 **Modular Design** | DDL parsing via standalone spark-ddl-parser package |
| 🎯 **Type Safe** | Full type checking with `ty`, comprehensive type annotations |

### Perfect For

- **Unit Testing** - Fast, isolated test execution with automatic cleanup
- **CI/CD Pipelines** - Reliable tests without infrastructure or resource leaks
- **Local Development** - Prototype without Spark cluster
- **Documentation** - Runnable examples without setup
- **Learning** - Understand PySpark without complexity
- **Integration Tests** - Configurable memory limits for large dataset testing

---

## Quick Start

### Installation

```bash
pip install sparkless
```

### Basic Usage

```python
from sparkless.sql import SparkSession, functions as F

# Create session
spark = SparkSession("MyApp")

# Your PySpark code works as-is
data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
df = spark.createDataFrame(data)

# All operations work
result = df.filter(F.col("age") > 25).select("name").collect()
print(result)
# Output: [Row(name='Bob')]

# Show the DataFrame
df.show()
# Output:
# DataFrame[2 rows, 2 columns]
# age name 
# 25    Alice  
# 30    Bob
```

### Storage API (Sparkless-Specific)

Sparkless provides a convenient `.storage` API for managing databases and tables. **Note:** This is a **sparkless-specific convenience API** that does not exist in PySpark. For PySpark compatibility, use SQL commands or DataFrame operations instead:

```python
# Sparkless: Using .storage API (convenient but NOT PySpark-compatible)
spark._storage.create_schema("test_db")
spark._storage.create_table("test_db", "users", schema)
spark._storage.insert_data("test_db", "users", data)
df = spark._storage.query_table("test_db", "users")

# Both Sparkless and PySpark: Using SQL commands (recommended for compatibility)
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("CREATE TABLE test_db.users (name STRING, age INT)")
df.write.saveAsTable("test_db.users")  # Write DataFrame to table
df = spark.table("test_db.users")  # Read table as DataFrame

# PySpark equivalent for insert_data:
# df = spark.createDataFrame(data, schema)
# df.write.mode("append").saveAsTable("test_db.users")
```

**Migration Guide:**
- `spark._storage.create_schema()` → `spark.sql("CREATE DATABASE IF NOT EXISTS ...")`
- `spark._storage.create_table()` → `spark.sql("CREATE TABLE ...")` or `df.write.saveAsTable()`
- `spark._storage.insert_data()` → `df.write.mode("append").saveAsTable()`
- `spark._storage.query_table()` → `spark.table()` or `spark.sql("SELECT * FROM ...")`

See the [Storage API Guide](docs/storage_api_guide.md) and [Migration Guide](docs/migration_from_pyspark.md) for more details.

### Testing Example

```python
import pytest
from sparkless.sql import SparkSession, functions as F

def test_data_pipeline():
    """Test PySpark logic without Spark cluster."""
    spark = SparkSession("TestApp")
    
    # Test data
    data = [{"score": 95}, {"score": 87}, {"score": 92}]
    df = spark.createDataFrame(data)
    
    # Business logic
    high_scores = df.filter(F.col("score") > 90)
    
    # Assertions
    assert high_scores.count() == 2
    assert high_scores.agg(F.avg("score")).collect()[0][0] == 93.5
    
    # Always clean up
    spark.stop()
```

---

## Core Features

### 🚀 Complete PySpark API Compatibility

Sparkless implements **120+ functions** and **70+ DataFrame methods** across PySpark 3.0-3.5:

| Category | Functions | Examples |
|----------|-----------|----------|
| **String** (40+) | Text manipulation, regex, formatting | `upper`, `concat`, `regexp_extract`, `soundex` |
| **Math** (35+) | Arithmetic, trigonometry, rounding | `abs`, `sqrt`, `sin`, `cos`, `ln` |
| **DateTime** (30+) | Date/time operations, timezones | `date_add`, `hour`, `weekday`, `convert_timezone` |
| **Array** (25+) | Array manipulation, lambdas | `array_distinct`, `transform`, `filter`, `aggregate` |
| **Aggregate** (20+) | Statistical functions | `sum`, `avg`, `median`, `percentile`, `max_by` |
| **Map** (10+) | Dictionary operations | `map_keys`, `map_filter`, `transform_values` |
| **Conditional** (8+) | Logic and null handling | `when`, `coalesce`, `ifnull`, `nullif` |
| **Window** (8+) | Ranking and analytics | `row_number`, `rank`, `lag`, `lead` |
| **XML** (9+) | XML parsing and generation | `from_xml`, `to_xml`, `xpath_*` |
| **Bitwise** (6+) | Bit manipulation | `bit_count`, `bit_and`, `bit_xor` |

📖 **See complete function list**: [`PYSPARK_FUNCTION_MATRIX.md`](PYSPARK_FUNCTION_MATRIX.md)

### DataFrame Operations

- **Transformations**: `select`, `filter`, `withColumn`, `drop`, `distinct`, `orderBy`, `replace`
- **Aggregations**: `groupBy`, `agg`, `count`, `sum`, `avg`, `min`, `max`, `median`, `mode`
- **Joins**: `inner`, `left`, `right`, `outer`, `cross`
- **Advanced**: `union`, `pivot`, `unpivot`, `explode`, `transform`

### Window Functions

```python
from sparkless.sql import Window, functions as F

# Ranking and analytics
df = spark.createDataFrame([
    {"name": "Alice", "dept": "IT", "salary": 50000},
    {"name": "Bob", "dept": "HR", "salary": 60000},
    {"name": "Charlie", "dept": "IT", "salary": 70000},
])

result = df.withColumn("rank", F.row_number().over(
    Window.partitionBy("dept").orderBy("salary")
))

# Show results
for row in result.collect():
    print(row)
# Output:
# Row(dept='HR', name='Bob', salary=60000, rank=1)
# Row(dept='IT', name='Alice', salary=50000, rank=1)
# Row(dept='IT', name='Charlie', salary=70000, rank=2)
```

### SQL Support

```python
df = spark.createDataFrame([
    {"name": "Alice", "salary": 50000},
    {"name": "Bob", "salary": 60000},
    {"name": "Charlie", "salary": 70000},
])

# Create temporary view for SQL queries
df.createOrReplaceTempView("employees")

# Execute SQL queries
result = spark.sql("SELECT name, salary FROM employees WHERE salary > 50000")
result.show()
# SQL support enables querying DataFrames using SQL syntax
```

### Delta Lake Format

Full Delta Lake table format support:

```python
# Write as Delta table
df.write.format("delta").mode("overwrite").saveAsTable("catalog.users")

# Time travel - query historical versions
v0_data = spark.read.format("delta").option("versionAsOf", 0).table("catalog.users")

# Schema evolution
new_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("catalog.users")

# MERGE operations for upserts
spark.sql("""
    MERGE INTO catalog.users AS target
    USING updates AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

### Lazy Evaluation

Sparkless mirrors PySpark's lazy execution model:

```python
# Transformations are queued (not executed)
result = df.filter(F.col("age") > 25).select("name")  

# Actions trigger execution
rows = result.collect()  # ← Execution happens here
count = result.count()    # ← Or here
```

### CTE Query Optimization

DataFrame operation chains are automatically optimized using Common Table Expressions:

```python
# Enable lazy evaluation for CTE optimization
data = [
    {"name": "Alice", "age": 25, "salary": 50000},
    {"name": "Bob", "age": 30, "salary": 60000},
    {"name": "Charlie", "age": 35, "salary": 70000},
    {"name": "David", "age": 28, "salary": 55000},
]
df = spark.createDataFrame(data)

# This entire chain executes as ONE optimized query:
result = (
    df.filter(F.col("age") > 25)           # CTE 0: WHERE clause
      .select("name", "age", "salary")     # CTE 1: Column selection
      .withColumn("bonus", F.col("salary") * 0.1)  # CTE 2: New column
      .orderBy(F.desc("salary"))           # CTE 3: ORDER BY
      .limit(2)                            # CTE 4: LIMIT
).collect()  # Single query execution here

# Result:
# [Row(name='Charlie', age=35, salary=70000, bonus=7000.0),
#  Row(name='Bob', age=30, salary=60000, bonus=6000.0)]

# Performance: 5-10x faster than creating 5 intermediate tables
```

---

## Backend Architecture

### Polars Backend (Default)

Sparkless uses **Polars** as the default backend, providing:

- 🧵 **Thread Safety** - Designed for parallel execution
- ⚡ **High Performance** - Optimized DataFrame operations
- 📊 **Parquet Storage** - Tables persist as Parquet files
- 🔄 **Lazy Evaluation** - Automatic query optimization

```python
# Default backend (Polars) - thread-safe, high-performance
spark = SparkSession("MyApp")

# Explicit backend selection
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "polars") \
    .getOrCreate()
```

### Alternative Backends

```python
# Memory backend for lightweight testing
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "memory") \
    .getOrCreate()

# File backend for persistent storage
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "file") \
    .config("spark.sparkless.backend.basePath", "/tmp/sparkless") \
    .getOrCreate()
```

---

## Advanced Features

### Table Persistence

Tables created with `saveAsTable()` can persist across multiple sessions:

```python
# First session - create table
spark1 = SparkSession("App1", db_path="test.db")
df = spark1.createDataFrame([{"id": 1, "name": "Alice"}])
df.write.mode("overwrite").saveAsTable("schema.my_table")
spark1.stop()

# Second session - table persists
spark2 = SparkSession("App2", db_path="test.db")
assert spark2.catalog.tableExists("schema", "my_table")  # ✅ True
result = spark2.table("schema.my_table").collect()  # ✅ Works!
spark2.stop()
```

**Key Features:**
- **Cross-Session Persistence**: Tables persist when using `db_path` parameter
- **Schema Discovery**: Automatically discovers existing schemas and tables
- **Catalog Synchronization**: Reliable `catalog.tableExists()` checks
- **Data Integrity**: Full support for `append` and `overwrite` modes

### Configurable Memory & Isolation

Control memory usage and test isolation:

```python
# Default: 1GB memory limit, no disk spillover (best for tests)
spark = SparkSession("MyApp")

# Custom memory limit
spark = SparkSession("MyApp", max_memory="4GB")

# Allow disk spillover for large datasets
spark = SparkSession(
    "MyApp",
    max_memory="8GB",
    allow_disk_spillover=True  # Uses unique temp directory per session
)
```

---

## Performance Comparison

Real-world test suite improvements:

| Operation | PySpark | Sparkless | Speedup |
|-----------|---------|------------|---------|
| Session Creation | 30-45s | 0.1s | **300x** |
| Simple Query | 2-5s | 0.01s | **200x** |
| Window Functions | 5-10s | 0.05s | **100x** |
| Full Test Suite | 5-10min | 30-60s | **10x** |

### Performance Tooling

- [Hot path profiling guide](docs/performance/profiling.md)
- [Pandas fallback vs native benchmarks](docs/performance/pandas_fallback.md)

---

---

## Recent Updates

### Version 3.20.0 - Logic Bug Fixes & Code Quality Improvements

- 🐛 **Exception Handling Fixes** – Fixed critical exception handling issues (issue #183): replaced bare `except:` clause with `except Exception:` and added comprehensive logging to exception handlers for better debuggability.
- 🧪 **Comprehensive Test Coverage** – Added 10 comprehensive test cases for string concatenation cache handling edge cases (issue #188), covering empty strings, None values, nested operations, and numeric vs string operations.
- 📚 **Improved Documentation** – Enhanced documentation for string concatenation cache heuristic, documenting limitations and expected behavior vs PySpark.
- 🔍 **Code Quality Review** – Systematic review of dictionary.get() usage patterns throughout codebase, confirming all patterns are safe with appropriate default values.
- ✅ **Type Safety** – Fixed mypy errors in CI: improved type narrowing for ColumnOperation.operation and removed redundant casts in writer.py.

### Version 3.7.0 - Full SQL DDL/DML Support

- 🗄️ **Complete SQL DDL/DML** – Full implementation of `CREATE TABLE`, `DROP TABLE`, `INSERT INTO`, `UPDATE`, and `DELETE FROM` statements in the SQL executor.
- 📝 **Enhanced SQL Parser** – Comprehensive support for DDL statements with column definitions, `IF NOT EXISTS`, and `IF EXISTS` clauses.
- 💾 **INSERT Operations** – Support for `INSERT INTO ... VALUES (...)` with multiple rows and `INSERT INTO ... SELECT ...` sub-queries.
- 🔄 **UPDATE & DELETE** – Full support for `UPDATE ... SET ... WHERE ...` and `DELETE FROM ... WHERE ...` with Python-based expression evaluation.
- 🐛 **Bug Fixes** – Fixed recursion errors in schema projection and resolved import shadowing issues in SQL executor.
- ✨ **Code Quality** – Improved linting, formatting, and type safety across the codebase.

### Version 3.6.0 - Profiling & Adaptive Execution

- ⚡ **Feature-Flagged Profiling** – Introduced `sparkless.utils.profiling` with opt-in instrumentation for Polars hot paths and expression evaluation, plus a new guide at `docs/performance/profiling.md`.
- 🔁 **Adaptive Execution Simulation** – Query plans can now inject synthetic `REPARTITION` steps based on skew metrics, configurable via `QueryOptimizer.configure_adaptive_execution` and covered by new regression tests.
- 🐼 **Pandas Backend Choice** – Added an optional native pandas mode (`MOCK_SPARK_PANDAS_MODE`) with benchmarking support (`scripts/benchmark_pandas_fallback.py`) and documentation in `docs/performance/pandas_fallback.md`.

### Version 3.5.0 - Session-Aware Catalog & Safer Fallbacks

- 🧭 **Session-Literal Helpers** – `F.current_catalog`, `F.current_database`, `F.current_schema`, and `F.current_user` return PySpark-compatible literals and understand the active session (with new regression coverage).
- 🗃️ **Reliable Catalog Context** – The Polars backend and unified storage manager now track the selected schema so `setCurrentDatabase` works end-to-end, and `SparkContext.sparkUser()` mirrors PySpark behaviour.
- 🧮 **Pure-Python Stats** – Lightweight `percentile` and `covariance` helpers keep percentile/cov tests green even without NumPy, eliminating native-crash regressions.
- 🛠️ **Dynamic Dispatch** – `F.call_function("func_name", ...)` lets wrappers dynamically invoke registered Sparkless functions with PySpark-style error messages.

### Version 3.4.0 - Workflow & CI Refresh

- ♻️ **Unified Commands** – `Makefile`, `install.sh`, and docs now point to `bash tests/run_all_tests.sh`, `ruff`, and `mypy` as the standard dev workflow.
- 🛡️ **Automated Gates** – New GitHub Actions pipeline runs linting, type-checking, and the full test suite on every push and PR.
- 🗺️ **Forward Roadmap** – Published `plans/typing_delta_roadmap.md` to track mypy debt reduction and Delta feature milestones.
- 📝 **Documentation Sweep** – README and quick-start docs highlight the 3.4.0 tooling changes and contributor expectations.

### Version 3.3.0 - Type Hardening & Clean Type Check

- 🧮 **Zero mypy Debt** – `mypy sparkless` now runs clean after migrating the Polars executor,
  expression evaluator, Delta merge helpers, and reader/writer stack to Python 3.8+ compatible type syntax.
- 🧾 **Accurate DataFrame Interfaces** – `DataFrameReader.load()` and related helpers now return
  `IDataFrame` consistently while keeping type-only imports behind `TYPE_CHECKING`.
- 🧱 **Safer Delta & Projection Fallbacks** – Python-evaluated select columns always receive string
  aliases, and Delta merge alias handling no longer leaks `None` keys into evaluation contexts.
- 📚 **Docs & Metadata Updated** – README highlights the new type guarantees and all packaging
  metadata points to v3.3.0.

### Version 3.2.0 - Python 3.8 Baseline & Tooling Refresh

- 🐍 **Python 3.8+ Required** – Packaging metadata, tooling configs, and installation docs now align on Python 3.8 as the minimum supported runtime.
- 🧩 **Compatibility Layer** – Uses `typing_extensions` for Python 3.8 compatibility; datetime helpers use native typing with proper fallbacks.
- 🪄 **Type Hint Modernisation** – Uses `typing` module generics (`List`, `Dict`, `Tuple`) for Python 3.8 compatibility, with `from __future__ import annotations` for deferred evaluation.
- 🧼 **Ruff Formatting by Default** – Adopted `ruff format` across the repository, keeping style consistent with the Ruff rule set.

### Version 3.1.0 - Type-Safe Protocols & Tooling

- ✅ **260-File Type Coverage** – DataFrame mixins now implement structural typing protocols (`SupportsDataFrameOps`), giving a clean `mypy` run across the entire project.
- 🧹 **Zero Ruff Debt** – Repository-wide linting is enabled by default; `ruff check` passes with no warnings thanks to tighter casts, imports, and configuration.
- 🧭 **Backend Selection Docs** – Updated configuration builder and new `docs/backend_selection.md` make it trivial to toggle between Polars, Memory, File, or DuckDB backends.
- 🧪 **Delta Schema Evolution Fixes** – Polars mergeSchema appends now align frames to the on-disk schema, restoring compatibility with evolving Delta tables.
- 🧰 **Improved Test Harness** – `tests/run_all_tests.sh` respects virtual environments and ensures documentation examples are executed with the correct interpreter.

### Version 3.0.0+ - Code Quality & Cleanup

**Dependency Cleanup & Type Safety:**

- 🧹 **Removed Legacy Dependencies** - Removed unused `sqlglot` dependency (legacy DuckDB/SQL backend code)
- 🗑️ **Code Cleanup** - Removed unused legacy SQL translation modules (`sql_translator.py`, `spark_function_mapper.py`)
- ✅ **Type Safety** - Fixed 177 type errors using `ty` type checker, improved return type annotations
- 🔍 **Linting** - Fixed all 63 ruff linting errors, codebase fully formatted
- ✅ **All Tests Passing** - Full test suite validated (641+ tests, all passing)
- 📦 **Cleaner Dependencies** - Reduced dependency footprint, faster installation

### Version 3.0.0 - MAJOR UPDATE

**Polars Backend Migration:**

- 🚀 **Polars Backend** - Complete migration to Polars for thread-safe, high-performance operations
- 🧵 **Thread Safety** - Polars is thread-safe by design - no more connection locks or threading issues
- 📊 **Parquet Storage** - Tables now persist as Parquet files
- ⚡ **Performance** - Better performance for DataFrame operations
- ✅ **All tests passing** - Full test suite validated with Polars backend
- 📦 **Production-ready** - Stable release with improved architecture

See [Migration Guide](docs/migration_from_v2_to_v3.md) for details.

---

## Documentation

### Getting Started
- 📖 [Installation & Setup](docs/getting_started.md)
- 🎯 [Quick Start Guide](docs/getting_started.md#quick-start)
- 🔄 [Migration from PySpark](docs/guides/migration.md)

### Related Packages
- 🔧 [spark-ddl-parser](https://github.com/eddiethedean/spark-ddl-parser) - Zero-dependency PySpark DDL schema parser

### Core Concepts
- 📊 [API Reference](docs/api_reference.md)
- 🔄 [Lazy Evaluation](docs/guides/lazy_evaluation.md)
- 🗄️ [SQL Operations](docs/sql_operations_guide.md)
- 💾 [Storage & Persistence](docs/storage_serialization_guide.md)

### Advanced Topics
- ⚙️ [Configuration](docs/guides/configuration.md)
- 📈 [Benchmarking](docs/guides/benchmarking.md)
- 🔌 [Plugins & Hooks](docs/guides/plugins.md)
- 🐍 [Pytest Integration](docs/guides/pytest_integration.md)

---

## Development Setup

```bash
# Install for development
git clone https://github.com/eddiethedean/sparkless.git
cd sparkless
pip install -e ".[dev]"

# Run all tests (with proper isolation)
bash tests/run_all_tests.sh

# Format code
ruff format .
ruff check . --fix

# Type checking
mypy sparkless tests

# Linting
ruff check .
```

---

## Contributing

We welcome contributions! Areas of interest:

- ⚡ **Performance** - Further Polars optimizations
- 📚 **Documentation** - Examples, guides, tutorials
- 🐛 **Bug Fixes** - Edge cases and compatibility issues
- 🧪 **PySpark API Coverage** - Additional functions and methods
- 🧪 **Tests** - Additional test coverage and scenarios

---

## Known Limitations

While Sparkless provides comprehensive PySpark compatibility, some advanced features are planned for future releases:

- **Error Handling**: Enhanced error messages with recovery strategies
- **Performance**: Advanced query optimization, parallel execution, intelligent caching
- **Enterprise**: Schema evolution, data lineage, audit logging
- **Compatibility**: PySpark 3.6+, Iceberg support

**Want to contribute?** These are great opportunities for community contributions!

---

## License

MIT License - see [LICENSE](LICENSE) file for details.

---

## Links

- **GitHub**: [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless)
- **PyPI**: [pypi.org/project/sparkless](https://pypi.org/project/sparkless/)
- **Issues**: [github.com/eddiethedean/sparkless/issues](https://github.com/eddiethedean/sparkless/issues)
- **Documentation**: [Full documentation](docs/)

---

<div align="center">

**Built with ❤️ for the PySpark community**

*Star ⭐ this repo if Sparkless helps speed up your tests!*

</div>
