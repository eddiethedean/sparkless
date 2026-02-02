# Mock Spark Features


This guide explains sparkless-specific features that are not available in PySpark, and when to use them versus PySpark-compatible APIs.

## Overview

Sparkless provides two categories of APIs:

1. **PySpark-Compatible APIs** - Use these for code that needs to work with both sparkless and PySpark
2. **sparkless Convenience APIs** - Use these for sparkless-specific test utilities and convenience features

## PySpark-Compatible APIs (Recommended)

These APIs work identically in both sparkless and PySpark. Use them when:

- Writing code that needs to work with both engines
- Following PySpark best practices
- Writing production-like code
- Sharing code with teams using PySpark

### SQL Commands

```python
from sparkless.sql import SparkSession

spark = SparkSession("MyApp")

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")

# Create table
spark.sql("CREATE TABLE test_db.users (name STRING, age INT)")

# Insert data
spark.sql("INSERT INTO test_db.users VALUES ('Alice', 25), ('Bob', 30)")

# Query
result = spark.sql("SELECT * FROM test_db.users WHERE age > 25")
```

### Functions Module

```python
# PySpark-compatible import
from sparkless.sql import functions as F

df.select(F.col("name"), F.upper(F.col("name")))
```

### Catalog API

```python
# List databases
databases = spark.catalog.listDatabases()

# List tables
tables = spark.catalog.listTables("test_db")

# Check if table exists
exists = spark.catalog.tableExists("users", "test_db")

# Get table information
table = spark.catalog.getTable("users", "test_db")
```

## sparkless Convenience APIs

These APIs are specific to sparkless and provide convenient programmatic access. **They will not work with PySpark.**

### Storage API

The `.storage` API provides convenient programmatic access to databases and tables:

```python
from sparkless.sql import SparkSession
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession("MyApp")

# Create schema (database)
spark._storage.create_schema("test_db")

# Create table with schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
spark._storage.create_table("test_db", "users", schema)

# Insert data
spark._storage.insert_data("test_db", "users", [
    {"name": "Alice", "age": 25},
    {"name": "Bob", "age": 30}
])

# Get table as DataFrame
df = spark._storage.get_table("test_db", "users")
```

**When to use:**
- Writing sparkless-specific test utilities
- Setting up test fixtures
- Need convenient programmatic access
- Code will only run with sparkless

**When NOT to use:**
- Code that needs to work with PySpark
- Production-like code
- Sharing code with PySpark users

### Enhanced Error Messages

Sparkless provides enhanced error messages with migration guidance:

```python
from sparkless.core.exceptions.analysis import AnalysisException

try:
    spark.sql("SELECT * FROM non_existent_table")
except AnalysisException as e:
    print(e)  # Includes helpful migration hints
```

The error messages automatically detect common patterns and provide hints:
- Table not found → Guidance on creating tables
- Database not found → Guidance on creating databases
- Column not found → Suggestion to check column names

### Enhanced Explain Method

Sparkless's `explain()` method provides detailed execution plans:

```python
df.explain()  # Basic plan
df.explain(extended=True)  # Extended plan with schema details
```

Shows:
- Source operations
- Pending transformations (lazy evaluation)
- Schema information (when extended=True)

### DataFrameWriter.delta() Convenience Method

Sparkless provides a convenience method for Delta Lake format:

```python
# Convenience method (sparkless)
df.write.delta("/path/to/delta_table")

# Equivalent PySpark-compatible way
df.write.format("delta").save("/path/to/delta_table")
```

Both work, but the convenience method is shorter.

## Migration Guide

### From sparkless Convenience APIs to PySpark-Compatible

If you have code using sparkless convenience APIs and want to make it PySpark-compatible:

**Before (sparkless only):**
```python
spark._storage.create_schema("test_db")
schema = StructType([StructField("name", StringType())])
spark._storage.create_table("test_db", "users", schema)
spark._storage.insert_data("test_db", "users", [{"name": "Alice"}])
```

**After (PySpark-compatible):**
```python
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("CREATE TABLE test_db.users (name STRING)")
spark.sql("INSERT INTO test_db.users VALUES ('Alice')")
```

### From PySpark-Compatible to sparkless Convenience APIs

If you want to use convenience APIs in sparkless-specific code:

**Before (SQL):**
```python
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("CREATE TABLE test_db.users (name STRING, age INT)")
spark.sql("INSERT INTO test_db.users VALUES ('Alice', 25)")
```

**After (convenience API):**
```python
spark._storage.create_schema("test_db")
schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType())
])
spark._storage.create_table("test_db", "users", schema)
spark._storage.insert_data("test_db", "users", [{"name": "Alice", "age": 25}])
```

## Best Practices

### For Production-Like Code

✅ **Use PySpark-Compatible APIs:**
- SQL commands for database/table operations
- Standard functions module import
- Catalog API for metadata operations

```python
# Good: Works with both sparkless and PySpark
spark.sql("CREATE DATABASE IF NOT EXISTS analytics")
spark.sql("CREATE TABLE analytics.events (timestamp TIMESTAMP, event_type STRING)")
```

### For Test Utilities

✅ **Use sparkless Convenience APIs:**
- `.storage` API for test setup
- Enhanced error messages for debugging
- Convenience methods for faster test writing

```python
# Good: Convenient for tests, but sparkless-specific
@pytest.fixture
def setup_test_data(spark):
    spark._storage.create_schema("test")
    schema = StructType([StructField("id", IntegerType())])
    spark._storage.create_table("test", "data", schema)
    return spark
```

### For Learning PySpark

✅ **Use PySpark-Compatible APIs:**
- Learn patterns that work in real PySpark
- Understand SQL-based operations
- Practice with standard PySpark APIs

## Summary

| Feature | PySpark-Compatible | sparkless Convenience |
|---------|-------------------|----------------------|
| **Storage Management** | SQL commands | `.storage` API |
| **Functions** | `from sparkless.sql import functions as F` | Same (no convenience API) |
| **Error Messages** | Standard exceptions | Enhanced with hints |
| **Explain** | Basic plan | Enhanced with details |
| **Delta Writer** | `df.write.format("delta").save()` | `df.write.delta()` |

**Recommendation:** Use PySpark-compatible APIs for code that needs to work with both engines. Use sparkless convenience APIs for sparkless-specific test utilities.

## See Also

- [Storage API Guide](storage_api_guide.md) - Detailed guide on storage APIs
- [Getting Started](getting_started.md) - Quick start guide
- [API Reference](api_reference.md) - Complete API documentation

