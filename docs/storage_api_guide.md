# Storage API Guide


**⚠️ Important:** The `spark._storage` API is a **private sparkless-specific convenience feature** that does not exist in PySpark. For code that needs to work with both sparkless and PySpark, use SQL commands or DataFrame operations instead. The `spark._storage` API is now private and should not be used in production code.

This guide explains the two ways to manage databases and tables in sparkless, and when to use each approach.

## Overview

Sparkless provides two APIs for managing storage:

1. **PySpark-Compatible APIs** (SQL commands) - ✅ Use for compatibility with PySpark
2. **sparkless Convenience APIs** (`._storage` API) - ⚠️ Private sparkless-specific, not available in PySpark

Both work identically in sparkless, but **only SQL commands are portable** between sparkless and PySpark.

## PySpark-Compatible APIs (Recommended for Compatibility)

Use SQL commands when you need code that works with both sparkless and PySpark.

### Creating Databases

```python
from sparkless.sql import SparkSession

spark = SparkSession("MyApp")

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")

# Use database
spark.sql("USE test_db")

# Drop database
spark.sql("DROP DATABASE IF EXISTS test_db CASCADE")
```

### Creating Tables

```python
# Create table with schema
spark.sql("""
    CREATE TABLE IF NOT EXISTS users (
        id INT,
        name STRING,
        age INT
    )
""")

# Insert data
spark.sql("""
    INSERT INTO users VALUES
    (1, 'Alice', 25),
    (2, 'Bob', 30),
    (3, 'Charlie', 35)
""")

# Query table
result = spark.sql("SELECT * FROM users WHERE age > 25")
result.show()
```

### Using Catalog API

```python
# List databases
databases = spark.catalog.listDatabases()
for db in databases:
    print(db.name)

# List tables
tables = spark.catalog.listTables("test_db")
for table in tables:
    print(table.name)

# Check if table exists
exists = spark.catalog.tableExists("users", "test_db")
```

### Benefits

- ✅ Works identically in PySpark and sparkless
- ✅ Standard SQL syntax
- ✅ No code changes needed when switching engines
- ✅ Familiar to PySpark developers

## sparkless Convenience APIs

Use the `.storage` API when writing sparkless-specific test utilities or when you need more convenient programmatic access.

### Creating Databases (Schemas)

```python
from sparkless.sql import SparkSession

spark = SparkSession("MyApp")

# Create schema (database)
spark._storage.create_schema("test_db")

# Check if schema exists
exists = spark._storage.schema_exists("test_db")

# List all schemas
schemas = spark._storage.list_schemas()

# Drop schema
spark._storage.drop_schema("test_db")
```

### Creating Tables

```python
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

# Define schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create table
spark._storage.create_table("test_db", "users", schema)

# Insert data
data = [
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30},
    {"id": 3, "name": "Charlie", "age": 35}
]
spark._storage.insert_data("test_db", "users", data)

# Get table as DataFrame
df = spark._storage.get_table("test_db", "users")
```

### Benefits

- ✅ More Pythonic API
- ✅ Direct programmatic access
- ✅ Easier for test setup
- ⚠️ **Not available in PySpark** - code won't work with real PySpark

## When to Use Which API

### Use SQL Commands (PySpark-Compatible) When:

- Writing code that needs to work with both sparkless and PySpark
- Following PySpark best practices
- Writing production-like code
- Sharing code with teams using PySpark
- Learning PySpark patterns

**Example:**
```python
# This code works in both sparkless and PySpark
spark.sql("CREATE DATABASE IF NOT EXISTS analytics")
spark.sql("CREATE TABLE analytics.events (timestamp TIMESTAMP, event_type STRING)")
```

### Use `.storage` API (sparkless Convenience) When:

- Writing sparkless-specific test utilities
- Setting up test fixtures
- Need convenient programmatic access
- Code will only run with sparkless

**Example:**
```python
# This is convenient for tests, but won't work with PySpark
@pytest.fixture
def setup_test_data(spark):
    spark._storage.create_schema("test")
    schema = StructType([StructField("id", IntegerType())])
    spark._storage.create_table("test", "data", schema)
    return spark
```

## Migration Guide

### Migrating from `.storage` API to SQL Commands

If you have code using `.storage` API and want to make it PySpark-compatible:

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

### Migrating from SQL Commands to `.storage` API

If you want to use the convenience API in sparkless-specific code:

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

1. **For Production-Like Code**: Always use SQL commands for maximum compatibility
2. **For Test Utilities**: Use `.storage` API for convenience in sparkless-specific test helpers
3. **For Learning**: Use SQL commands to learn PySpark patterns
4. **For Sharing**: Use SQL commands so code works for everyone

## Summary

| Feature | SQL Commands | `.storage` API |
|---------|--------------|----------------|
| PySpark Compatible | ✅ Yes | ❌ No |
| Standard SQL | ✅ Yes | ❌ No |
| Programmatic Access | ⚠️ Via SQL strings | ✅ Direct API |
| Test Convenience | ⚠️ More verbose | ✅ More concise |
| Learning PySpark | ✅ Recommended | ⚠️ sparkless specific |

**Recommendation**: Use SQL commands for code that needs to work with both engines. Use `.storage` API for sparkless-specific test utilities.

