# Getting Started with Sparkless

> **Compatibility Snapshot:** This guide targets Sparkless `3.25.0`, which provides parity with PySpark 3.2–3.5 and ships with 1106+ passing regression tests.

## Installation

Install Sparkless using pip:

```bash
pip install sparkless
```

For development with testing tools:

```bash
pip install sparkless[dev]
```

## Quick Start

### Basic Example

```python
from sparkless.sql import SparkSession, functions as F

# Create session
spark = SparkSession("MyApp")

# Create DataFrame
data = [
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30},
]
df = spark.createDataFrame(data)

# Operations work just like PySpark
result = df.filter(F.col("age") > 25).select("name")
print(result.collect())  # [Row(name='Bob')]
```

### Drop-in PySpark Replacement

Sparkless is designed to be a drop-in replacement for PySpark:

```python
# Before (PySpark)
from pyspark.sql import SparkSession

# After (Sparkless)
from sparkless.sql import SparkSession
```

That's it! Your existing PySpark code works unchanged.

## Core Features

### DataFrame Operations

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession("Example")
data = [
    {"name": "Alice", "dept": "Engineering", "salary": 80000},
    {"name": "Bob", "dept": "Sales", "salary": 75000},
    {"name": "Charlie", "dept": "Engineering", "salary": 90000},
]
df = spark.createDataFrame(data)

# Filter
high_earners = df.filter(F.col("salary") > 75000)

# Null-safe equality (for comparing columns that may contain NULL)
# NULL <=> NULL returns True, NULL <=> non-NULL returns False
employees_with_managers = df.filter(F.col("id").eqNullSafe(F.col("manager_id")))

# Select
names = df.select("name", "dept")

# Aggregations
dept_avg = df.groupBy("dept").avg("salary")
```

### Window Functions

```python
from sparkless.sql import Window, functions as F

# Ranking within departments
window_spec = Window.partitionBy("dept").orderBy(F.desc("salary"))
ranked = df.select(
    "name",
    "dept",
    "salary",
    F.row_number().over(window_spec).alias("rank")
)
```

### SQL Queries

```python
# Create temporary view
df.createOrReplaceTempView("employees")
```

### Storage Management

Sparkless provides two ways to manage databases and tables:

**Option 1: SQL Commands (PySpark-Compatible - Recommended)**
```python
# Works in both sparkless and PySpark
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("CREATE TABLE test_db.users (name STRING, age INT)")
spark.sql("INSERT INTO test_db.users VALUES ('Alice', 25), ('Bob', 30)")
```

**Option 2: Storage API (Sparkless-Specific)**
```python
# Convenient API, but sparkless-specific
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

spark._storage.create_schema("test_db")
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
spark._storage.create_table("test_db", "users", schema)
spark._storage.insert_data("test_db", "users", [
    {"name": "Alice", "age": 25},
    {"name": "Bob", "age": 30}
])
```

**Note:** For maximum compatibility with PySpark, use SQL commands. The `.storage` API is a sparkless convenience feature that doesn't exist in PySpark.

# Run SQL queries
result = spark.sql("SELECT name, salary FROM employees WHERE salary > 80000")
result.show()
```

## Testing with Sparkless

### Unit Test Example

```python
import pytest
from sparkless.sql import SparkSession, functions as F

def test_data_transformation():
    """Test DataFrame transformation logic."""
    spark = SparkSession("TestApp")
    
    # Test data
    data = [{"value": 10}, {"value": 20}, {"value": 30}]
    df = spark.createDataFrame(data)
    
    # Apply transformation
    result = df.filter(F.col("value") > 15)
    
    # Assertions
    assert result.count() == 2
    rows = result.collect()
    assert rows[0]["value"] == 20
    assert rows[1]["value"] == 30

def test_aggregation():
    """Test aggregation logic."""
    spark = SparkSession("TestApp")
    
    data = [
        {"category": "A", "value": 100},
        {"category": "A", "value": 200},
        {"category": "B", "value": 150},
    ]
    df = spark.createDataFrame(data)
    
    # Group and aggregate
    result = df.groupBy("category").sum("value")
    
    # Verify results
    assert result.count() == 2
```

## Lazy Evaluation

Sparkless mirrors PySpark's lazy evaluation model:

```python
# Transformations are queued (not executed)
filtered = df.filter(F.col("age") > 25)
selected = filtered.select("name")
# No execution yet!

# Actions trigger execution
rows = selected.collect()  # ← Executes now
count = selected.count()   # ← Executes now
```

Control evaluation mode:

```python
# Lazy (default, recommended)
spark = SparkSession("App", enable_lazy_evaluation=True)

# Eager (for legacy tests)
spark = SparkSession("App", enable_lazy_evaluation=False)
```

## Performance

Sparkless provides significant speed improvements:

| Operation | PySpark | Sparkless | Speedup |
|-----------|---------|------------|---------|
| Session Creation | 30-45s | 0.1s | 300x |
| Simple Query | 2-5s | 0.01s | 200x |
| Full Test Suite | 5-10min | 30-60s | 10x |

## Next Steps

- **[API Reference](api_reference.md)** - Complete API documentation
- **[SQL Operations](sql_operations_guide.md)** - SQL query examples
- **[Testing Utilities](testing_utilities_guide.md)** - Test helpers and fixtures
- **[Examples](../examples/)** - More code examples

## Getting Help

- **GitHub**: [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless)
- **Issues**: [github.com/eddiethedean/sparkless/issues](https://github.com/eddiethedean/sparkless/issues)
- **Documentation**: [docs/](.)
