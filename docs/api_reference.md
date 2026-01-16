# Sparkless API Reference

## Overview

Sparkless provides 100% API compatibility with PySpark while using Polars as the default backend. This reference covers all supported functions, classes, and operations.

## Session Management

### SparkSession

```python
from sparkless.sql import SparkSession

# Create session
spark = SparkSession("my_app")

# With configuration
spark = SparkSession("my_app", config={
    "spark.sql.debug": "true",
    "spark.sql.adaptive.enabled": "true"
})
```

**Methods:**
- `createDataFrame(data, schema=None)` - Create DataFrame from Python data
- `read` - Access to data source readers
- `sql(query)` - Execute SQL queries
- `catalog` - Access to catalog operations
- `conf` - Configuration management

### Configuration

```python
# Set configuration
spark.conf.set("spark.sql.debug", "true")

# Get configuration
debug_mode = spark.conf.get("spark.sql.debug")
```

## DataFrame Operations

### Creation

```python
# From Python data
df = spark.createDataFrame([
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30}
])

# With explicit schema
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

# Empty DataFrames require explicit StructType schema (PySpark compatibility)
# ❌ This will raise ValueError:
# df = spark.createDataFrame([], ['col1', 'col2'])

# ✅ Correct way:
from sparkless.sql.types import StructType, StructField, StringType, IntegerType
empty_schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", IntegerType(), True)
])
df = spark.createDataFrame([], empty_schema)
```

### Set Operations

```python
# Union operations require compatible schemas (PySpark compatibility)
df1 = spark.createDataFrame([("a", 1)], ["col1", "col2"])
df2 = spark.createDataFrame([("b", 2)], ["col1", "col2"])

# ✅ Compatible schemas - same column names and compatible types
result = df1.union(df2)

# ❌ This will raise AnalysisException (different column counts):
# df3 = spark.createDataFrame([("c",)], ["col1"])
# result = df1.union(df3)

# Numeric type promotion is allowed (IntegerType -> LongType -> FloatType -> DoubleType)
schema1 = StructType([StructField("value", IntegerType(), True)])
schema2 = StructType([StructField("value", LongType(), True)])
df1 = spark.createDataFrame([(1,)], schema1)
df2 = spark.createDataFrame([(2,)], schema2)
result = df1.union(df2)  # ✅ Works - numeric types are compatible
```

### Column Access

```python
# Both syntaxes supported
df.select("name", df.age)  # ✅ Direct column access
df.select(F.col("name"), F.col("age"))  # ✅ F.col syntax
```

### Selection and Filtering

```python
# Select columns
df.select("id", "name")
df.select(df.id, df.name)
df.select(F.col("id"), F.col("name"))

# Filter rows
df.filter(df.age > 25)
df.filter(F.col("age") > 25)
df.where(df.age > 25)

# Multiple conditions
df.filter((df.age > 25) & (df.salary > 50000))
df.filter(df.age > 25).filter(df.salary > 50000)
```

### Column Operations

```python
# Add new columns
df.withColumn("full_name", F.concat(df.first_name, F.lit(" "), df.last_name))
df.withColumn("age_group", F.when(df.age < 30, "young").otherwise("old"))

# Rename columns
df.withColumnRenamed("old_name", "new_name")

# Drop columns
df.drop("unwanted_column")
df.drop("col1", "col2")
```

### Aggregations

```python
# Simple aggregations
df.agg(F.count("*"))
df.agg(F.sum("salary"))
df.agg(F.avg("age"))

# Multiple aggregations
df.agg(
    F.count("*").alias("count"),
    F.avg("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary")
)

# Group by aggregations
df.groupBy("department").agg(
    F.count("*").alias("count"),
    F.avg("salary").alias("avg_salary")
)
```

## Functions Reference

### Column Functions

#### Basic Operations

```python
from sparkless.sql import functions as F

# Literal values
F.lit("constant")
F.lit(42)
F.lit(True)

# Column references
F.col("column_name")
F.col("table.column_name")

# Column operations
F.col("age") + 1
F.col("salary") * 1.1
F.col("name").isNull()
F.col("age").isNotNull()
```

#### String Functions

```python
# String operations
F.concat(F.col("first"), F.lit(" "), F.col("last"))
F.concat_ws("-", F.col("col1"), F.col("col2"))
F.length(F.col("name"))
F.upper(F.col("name"))
F.lower(F.col("name"))
F.trim(F.col("name"))
F.ltrim(F.col("name"))
F.rtrim(F.col("name"))

# String matching
F.col("name").contains("Alice")
F.col("email").startswith("user@")
F.col("email").endswith(".com")
F.col("name").like("A%")
F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# String replacement
F.regexp_replace(F.col("text"), r"\d+", "NUMBER")
F.translate(F.col("text"), "abc", "xyz")
```

#### Numeric Functions

```python
# Basic arithmetic
F.col("salary") + F.col("bonus")
F.col("price") * 1.1
F.col("total") / F.col("count")

# Mathematical functions
F.abs(F.col("value"))
F.ceil(F.col("value"))
F.floor(F.col("value"))
F.round(F.col("value"), 2)
F.sqrt(F.col("value"))
F.pow(F.col("base"), F.col("exponent"))

# Aggregation functions
F.sum(F.col("amount"))
F.avg(F.col("price"))
F.min(F.col("date"))
F.max(F.col("date"))
F.count(F.col("id"))
F.countDistinct(F.col("user_id"))
```

#### Date and Time Functions

```python
# Current date/time
F.current_date()
F.current_timestamp()

# Date extraction
F.year(F.col("date"))
F.month(F.col("date"))
F.day(F.col("date"))
F.hour(F.col("timestamp"))
F.minute(F.col("timestamp"))
F.second(F.col("timestamp"))
F.dayofweek(F.col("date"))
F.dayofyear(F.col("date"))
F.weekofyear(F.col("date"))

# Date conversion
F.to_date(F.col("date_string"))
F.to_timestamp(F.col("timestamp_string"))
F.to_timestamp(F.col("date_string"), "yyyy-MM-dd")

# Date arithmetic
F.date_add(F.col("date"), 7)
F.date_sub(F.col("date"), 7)
F.datediff(F.col("end_date"), F.col("start_date"))
```

#### Conditional Functions

```python
# When/otherwise
F.when(F.col("age") > 65, "senior")
 .when(F.col("age") > 18, "adult")
 .otherwise("minor")

# Case statements
F.expr("CASE WHEN age > 65 THEN 'senior' WHEN age > 18 THEN 'adult' ELSE 'minor' END")

# Null handling
F.coalesce(F.col("col1"), F.col("col2"), F.lit("default"))
F.nvl(F.col("nullable_col"), F.lit("default"))
F.nanvl(F.col("float_col"), F.lit(0.0))

# Null checks
F.col("column").isNull()
F.col("column").isNotNull()
```

#### Type Casting

```python
# Type conversion
F.col("string_number").cast("int")
F.col("int_value").cast("string")
F.col("date_string").cast("date")
F.col("timestamp_string").cast("timestamp")

# Safe casting
F.col("value").cast("int")  # May fail on invalid values
```

## Window Functions

### Window Specification

```python
from sparkless.sql import Window

# Basic window
window = Window.partitionBy("department").orderBy("salary")

# With frame
window = Window.partitionBy("department") \
    .orderBy("salary") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Range frame
window = Window.partitionBy("category") \
    .orderBy("date") \
    .rangeBetween(-7, 0)  # 7 days before to current
```

### Window Functions

```python
# Ranking functions
F.row_number().over(window)
F.rank().over(window)
F.dense_rank().over(window)
F.percent_rank().over(window)
F.ntile(4).over(window)

# Offset functions
F.lag(F.col("value"), 1).over(window)
F.lead(F.col("value"), 1).over(window)

# Aggregate functions
F.sum(F.col("amount")).over(window)
F.avg(F.col("price")).over(window)
F.count("*").over(window)
F.max(F.col("date")).over(window)
F.min(F.col("date")).over(window)

# First/Last functions
F.first(F.col("value")).over(window)
F.last(F.col("value")).over(window)
F.first(F.col("value"), ignoreNulls=True).over(window)
```

## Data Types

### Primitive Types

```python
from sparkless.sql.types import *

# Basic types
StringType()
IntegerType()
LongType()
DoubleType()
FloatType()
BooleanType()
DateType()
TimestampType()
```

### Complex Types

```python
# Array type
# PySpark convention (camelCase keyword - Issue #247)
ArrayType(elementType=StringType())
# Backward-compatible (snake_case keyword)
ArrayType(element_type=StringType())
# Positional argument
ArrayType(StringType())

# Map type
MapType(StringType(), IntegerType())

# Struct type
StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), False)
])

# Nested types
StructType([
    StructField("id", IntegerType(), False),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True)
    ]), True)
])
```

## Catalog Operations

### Database Management

```python
# Create database
spark.catalog.createDatabase("my_db")

# Set current database
spark.catalog.setCurrentDatabase("my_db")

# Get current database
current_db = spark.catalog.currentDatabase()

# List databases
databases = spark.catalog.listDatabases()

# Drop database
spark.catalog.dropDatabase("my_db")
```

### Table Management

```python
# List tables
tables = spark.catalog.listTables()
tables = spark.catalog.listTables("my_db")

# Check if table exists
exists = spark.catalog.tableExists("my_table")
exists = spark.catalog.tableExists("my_db.my_table")

# Get table details
table = spark.catalog.getTable("my_table")

# Drop table
spark.catalog.dropTable("my_table")
```

## Data Sources

### Reading Data

```python
# JSON
df = spark.read.json("data.json")
df = spark.read.json("data.json", schema=my_schema)

# CSV
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Parquet
df = spark.read.parquet("data.parquet")

# Table
df = spark.table("my_table")
df = spark.table("my_db.my_table")
```

### Writing Data

```python
# Save as table
df.write.saveAsTable("my_table")
df.write.saveAsTable("my_table", mode="overwrite")

# Save as file
df.write.json("output.json")
df.write.csv("output.csv")
df.write.parquet("output.parquet")

# Write modes
df.write.mode("overwrite").saveAsTable("my_table")
df.write.mode("append").saveAsTable("my_table")
df.write.mode("ignore").saveAsTable("my_table")
df.write.mode("error").saveAsTable("my_table")  # Default
```

## SQL Operations

### SQL Queries

```python
# Register DataFrame as table
df.createOrReplaceTempView("my_table")

# Execute SQL
result = spark.sql("SELECT * FROM my_table WHERE age > 25")
result = spark.sql("""
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""")
```

### SQL Functions

```python
# SQL functions in expressions
F.expr("CASE WHEN age > 65 THEN 'senior' ELSE 'junior' END")
F.expr("SUBSTRING(name, 1, 3)")
F.expr("DATE_ADD(date_col, 7)")
```

## Error Handling

### Exception Types

```python
from sparkless.core.exceptions import *

# Column not found
MockSparkColumnNotFoundError

# Type mismatch
MockSparkTypeMismatchError

# Operation errors
MockSparkOperationError

# SQL generation errors
MockSparkSQLGenerationError

# Query execution errors
MockSparkQueryExecutionError
```

### Debug Mode

```python
# Enable debug mode
spark.conf.set("spark.sql.debug", "true")

# Or globally
import sparkless
sparkless.set_debug_mode(True)
```

## Performance Tips

### Optimization

```python
# Use column pruning
df.select("id", "name")  # Only select needed columns

# Filter early
df.filter(df.active == True).select("id", "name")

# Use appropriate data types
df.withColumn("id", F.col("id").cast("int"))
```

### Memory Management

```python
# Cache frequently used DataFrames
df.cache()

# Unpersist when done
df.unpersist()

# Check storage level
df.storageLevel
```

## Testing

### Unit Testing

```python
import pytest
from sparkless import SparkSession

@pytest.fixture
def spark():
    return SparkSession("test")

def test_data_processing(spark):
    df = spark.createDataFrame([
        {"id": 1, "value": 10},
        {"id": 2, "value": 20}
    ])
    
    result = df.filter(df.value > 15).collect()
    assert len(result) == 1
```

### Integration Testing

```python
def test_complex_pipeline(spark):
    # Test entire data pipeline
    df = spark.read.json("data.json")
    
    processed = df.filter(df.status == "active") \
        .withColumn("processed_at", F.current_timestamp()) \
        .groupBy("category") \
        .agg(F.count("*").alias("count"))
    
    results = processed.collect()
    assert len(results) > 0
```

## Compatibility Notes

### PySpark Compatibility

- **100% API Compatibility**: All PySpark operations work identically
- **Column Access**: Both `df.column_name` and `F.col("column_name")` supported
- **Data Types**: All PySpark data types supported
- **Functions**: All PySpark functions supported

### Known Differences

- **Backend**: Uses Polars instead of Spark SQL (DuckDB available as optional legacy backend)
- **Performance**: 10x faster than PySpark for most operations
- **Memory**: Lower memory usage, no JVM overhead
- **SQL Generation**: Some complex operations may generate different SQL

### Migration from PySpark

See `docs/migration_from_pyspark.md` for detailed migration guide.

## Examples

### Basic Data Processing

```python
from sparkless.sql import SparkSession, functions as F

# Create session
spark = SparkSession("data_processing")

# Create DataFrame
df = spark.createDataFrame([
    {"id": 1, "name": "Alice", "age": 25, "salary": 50000},
    {"id": 2, "name": "Bob", "age": 30, "salary": 60000},
    {"id": 3, "name": "Charlie", "age": 35, "salary": 70000}
])

# Process data
result = df.filter(df.age > 25) \
    .withColumn("senior", F.when(df.age > 30, True).otherwise(False)) \
    .groupBy("senior") \
    .agg(F.avg("salary").alias("avg_salary")) \
    .collect()

print(result)
```

### Window Functions

```python
from sparkless.sql import Window

# Define window
window = Window.partitionBy("department").orderBy("salary")

# Apply window functions
result = df.withColumn("rank", F.rank().over(window)) \
    .withColumn("row_num", F.row_number().over(window)) \
    .withColumn("lag_salary", F.lag("salary", 1).over(window)) \
    .collect()
```

### Complex Aggregations

```python
# Multiple aggregations
result = df.groupBy("department") \
    .agg(
        F.count("*").alias("count"),
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
        F.min("salary").alias("min_salary")
    ) \
    .collect()
```

This API reference provides comprehensive documentation for all Sparkless functionality. For more examples and patterns, see `docs/testing_patterns.md`.