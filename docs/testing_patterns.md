# Testing Patterns


## Overview

Sparkless is designed for testing PySpark applications with 100% API compatibility. This guide covers best practices for testing with Sparkless, including setup, patterns, and optimization techniques.

## Setup Test Fixtures

### Basic Setup

```python
import pytest
from sparkless.sql import SparkSession, functions as F
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession("test_app")

@pytest.fixture
def sample_data():
    """Sample data for testing."""
    return [
        {"id": 1, "name": "Alice", "age": 25, "salary": 50000},
        {"id": 2, "name": "Bob", "age": 30, "salary": 60000},
        {"id": 3, "name": "Charlie", "age": 35, "salary": 70000}
    ]

@pytest.fixture
def sample_schema():
    """Sample schema for testing."""
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", IntegerType(), True)
    ])
```

### Advanced Setup

```python
@pytest.fixture(scope="session")
def spark_with_config():
    """Create SparkSession with specific configuration."""
    return SparkSession("test_app", config={
        "spark.sql.debug": "true",
        "spark.sql.adaptive.enabled": "true"
    })

@pytest.fixture
def large_dataset():
    """Generate large dataset for performance testing."""
    import random
    return [
        {
            "id": i,
            "name": f"User{i}",
            "age": random.randint(18, 65),
            "salary": random.randint(30000, 100000),
            "department": random.choice(["IT", "HR", "Finance", "Marketing"])
        }
        for i in range(10000)
    ]

@pytest.fixture
def complex_schema():
    """Complex schema with nested types."""
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True)
        ]), True),
        StructField("scores", ArrayType(elementType=IntegerType()), True)
    ])
```

## Test Patterns

### Basic DataFrame Operations

```python
def test_dataframe_creation(spark, sample_data, sample_schema):
    """Test DataFrame creation from data."""
    df = spark.createDataFrame(sample_data, sample_schema)
    
    assert df.count() == 3
    assert len(df.columns) == 4
    assert "id" in df.columns
    assert "name" in df.columns

def test_column_access(spark, sample_data):
    """Test both column access patterns."""
    df = spark.createDataFrame(sample_data)
    
    # Test direct column access
    result1 = df.select(df.id, df.name).collect()
    
    # Test F.col access
    result2 = df.select(F.col("id"), F.col("name")).collect()
    
    assert result1 == result2

def test_filtering(spark, sample_data):
    """Test filtering operations."""
    df = spark.createDataFrame(sample_data)
    
    # Test simple filter
    filtered = df.filter(df.age > 25)
    assert filtered.count() == 2
    
    # Test complex filter
    complex_filtered = df.filter((df.age > 25) & (df.salary > 55000))
    assert complex_filtered.count() == 1

def test_column_operations(spark, sample_data):
    """Test column operations."""
    df = spark.createDataFrame(sample_data)
    
    # Test withColumn
    result = df.withColumn("age_group", 
                          F.when(df.age < 30, "young")
                           .when(df.age < 50, "middle")
                           .otherwise("senior"))
    
    assert "age_group" in result.columns
    assert result.count() == 3
```

### Aggregation Testing

```python
def test_simple_aggregations(spark, sample_data):
    """Test basic aggregation functions."""
    df = spark.createDataFrame(sample_data)
    
    # Test single aggregation
    result = df.agg(F.avg("salary")).collect()
    assert len(result) == 1
    assert result[0]["avg(salary)"] == 60000.0
    
    # Test multiple aggregations
    result = df.agg(
        F.count("*").alias("count"),
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary")
    ).collect()
    
    assert result[0]["count"] == 3
    assert result[0]["avg_salary"] == 60000.0
    assert result[0]["max_salary"] == 70000

def test_group_by_aggregations(spark, sample_data):
    """Test groupBy aggregations."""
    # Add department column
    df = spark.createDataFrame(sample_data)
    df_with_dept = df.withColumn("department", 
                                F.when(df.id == 1, "IT")
                                 .when(df.id == 2, "HR")
                                 .otherwise("Finance"))
    
    result = df_with_dept.groupBy("department") \
        .agg(F.count("*").alias("count"),
             F.avg("salary").alias("avg_salary")) \
        .collect()
    
    assert len(result) == 3
    # Verify each department has correct count
    dept_counts = {row["department"]: row["count"] for row in result}
    assert dept_counts["IT"] == 1
    assert dept_counts["HR"] == 1
    assert dept_counts["Finance"] == 1
```

### Window Function Testing

```python
def test_window_functions(spark, sample_data):
    """Test window functions."""
    df = spark.createDataFrame(sample_data)
    
    # Define window
    window = Window.partitionBy("department").orderBy("salary")
    
    # Add department column
    df_with_dept = df.withColumn("department", 
                                F.when(df.id == 1, "IT")
                                 .when(df.id == 2, "IT")
                                 .otherwise("HR"))
    
    result = df_with_dept.withColumn("rank", F.rank().over(window)) \
        .withColumn("row_num", F.row_number().over(window)) \
        .collect()
    
    assert len(result) == 3
    # Verify ranking works correctly
    for row in result:
        assert "rank" in row
        assert "row_num" in row

def test_window_boundaries(spark, sample_data):
    """Test window boundaries."""
    df = spark.createDataFrame(sample_data)
    
    # Test ROWS BETWEEN
    window_rows = Window.orderBy("salary") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    result = df.withColumn("running_sum", F.sum("salary").over(window_rows)) \
        .collect()
    
    assert len(result) == 3
    # Verify running sum
    running_sums = [row["running_sum"] for row in result]
    assert running_sums[0] == 50000  # First row
    assert running_sums[1] == 110000  # First + second
    assert running_sums[2] == 180000  # All three
```

### String Function Testing

```python
def test_string_functions(spark, sample_data):
    """Test string functions."""
    df = spark.createDataFrame(sample_data)
    
    result = df.withColumn("upper_name", F.upper(df.name)) \
        .withColumn("name_length", F.length(df.name)) \
        .withColumn("starts_with_a", df.name.startswith("A")) \
        .collect()
    
    assert len(result) == 3
    for row in result:
        assert "upper_name" in row
        assert "name_length" in row
        assert "starts_with_a" in row

def test_regex_operations(spark, sample_data):
    """Test regex operations."""
    df = spark.createDataFrame(sample_data)
    
    # Test rlike
    result = df.filter(df.name.rlike("^[A-Z]")) \
        .collect()
    
    assert len(result) == 3  # All names start with capital letter
    
    # Test regexp_replace
    result = df.withColumn("name_clean", 
                          F.regexp_replace(df.name, "e", "X")) \
        .collect()
    
    assert len(result) == 3
```

### Date and Time Testing

```python
def test_datetime_functions(spark):
    """Test datetime functions."""
    data = [
        {"id": 1, "date_str": "2024-01-15", "timestamp_str": "2024-01-15 10:30:00"},
        {"id": 2, "date_str": "2024-01-16", "timestamp_str": "2024-01-16 14:45:00"}
    ]
    
    df = spark.createDataFrame(data)
    
    result = df.withColumn("date_col", F.to_date(df.date_str)) \
        .withColumn("timestamp_col", F.to_timestamp(df.timestamp_str)) \
        .withColumn("year", F.year(F.to_date(df.date_str))) \
        .withColumn("hour", F.hour(F.to_timestamp(df.timestamp_str))) \
        .collect()
    
    assert len(result) == 2
    for row in result:
        assert "date_col" in row
        assert "timestamp_col" in row
        assert "year" in row
        assert "hour" in row

def test_date_arithmetic(spark):
    """Test date arithmetic."""
    data = [
        {"id": 1, "date": "2024-01-15"},
        {"id": 2, "date": "2024-01-16"}
    ]
    
    df = spark.createDataFrame(data)
    
    result = df.withColumn("date_plus_7", F.date_add(F.to_date(df.date), 7)) \
        .withColumn("date_minus_7", F.date_sub(F.to_date(df.date), 7)) \
        .collect()
    
    assert len(result) == 2
    for row in result:
        assert "date_plus_7" in row
        assert "date_minus_7" in row
```

### Type Casting Testing

```python
def test_type_casting(spark, sample_data):
    """Test type casting operations."""
    df = spark.createDataFrame(sample_data)
    
    result = df.withColumn("age_str", df.age.cast("string")) \
        .withColumn("salary_double", df.salary.cast("double")) \
        .withColumn("id_long", df.id.cast("long")) \
        .collect()
    
    assert len(result) == 3
    for row in result:
        assert "age_str" in row
        assert "salary_double" in row
        assert "id_long" in row

def test_safe_casting(spark):
    """Test safe casting with invalid values."""
    data = [
        {"id": 1, "value": "10.5"},
        {"id": 2, "value": "invalid"},
        {"id": 3, "value": "30.9"}
    ]
    
    df = spark.createDataFrame(data)
    
    result = df.withColumn("value_double", df.value.cast("double")) \
        .collect()
    
    assert len(result) == 3
    # Verify that invalid values are handled appropriately
    for row in result:
        assert "value_double" in row
```

## Performance Testing

### Benchmarking

```python
import time
import pytest

def test_performance_basic_operations(spark, large_dataset):
    """Test performance of basic operations."""
    df = spark.createDataFrame(large_dataset)
    
    start = time.time()
    result = df.filter(df.age > 30).count()
    end = time.time()
    
    assert result > 0
    assert (end - start) < 1.0  # Should complete in under 1 second

def test_performance_aggregations(spark, large_dataset):
    """Test performance of aggregation operations."""
    df = spark.createDataFrame(large_dataset)
    
    start = time.time()
    result = df.groupBy("department") \
        .agg(F.count("*").alias("count"),
             F.avg("salary").alias("avg_salary")) \
        .collect()
    end = time.time()
    
    assert len(result) > 0
    assert (end - start) < 2.0  # Should complete in under 2 seconds

def test_performance_window_functions(spark, large_dataset):
    """Test performance of window functions."""
    df = spark.createDataFrame(large_dataset)
    
    window = Window.partitionBy("department").orderBy("salary")
    
    start = time.time()
    result = df.withColumn("rank", F.rank().over(window)) \
        .collect()
    end = time.time()
    
    assert len(result) > 0
    assert (end - start) < 3.0  # Should complete in under 3 seconds
```

### Memory Testing

```python
def test_memory_usage(spark, large_dataset):
    """Test memory usage patterns."""
    df = spark.createDataFrame(large_dataset)
    
    # Test caching
    df.cache()
    assert df.storageLevel is not None
    
    # Test uncaching
    df.unpersist()
    
    # Test multiple operations without memory leaks
    for i in range(10):
        result = df.filter(df.age > 30).count()
        assert result > 0
```

## Error Handling Testing

### Exception Testing

```python
def test_column_not_found_error(spark, sample_data):
    """Test column not found error handling."""
    df = spark.createDataFrame(sample_data)
    
    with pytest.raises(Exception):  # Should raise MockSparkColumnNotFoundError
        df.select("nonexistent_column").collect()

def test_type_mismatch_error(spark):
    """Test type mismatch error handling."""
    data = [{"id": 1, "value": "not_a_number"}]
    df = spark.createDataFrame(data)
    
    with pytest.raises(Exception):  # Should raise MockSparkTypeMismatchError
        df.select(df.value.cast("int")).collect()

def test_sql_generation_error(spark, sample_data):
    """Test SQL generation error handling."""
    df = spark.createDataFrame(sample_data)
    
    # This might cause SQL generation issues
    with pytest.raises(Exception):
        df.select(df.id.cast("timestamp")).collect()
```

### Debug Mode Testing

```python
def test_debug_mode(spark, sample_data, capsys):
    """Test debug mode output."""
    # Enable debug mode
    spark.conf.set("spark.sql.debug", "true")
    
    df = spark.createDataFrame(sample_data)
    df.filter(df.age > 25).collect()
    
    # Check if debug output was produced
    captured = capsys.readouterr()
    assert "DEBUG" in captured.out or "debug" in captured.out.lower()

def test_error_messages(spark, sample_data):
    """Test error message quality."""
    df = spark.createDataFrame(sample_data)
    
    try:
        df.select("invalid_column").collect()
    except Exception as e:
        error_msg = str(e)
        assert "Column" in error_msg
        assert "not found" in error_msg
        assert "Available columns" in error_msg
```

## Integration Testing

### End-to-End Testing

```python
def test_complete_data_pipeline(spark, sample_data):
    """Test complete data processing pipeline."""
    # Create DataFrame
    df = spark.createDataFrame(sample_data)
    
    # Process data
    result = df.filter(df.age > 25) \
        .withColumn("age_group", F.when(df.age > 30, "senior").otherwise("adult")) \
        .withColumn("salary_bucket", F.when(df.salary > 60000, "high").otherwise("low")) \
        .groupBy("age_group", "salary_bucket") \
        .agg(F.count("*").alias("count"),
             F.avg("salary").alias("avg_salary")) \
        .orderBy("age_group", "salary_bucket") \
        .collect()
    
    assert len(result) > 0
    for row in result:
        assert "age_group" in row
        assert "salary_bucket" in row
        assert "count" in row
        assert "avg_salary" in row

def test_complex_window_operations(spark, sample_data):
    """Test complex window operations."""
    df = spark.createDataFrame(sample_data)
    
    # Add department column
    df_with_dept = df.withColumn("department", 
                                F.when(df.id == 1, "IT")
                                 .when(df.id == 2, "IT")
                                 .otherwise("HR"))
    
    # Complex window operations
    window = Window.partitionBy("department").orderBy("salary")
    
    result = df_with_dept.withColumn("rank", F.rank().over(window)) \
        .withColumn("dense_rank", F.dense_rank().over(window)) \
        .withColumn("row_number", F.row_number().over(window)) \
        .withColumn("lag_salary", F.lag("salary", 1).over(window)) \
        .withColumn("lead_salary", F.lead("salary", 1).over(window)) \
        .withColumn("running_sum", F.sum("salary").over(window)) \
        .collect()
    
    assert len(result) == 3
    for row in result:
        assert "rank" in row
        assert "dense_rank" in row
        assert "row_number" in row
        assert "lag_salary" in row
        assert "lead_salary" in row
        assert "running_sum" in row
```

## Best Practices

### Test Organization

```python
# Group related tests in classes
class TestDataFrameOperations:
    def test_basic_operations(self, spark, sample_data):
        pass
    
    def test_column_operations(self, spark, sample_data):
        pass
    
    def test_filtering(self, spark, sample_data):
        pass

class TestWindowFunctions:
    def test_ranking_functions(self, spark, sample_data):
        pass
    
    def test_aggregate_functions(self, spark, sample_data):
        pass
    
    def test_offset_functions(self, spark, sample_data):
        pass
```

### Test Data Management

```python
# Use parametrized tests for multiple datasets
@pytest.mark.parametrize("dataset_name,expected_count", [
    ("small_dataset", 3),
    ("medium_dataset", 100),
    ("large_dataset", 10000)
])
def test_dataset_size(spark, dataset_name, expected_count):
    """Test with different dataset sizes."""
    # Load dataset based on name
    df = spark.createDataFrame(get_dataset(dataset_name))
    assert df.count() == expected_count

# Use fixtures for complex data setup
@pytest.fixture
def hierarchical_data():
    """Create hierarchical test data."""
    return [
        {"id": 1, "parent_id": None, "name": "Root", "level": 0},
        {"id": 2, "parent_id": 1, "name": "Child1", "level": 1},
        {"id": 3, "parent_id": 1, "name": "Child2", "level": 1},
        {"id": 4, "parent_id": 2, "name": "Grandchild1", "level": 2}
    ]
```

### Performance Optimization

```python
# Use appropriate test data sizes
@pytest.fixture
def test_data_size():
    """Determine appropriate test data size."""
    import os
    if os.getenv("CI"):  # Smaller data in CI
        return 1000
    else:  # Larger data locally
        return 10000

# Optimize test execution
@pytest.fixture(scope="session")
def spark_session():
    """Reuse Spark session across tests."""
    return SparkSession("test_session")

# Clean up resources
@pytest.fixture(autouse=True)
def cleanup_spark(spark_session):
    """Clean up Spark resources after each test."""
    yield
    spark_session.stop()
```

## Common Pitfalls

### Memory Issues

```python
# Don't collect large datasets unnecessarily
def test_avoid_collecting_large_data(spark, large_dataset):
    """Avoid collecting large datasets."""
    df = spark.createDataFrame(large_dataset)
    
    # Good: Use count() instead of collect()
    count = df.filter(df.age > 30).count()
    assert count > 0
    
    # Bad: Don't do this with large datasets
    # results = df.filter(df.age > 30).collect()
```

### Type Issues

```python
# Be explicit about data types
def test_explicit_types(spark):
    """Use explicit types for better testing."""
    data = [{"id": "1", "value": "10.5"}]
    
    # Good: Explicit schema
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    # Test type casting
    result = df.withColumn("id_int", df.id.cast("int")) \
        .withColumn("value_double", df.value.cast("double")) \
        .collect()
    
    assert result[0]["id_int"] == 1
    assert result[0]["value_double"] == 10.5
```

### Error Handling

```python
# Test error conditions properly
def test_error_conditions(spark, sample_data):
    """Test error conditions properly."""
    df = spark.createDataFrame(sample_data)
    
    # Test with invalid operations
    with pytest.raises(Exception):
        df.select("nonexistent_column").collect()
    
    # Test with type mismatches
    with pytest.raises(Exception):
        df.select(df.name.cast("int")).collect()
    
    # Test with invalid window specifications
    with pytest.raises(Exception):
        invalid_window = Window.partitionBy("nonexistent_column")
        df.withColumn("rank", F.rank().over(invalid_window)).collect()
```

This testing patterns guide provides comprehensive coverage of testing with Sparkless. For more examples and advanced patterns, see the test files in the `tests/` directory.
