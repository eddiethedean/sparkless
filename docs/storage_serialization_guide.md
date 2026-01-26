
This guide provides comprehensive documentation for Sparkless's storage serialization capabilities, including CSV, JSON, and other format support.


## Overview

Sparkless provides a complete storage serialization system that supports multiple data formats:

- **Parquet Storage** - Columnar storage format (default for Polars backend in v3.0.0+)
- **CSV Serialization** - Comma-separated values format
- **JSON Serialization** - JavaScript Object Notation format
- **Custom Formats** - Extensible serialization framework

**Note**: With Polars backend (v3.0.0+), tables are persisted as Parquet files by default. CSV and JSON serialization are still available for export/import operations.

## CSV Serialization

The `CSVSerializer` class provides CSV serialization and deserialization capabilities.

### Basic Usage

```python
from sparkless.storage.serialization.csv import CSVSerializer
from sparkless.sql import SparkSession

# Create Spark session
spark = SparkSession("CSVExample")

# Create sample data
data = [
    {"name": "Alice", "age": 25, "city": "New York"},
    {"name": "Bob", "age": 30, "city": "San Francisco"},
    {"name": "Charlie", "age": 35, "city": "Chicago"}
]
df = spark.createDataFrame(data)

# Serialize to CSV
csv_serializer = CSVSerializer()
csv_data = csv_serializer.serialize(df.collect())
print(csv_data)
# name,age,city
# Alice,25,New York
# Bob,30,San Francisco
# Charlie,35,Chicago
```

### CSV Options

```python
# Custom CSV options
csv_serializer = CSVSerializer(
    delimiter=',',           # Field delimiter
    quote_char='"',          # Quote character
    escape_char='\\',        # Escape character
    header=True,             # Include header row
    null_value='',           # Null value representation
    date_format='yyyy-MM-dd', # Date format
    timestamp_format='yyyy-MM-dd HH:mm:ss' # Timestamp format
)

csv_data = csv_serializer.serialize(df.collect())
```

### Deserialization

```python
# Deserialize CSV data
csv_data = """name,age,city
Alice,25,New York
Bob,30,San Francisco
Charlie,35,Chicago"""

deserialized_data = csv_serializer.deserialize(csv_data)
print(deserialized_data)
# [{'name': 'Alice', 'age': 25, 'city': 'New York'}, ...]
```

### Schema-Aware Serialization

```python
from sparkless.spark_types import MockStructType, MockStructField, StringType, IntegerType

# Define schema
schema = MockStructType([
    MockStructField("name", StringType()),
    MockStructField("age", IntegerType()),
    MockStructField("city", StringType())
])

# Serialize with schema
csv_data = csv_serializer.serialize_with_schema(df.collect(), schema)

# Deserialize with schema
deserialized_data = csv_serializer.deserialize_with_schema(csv_data, schema)
```

### Advanced CSV Features

```python
# Custom field formatting
def format_age(age):
    return f"Age: {age}"

csv_serializer.add_formatter("age", format_age)

# Custom field parsing
def parse_age(age_str):
    return int(age_str.replace("Age: ", ""))

csv_serializer.add_parser("age", parse_age)

# Serialize with custom formatting
csv_data = csv_serializer.serialize(df.collect())
```

## JSON Serialization

The `JSONSerializer` class provides JSON serialization and deserialization capabilities.

### Basic Usage

```python
from sparkless.storage.serialization.json import JSONSerializer

# Create JSON serializer
json_serializer = JSONSerializer()

# Serialize to JSON
json_data = json_serializer.serialize(df.collect())
print(json_data)
# [{"name": "Alice", "age": 25, "city": "New York"}, ...]
```

### JSON Options

```python
# Custom JSON options
json_serializer = JSONSerializer(
    indent=2,                # Pretty print with indentation
    ensure_ascii=False,      # Allow non-ASCII characters
    sort_keys=True,          # Sort dictionary keys
    separators=(',', ':'),   # Custom separators
    default=str              # Default serializer for unknown types
)

json_data = json_serializer.serialize(df.collect())
```

### Deserialization

```python
# Deserialize JSON data
json_data = '[{"name": "Alice", "age": 25, "city": "New York"}]'
deserialized_data = json_serializer.deserialize(json_data)
print(deserialized_data)
```

### Schema-Aware JSON

```python
# Serialize with schema
json_data = json_serializer.serialize_with_schema(df.collect(), schema)

# Deserialize with schema
deserialized_data = json_serializer.deserialize_with_schema(json_data, schema)
```

### Custom JSON Serialization

```python
import json
from datetime import datetime

# Custom JSON encoder
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Use custom encoder
json_serializer = JSONSerializer(encoder=CustomJSONEncoder)
json_data = json_serializer.serialize(df.collect())
```

## Storage Integration

### DataFrame Integration

```python
# Write DataFrame to CSV
df.write.format("csv").option("header", "true").save("/path/to/csv")

# Read DataFrame from CSV
df_read = spark.read.format("csv").option("header", "true").load("/path/to/csv")

# Write DataFrame to JSON
df.write.format("json").save("/path/to/json")

# Read DataFrame from JSON
df_read = spark.read.format("json").load("/path/to/json")
```

### Storage Backend Integration

```python
# Note: Polars is the default backend (v3.0.0+)
# DuckDB backend is available as optional/legacy backend

from sparkless.storage.backends.memory import MemoryStorageManager

# Create storage manager (in-memory by default)
storage = MemoryStorageManager()

# Store serialized data
csv_data = csv_serializer.serialize(df.collect())
storage.store_data("table_name", csv_data, format="csv")

# Retrieve and deserialize data
retrieved_data = storage.retrieve_data("table_name", format="csv")
deserialized_data = csv_serializer.deserialize(retrieved_data)
```

## Custom Serialization Formats

### Creating Custom Serializers

```python
from sparkless.storage.serialization.base import BaseSerializer

class CustomSerializer(BaseSerializer):
    def serialize(self, data):
        """Serialize data to custom format."""
        # Custom serialization logic
        return custom_format_data(data)
    
    def deserialize(self, data):
        """Deserialize data from custom format."""
        # Custom deserialization logic
        return custom_parse_data(data)
    
    def get_extension(self):
        """Return file extension for this format."""
        return ".custom"

# Use custom serializer
custom_serializer = CustomSerializer()
custom_data = custom_serializer.serialize(df.collect())
```

### Registering Custom Formats

```python
from sparkless.storage.serialization.registry import SerializationRegistry

# Register custom format
registry = SerializationRegistry()
registry.register("custom", CustomSerializer)

# Use registered format
serializer = registry.get_serializer("custom")
custom_data = serializer.serialize(df.collect())
```

## Performance Optimization

### Streaming Serialization

```python
# Stream large datasets
def stream_serialize(data_stream, serializer):
    for batch in data_stream:
        yield serializer.serialize(batch)

# Use streaming for large datasets
for serialized_batch in stream_serialize(large_data_stream, csv_serializer):
    # Process serialized batch
    process_batch(serialized_batch)
```

### Compression Support

```python
import gzip

# Compressed serialization
def compress_serialize(data, serializer):
    serialized = serializer.serialize(data)
    return gzip.compress(serialized.encode())

def decompress_deserialize(compressed_data, serializer):
    decompressed = gzip.decompress(compressed_data).decode()
    return serializer.deserialize(decompressed)

# Use compression
compressed_data = compress_serialize(df.collect(), csv_serializer)
decompressed_data = decompress_deserialize(compressed_data, csv_serializer)
```

### Memory-Efficient Processing

```python
# Process data in chunks
def chunked_serialize(data, serializer, chunk_size=1000):
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        yield serializer.serialize(chunk)

# Use chunked processing
for serialized_chunk in chunked_serialize(large_data, csv_serializer):
    # Process chunk
    process_chunk(serialized_chunk)
```

## Error Handling

### Serialization Errors

```python
from sparkless.storage.serialization.exceptions import SerializationError

try:
    csv_data = csv_serializer.serialize(invalid_data)
except SerializationError as e:
    print(f"Serialization error: {e}")
    # Handle error appropriately
```

### Deserialization Errors

```python
try:
    data = csv_serializer.deserialize(invalid_csv)
except SerializationError as e:
    print(f"Deserialization error: {e}")
    # Handle error appropriately
```

### Validation Errors

```python
from sparkless.storage.serialization.exceptions import ValidationError

try:
    validated_data = csv_serializer.validate_and_deserialize(csv_data, schema)
except ValidationError as e:
    print(f"Validation error: {e}")
    # Handle validation error
```

## Best Practices

1. **Choose appropriate format** - CSV for simple data, JSON for complex structures
2. **Use schema validation** - Always validate data against expected schema
3. **Handle errors gracefully** - Implement proper error handling
4. **Optimize for size** - Use compression for large datasets
5. **Stream large data** - Process large datasets in chunks
6. **Validate input** - Always validate input data before serialization

## Troubleshooting

### Common Issues

1. **Encoding problems** - Use UTF-8 encoding for international characters
2. **Schema mismatches** - Ensure data matches expected schema
3. **Memory issues** - Use streaming for large datasets
4. **Performance problems** - Use appropriate serialization format

### Debug Mode

```python
# Enable debug mode
csv_serializer.set_debug(True)
json_serializer.set_debug(True)

# Debug information will be printed during serialization/deserialization
```

This comprehensive storage serialization guide provides everything you need to work with Sparkless's serialization capabilities effectively.
