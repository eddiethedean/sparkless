# Threading


Sparkless's Polars backend is **thread-safe by design**, eliminating threading issues that were present with DuckDB. Polars uses Rayon (Rust's data parallelism library) internally, making it safe for concurrent operations.

## Thread Safety with Polars

Polars backend provides native thread safety:

1. **No Connection Locks**: Polars is thread-safe by design - no need for connection locks or thread-local handling
2. **Concurrent Operations**: Multiple threads can safely operate on DataFrames simultaneously
3. **Parallel Execution**: Polars automatically parallelizes operations across threads when beneficial

## Thread-Safe Operations

All operations with Polars backend are thread-safe:

```python
from sparkless.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor

spark = SparkSession("MyApp")

def create_table_in_thread(schema_name, table_name):
    """Create a table in a worker thread."""
    # No special handling needed - Polars is thread-safe
    df = spark.createDataFrame([{"id": 1, "name": "Alice"}])
    df.write.saveAsTable(f"{schema_name}.{table_name}")

# Use ThreadPoolExecutor - no threading issues!
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [
        executor.submit(create_table_in_thread, f"schema_{i}", f"table_{i}")
        for i in range(10)
    ]
    for future in futures:
        future.result()  # All operations succeed without locks
```

## How It Works

Polars provides thread safety through:

1. **Rust-Based Core**: Polars is built on Rust with Rayon for data parallelism
2. **No Shared Mutable State**: Each operation works on immutable DataFrames
3. **Automatic Parallelization**: Polars parallelizes operations internally when safe
4. **No Connection Management**: Unlike SQL databases, Polars doesn't require connection pooling

## Best Practices

### Using with ThreadPoolExecutor

When using `ThreadPoolExecutor` or similar parallel execution frameworks:

```python
from concurrent.futures import ThreadPoolExecutor

def process_data(schema_name, data):
    """Process data in a worker thread."""
    spark = SparkSession("MyApp")
    df = spark.createDataFrame(data)
    # No threading concerns - Polars handles it
    df.write.saveAsTable(f"{schema_name}.results")

with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [
        executor.submit(process_data, f"schema_{i}", data_list[i])
        for i in range(10)
    ]
    for future in futures:
        future.result()
```

### Using with pytest-xdist

When running tests with `pytest-xdist`:

```bash
# Run tests in parallel with 8 workers - no threading issues!
pytest -n 8 tests/

# Polars backend is thread-safe - no special configuration needed
```

No special configuration is needed - Sparkless with Polars backend handles threading automatically.

## Comparison with DuckDB Backend

### DuckDB Backend (v2.x and earlier)

- **Thread-local connections**: Each thread needed its own connection
- **Schema visibility issues**: Schemas created in one thread weren't visible to others
- **Connection locks required**: Required `_connection_lock` to prevent race conditions
- **Retry logic needed**: Schema creation needed retries with exponential backoff

### Polars Backend (v3.0.0+)

- **Thread-safe by design**: No connection management needed
- **No schema visibility issues**: Polars doesn't use SQL schemas
- **No locks required**: Operations are inherently thread-safe
- **No retry logic**: No race conditions to handle

## Performance Considerations

Polars threading provides excellent performance:

- **Automatic Parallelization**: Polars parallelizes operations across available CPU cores
- **No Lock Overhead**: No connection locks means lower overhead
- **Better Scalability**: Can safely use many threads without contention

## Example: Parallel Pipeline Execution

```python
from sparkless.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor

def run_pipeline_step(step_id, input_data):
    """Run a pipeline step in a worker thread."""
    spark = SparkSession("Pipeline")
    
    # Create input DataFrame
    df = spark.createDataFrame(input_data)
    
    # Process data
    result = df.select("id", "value").filter("value > 10")
    
    # Save to table (thread-safe - no special handling needed)
    result.write.saveAsTable(f"step_{step_id}.results")
    
    return result.count()

# Run multiple pipeline steps in parallel - no threading issues!
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [
        executor.submit(run_pipeline_step, i, data[i])
        for i in range(10)
    ]
    results = [future.result() for future in futures]
```

## Troubleshooting

### No Threading Issues!

With Polars backend, threading issues should be completely eliminated. If you encounter any issues:

1. **Verify Backend**: Ensure you're using Polars backend (default in v3.0.0+)
   ```python
   from sparkless.backend.factory import BackendFactory
   backend_type = BackendFactory.get_backend_type(spark._storage)
   assert backend_type == "polars"
   ```

2. **Check Polars Version**: Ensure you have a recent version of Polars
   ```bash
   pip install polars>=0.20.0
   ```

3. **No Special Configuration**: No threading-related configuration needed

## Migration from DuckDB

If you're migrating from DuckDB backend and had threading issues:

- **No more locks**: Remove any threading workarounds
- **No retry logic**: Remove schema creation retries
- **Simpler code**: Threading concerns are handled automatically

## See Also

- [Configuration Guide](./configuration.md) - Configuration options for Sparkless
- [Pytest Integration Guide](./pytest_integration.md) - Using Sparkless with pytest
- [Memory Management Guide](./memory_management.md) - Managing memory in parallel contexts
- [Migration Guide](../migration_from_v2_to_v3.md) - Migrating from DuckDB to Polars
