# Backend Architecture


## Overview

This document describes the backend architecture with Polars as the default backend (v3.0.0+). The architecture supports multiple backends (Polars, memory, file) through a pluggable system with protocol-based interfaces.

## Architecture Changes

### Before Refactor

```
sparkless/
  storage/
    backends/
      (legacy storage implementations)
  dataframe/
    (legacy materialization implementations)
    export.py                # Mixed export logic
  session/
    core/
      session.py             # Direct backend instantiation
```

**Issues:**
- Backend logic scattered across multiple modules
- Tight coupling between components
- Direct instantiation prevents dependency injection
- Difficult to test modules independently
- No clear separation between backend and business logic

### Current Architecture (v3.0.0+)

```
sparkless/
  backend/
    __init__.py
    protocols.py             # Protocol definitions
    factory.py               # Backend factory
    polars/
      __init__.py
      storage.py             # Polars storage backend (Parquet-based)
      materializer.py        # Polars lazy evaluation
      expression_translator.py  # MockColumn → Polars expressions
      operation_executor.py    # DataFrame operations
      window_handler.py        # Window functions
      export.py              # Polars export utilities
      type_mapper.py         # Type conversion
      schema_registry.py     # JSON schema storage
      parquet_storage.py     # Parquet file operations
  session/
    core/
      session.py             # Uses BackendFactory + protocols
  dataframe/
    lazy.py                  # Uses BackendFactory
    export.py                # Delegates to backend
  storage/
    __init__.py              # Re-exports for backward compatibility
```

**Benefits:**
- All backend logic centralized in `sparkless/backend/`
- Modules decoupled via protocol interfaces
- Dependency injection via `BackendFactory`
- Easy to test with mock backends
- Clear separation of concerns
- Multiple backend support (Polars default, memory, file)
- Thread-safe by design (Polars backend)

## Protocol Definitions

### QueryExecutor Protocol

Defines the interface for executing queries on data.

```python
class QueryExecutor(Protocol):
    def execute_query(self, query: str) -> List[Dict[str, Any]]: ...
    def create_table(self, name: str, schema: MockStructType, data: List[Dict]): ...
    def close(self) -> None: ...
```

### DataMaterializer Protocol

Defines the interface for materializing lazy DataFrame operations.

```python
class DataMaterializer(Protocol):
    def materialize(
        self, data: List[Dict], schema: MockStructType, operations: List[Tuple]
    ) -> List[MockRow]: ...
    def close(self) -> None: ...
```

### StorageBackend Protocol

Defines the interface for storage operations (schemas, tables, data).

```python
class StorageBackend(Protocol):
    def create_schema(self, schema: str) -> None: ...
    def create_table(self, schema: str, table: str, columns) -> Optional[Any]: ...
    def insert_data(self, schema: str, table: str, data: List[Dict], mode: str) -> None: ...
    def query_table(self, schema: str, table: str, filter_expr: Optional[str]) -> List[Dict]: ...
    # ... other storage methods
```

### ExportBackend Protocol

Defines the interface for DataFrame export operations.

```python
class ExportBackend(Protocol):
    # Export methods are implemented directly in backend implementations
    ...
```

## Backend Factory

The `BackendFactory` provides centralized backend instantiation with dependency injection support.

```python
# Creating backends (Polars is default)
storage = BackendFactory.create_storage_backend("polars")
materializer = BackendFactory.create_materializer("polars")
exporter = BackendFactory.create_export_backend("polars")

# Using in session with DI
spark = SparkSession("app", storage_backend=custom_storage)
```

## Usage Examples

### Session with Default Backend

```python
from sparkless.sql import SparkSession

# Uses Polars backend by default (v3.0.0+)
spark = SparkSession("MyApp")
```

### Session with Custom Backend

```python
from sparkless.sql import SparkSession
from sparkless.backend.factory import BackendFactory

# Create custom backend (Polars)
custom_storage = BackendFactory.create_storage_backend("polars")

# Inject into session
spark = SparkSession("MyApp", storage_backend=custom_storage)
```

### Testing with Mock Backend

```python
from sparkless.sql import SparkSession
from unittest.mock import Mock

# Create mock backend for testing
mock_storage = Mock()
mock_storage.create_table.return_value = None

# Inject mock
spark = SparkSession("Test", storage_backend=mock_storage)

# Verify interactions
mock_storage.create_table.assert_called_once()
```

## Backend Configuration

### Configuration via Session Builder

Backend selection is now configurable through the session builder's `.config()` method:

```python
from sparkless.sql import SparkSession

# Default backend (Polars) - v3.0.0+
spark = SparkSession("MyApp")

# Explicit backend selection
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "polars") \
    .getOrCreate()

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

### Configuration Keys

| Key | Description | Default | Example |
|-----|-------------|---------|---------|
| `spark.sparkless.backend` | Backend type | `"polars"` | `"polars"`, `"memory"`, `"file"` |
| `spark.sparkless.backend.maxMemory` | Memory limit | `"1GB"` | `"4GB"`, `"8GB"` |
| `spark.sparkless.backend.allowDiskSpillover` | Allow disk usage | `false` | `true`, `false` |
| `spark.sparkless.backend.basePath` | Base path for file backend | `"sparkless_storage"` | `"/tmp/data"` |

### Backend Type Detection

The system automatically detects backend types from storage instances:

```python
from sparkless.backend.factory import BackendFactory

# Create a storage backend
storage = BackendFactory.create_storage_backend("polars")

# Detect the backend type
backend_type = BackendFactory.get_backend_type(storage)
print(backend_type)  # "polars"

# List available backends
available = BackendFactory.list_available_backends()
print(available)  # ["polars", "memory", "file"]
```

### Adding New Backends

To add a new backend implementation:

1. **Implement the protocols** in `sparkless/backend/<backend_name>/`:
   - `storage.py` - Implements `StorageBackend` protocol
   - `materializer.py` - Implements `DataMaterializer` protocol  
   - `export.py` - Implements `ExportBackend` protocol
   - `query_executor.py` - Implements `QueryExecutor` protocol

2. **Register in BackendFactory**:
   ```python
   # In create_storage_backend()
   elif backend_type == "new_backend":
       from .new_backend.storage import NewBackendStorageManager
       return NewBackendStorageManager(**kwargs)
   ```

3. **Add configuration support**:
   ```python
   # Add new config keys for backend-specific options
   .config("spark.sparkless.backend.newBackend.option", "value")
   ```

4. **Update detection logic**:
   ```python
   # In get_backend_type()
   elif "new_backend" in module_name:
       return "new_backend"
   ```

## Query Optimizer Hooks

### Adaptive Execution Simulation

Sparkless includes a lightweight adaptive execution simulation layer that can
rewrite logical plans when skewed partitions are detected. The feature is
disabled by default and can be toggled through configuration or
programmatically:

```python
from sparkless.optimizer.query_optimizer import QueryOptimizer

optimizer = QueryOptimizer()
optimizer.configure_adaptive_execution(
    enabled=True,
    skew_threshold=1.8,
    max_split_factor=8,
)
```

When enabled, the optimizer inspects the `skew_metrics` metadata attached to
operations. For joins or aggregations that exceed the configured threshold, the
optimizer injects a synthetic `REPARTITION` step with guidance about the target
split factor and affected columns:

```python
operation.metadata["skew_metrics"] = {
    "max_partition_ratio": 3.2,
    "partition_columns": ["region"],
    "hot_partitions": ["us-east"],
}
```

Runtime systems can also pass observed statistics at optimize time:

```python
plan = optimizer.optimize(
    operations,
    runtime_stats={
        "skew_hints": {
            "join-hotspot": {
                "max_partition_ratio": 6.5,
                "partition_columns": ["id"],
            }
        }
    },
)
```

Each synthesized `REPARTITION` operation carries structured metadata (`reason`,
`target_split_factor`, `skew_metrics`) that downstream components can use to
adjust execution strategies or produce diagnostics.

## Backward Compatibility

All existing imports continue to work via re-exports:

```python
# Still works - imports from new location transparently
from sparkless.storage import PolarsStorageManager

# Also works - explicit new import
from sparkless.backend.polars import PolarsStorageManager

# Factory pattern (recommended)
from sparkless.backend.factory import BackendFactory
storage = BackendFactory.create_storage_backend("polars")
```

## Migration Guide

### For Users

No changes required! All existing code continues to work.

### For Contributors

When adding new backend functionality:

1. Define protocol in `backend/protocols.py` if needed
2. Implement in appropriate `backend/<backend_type>/` directory
3. Update `BackendFactory` to support new backend
4. Add tests for new backend
5. Update this documentation

### For Testing

Use protocols for easier mocking:

```python
from sparkless.backend.protocols import StorageBackend
from typing import cast

def test_with_mock():
    mock_storage = Mock(spec=StorageBackend)
    spark = SparkSession("test", storage_backend=cast(StorageBackend, mock_storage))
    # Test with mock backend
```

## Test Results

After refactor:
- All tests passing ✅
- All compatibility tests passing ✅
- Backward compatibility maintained ✅

## Future Enhancements

Potential improvements enabled by this architecture:

1. **Additional Backends**: Easy to add SQLite, PostgreSQL, etc.
2. **Backend Switching**: Swap backends at runtime
3. **Performance Comparison**: Compare backend performance
4. **Custom Backends**: Users can provide their own implementations
5. **Backend Plugins**: Plugin system for third-party backends

## File Mapping

The backend architecture centralizes all backend logic in `sparkless/backend/`:
- Polars backend: `backend/polars/`
- Memory backend: `storage/backends/memory.py`
- File backend: `storage/backends/file.py`

## Summary

This refactor successfully:

✅ Isolated all backend logic in `sparkless/backend/`  
✅ Defined clear protocol interfaces for decoupling  
✅ Implemented dependency injection via `BackendFactory`  
✅ Maintained full backward compatibility  
✅ Passed all existing tests (510 passing)  
✅ Improved testability with protocol-based mocking  
✅ Enhanced maintainability with clear separation of concerns  

The architecture now follows SOLID principles, making the codebase more modular, testable, and extensible.

