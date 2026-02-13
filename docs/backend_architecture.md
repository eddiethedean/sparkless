# Backend Architecture


## Overview

This document describes the backend architecture. **In v4**, Sparkless re-exports Robin (robin-sparkless) as the execution engine; the only backend supported is Robin. The `BackendFactory` provides storage backend creation for catalog/table use; materializer and export backends have been removed.

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

### Current Architecture (v4)

```
sparkless/
  backend/
    __init__.py
    protocols.py             # Protocol definitions (StorageBackend; materializer/export removed)
    factory.py               # Backend factory (create_storage_backend("robin") only)
    robin/
      __init__.py
      storage.py             # Robin storage backend for catalog/table use
  sql/
    _session.py              # Thin SparkSession wrapper around Robin; createDataFrame(data, schema)
    __init__.py               # Re-exports Robin's DataFrame, Column, F, etc.
  storage/
    __init__.py               # Re-exports for backward compatibility
```

**v4:** Sparkless re-exports Robin (robin-sparkless). Execution uses Robin's DataFrame and session. The factory only provides storage for code that needs catalog/table persistence. Materializer and export backends have been removed.

## Protocol Definitions

### QueryExecutor Protocol

Defines the interface for executing queries on data.

```python
class QueryExecutor(Protocol):
    def execute_query(self, query: str) -> List[Dict[str, Any]]: ...
    def create_table(self, name: str, schema: MockStructType, data: List[Dict]): ...
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

## Backend Factory

In **v4**, Sparkless re-exports Robin (robin-sparkless) as the execution engine. The `BackendFactory` provides only storage backend creation for code that needs file-based catalog/table storage.

```python
# v4: Only storage is supported; materializer and export backends were removed.
storage = BackendFactory.create_storage_backend("robin")
# BackendFactory.create_materializer and create_export_backend no longer exist.
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

# v4: Robin is the only supported backend.
custom_storage = BackendFactory.create_storage_backend("robin")
# Sparkless.sql session uses Robin; custom_storage is for catalog/table use only.
spark = SparkSession.builder.appName("MyApp").getOrCreate()
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

# v4: Robin is the only backend; session uses Robin by default.
spark = SparkSession.builder.appName("MyApp").getOrCreate()
```

### Configuration Keys

| Key | Description | Default | Example |
|-----|-------------|---------|---------|
| `spark.sparkless.backend` | Backend type (v4: Robin only) | `"robin"` | `"robin"` |
| `spark.sparkless.backend.maxMemory` | Memory limit | `"1GB"` | `"4GB"`, `"8GB"` |
| `spark.sparkless.backend.allowDiskSpillover` | Allow disk usage | `false` | `true`, `false` |
| `spark.sparkless.backend.basePath` | Base path for file backend | `"sparkless_storage"` | `"/tmp/data"` |

### Backend Type Detection

The system automatically detects backend types from storage instances:

```python
from sparkless.backend.factory import BackendFactory

# v4: Only Robin storage is supported.
storage = BackendFactory.create_storage_backend("robin")

# Detect the backend type
backend_type = BackendFactory.get_backend_type(storage)
print(backend_type)  # "robin"

# List available backends
available = BackendFactory.list_available_backends()
print(available)  # ["robin"]
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

## Backward Compatibility (v4)

In v4, use the Robin-backed session and factory for storage only:

```python
# Recommended: use sparkless.sql for session and DataFrames
from sparkless.sql import SparkSession, DataFrame, F
spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.createDataFrame([{"a": 1}])

# Storage (for catalog/table use only)
from sparkless.backend.factory import BackendFactory
storage = BackendFactory.create_storage_backend("robin")
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

The backend architecture centralizes backend logic in `sparkless/backend/` (v4):
- Robin storage: `backend/robin/storage.py` (catalog/table use)
- Memory backend: `storage/backends/memory.py` (if still used by tests/legacy)
- Execution: via re-export from `sparkless.sql` (Robin's DataFrame/session)

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

