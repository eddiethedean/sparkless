
> **Versioning Note:** The functionality described here now ships as Sparkless `0.0.x`–`0.3.x` in the semver-aligned roadmap. References to “v3.0.0” map directly to the current `0.0.0` baseline release.

## Overview

Sparkless v3.0.0 introduces a **complete migration from DuckDB to Polars** as the default backend. This is a **breaking change** that requires attention when upgrading.

## Key Changes

### Backend Migration

- **Default Backend**: Changed from DuckDB to Polars
- **Storage Format**: Tables now persist as Parquet files instead of DuckDB database files
- **Thread Safety**: Polars is thread-safe by design - no more connection locks or threading issues
- **No SQL Required**: Polars uses DataFrame operations instead of SQL generation

### Breaking Changes

1. **Default Backend**: New sessions use Polars by default
2. **Storage Format**: Existing DuckDB databases are not automatically migrated
3. **Configuration**: `max_memory` and `allow_disk_spillover` options are ignored for Polars backend
4. **Dependencies**: DuckDB and SQLAlchemy are no longer required (optional for backward compatibility)

## Migration Steps

### 1. Update Dependencies

```bash
# Install v3.0.0
pip install sparkless>=3.0.0

# Polars is now a required dependency
# DuckDB and SQLAlchemy are optional (only if using DuckDB backend)
```

### 2. Update Code

Most code will work without changes since Polars backend implements the same interfaces:

```python
# Before (v2.x) - works the same in v3.0.0
from sparkless.sql import SparkSession

spark = SparkSession("MyApp")
df = spark.createDataFrame([{"name": "Alice", "age": 25}])
df.show()
```

### 3. Migrate Existing DuckDB Databases (Optional)

If you have existing DuckDB database files, you'll need to migrate them:

```python
# Option 1: Use migration script (if provided)
from sparkless.scripts.migrate_duckdb_to_parquet import migrate_database
migrate_database("old_database.duckdb", "new_storage_path")

# Option 2: Manual migration
# Export tables from DuckDB and recreate with Polars backend
```

### 4. Update Backend Configuration (If Needed)

If you explicitly configured DuckDB backend, you may need to update:

```python
# v2.x
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "duckdb") \
    .config("spark.sparkless.backend.maxMemory", "4GB") \
    .getOrCreate()

# v3.0.0 - Polars is default, no memory config needed
spark = SparkSession("MyApp")  # Uses Polars automatically

# Or explicitly set Polars
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "polars") \
    .getOrCreate()
```

### 5. Use DuckDB Backend (If Needed)

If you need DuckDB for specific features, you can still use it:

```python
# v3.0.0 - Use DuckDB backend explicitly
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "duckdb") \
    .config("spark.sparkless.backend.maxMemory", "4GB") \
    .getOrCreate()
```

**Note**: DuckDB backend requires `duckdb` and `duckdb-engine` packages to be installed.

## What Changed Under the Hood

### Storage

- **Before**: DuckDB database files (`.duckdb`)
- **After**: Parquet files (`{schema}/{table}.parquet`) + JSON schema files (`{table}.schema.json`)

### Query Execution

- **Before**: SQL generation via SQLAlchemy → DuckDB
- **After**: Polars expressions → Polars DataFrame operations

### Threading

- **Before**: Connection locks required (`_connection_lock`)
- **After**: Thread-safe by design (Polars uses Rayon internally)

## Performance Improvements

- **Thread Safety**: No locking overhead
- **Lazy Evaluation**: Polars lazy evaluation provides better optimization
- **Memory Efficiency**: Polars handles memory more efficiently
- **Faster Operations**: Polars is optimized for DataFrame operations

## Removed Features

- `max_memory` configuration option (Polars handles memory automatically)
- `allow_disk_spillover` configuration option (not needed with Polars)
- SQL-specific optimizations (replaced with Polars optimizations)

## Backward Compatibility

- **DuckDB Backend**: Still available as optional backend
- **API Compatibility**: All PySpark API methods remain the same
- **Storage Interface**: Same `IStorageManager` protocol

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError: No module named 'polars'`:

```bash
pip install polars>=0.20.0
```

### Threading Issues

If you had threading issues with v2.x, they should be resolved in v3.0.0 with Polars.

### Storage Migration

If you need to migrate existing DuckDB databases, contact support or use the migration script (if available).

## Testing

After migration, run your test suite:

```bash
pytest tests/
```

All tests should pass with Polars backend. If you encounter issues, you can temporarily use DuckDB backend:

```python
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "duckdb") \
    .getOrCreate()
```

## Questions?

If you encounter issues during migration, please:
1. Check this guide
2. Review the [changelog](../CHANGELOG.md)
3. Open an issue on GitHub

## Summary

- ✅ **Default backend**: Polars (thread-safe, high-performance)
- ✅ **Storage format**: Parquet files
- ✅ **Breaking changes**: Default backend, storage format
- ✅ **Backward compatibility**: DuckDB backend still available
- ✅ **API compatibility**: All PySpark APIs remain the same

