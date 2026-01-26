
## Overview

With Polars backend (v3.0.0+), query optimization is handled automatically by Polars' native lazy evaluation system. Polars builds an optimized execution plan for the entire operation chain, eliminating the need for manual CTE optimization.

**Note**: This guide describes the previous CTE-based optimization used with DuckDB backend. With Polars, optimization is automatic and more efficient.

## Polars Backend (v3.0.0+)

With Polars backend, optimization is automatic:

1. **Lazy Evaluation**: Operations are built into a lazy execution plan
2. **Automatic Optimization**: Polars optimizes the entire plan before execution
3. **No Intermediate Tables**: No need to create temporary tables
4. **Single Materialization**: Only the final result is materialized

### How It Works

```python
from sparkless.sql import SparkSession

spark = SparkSession("MyApp")  # Uses Polars by default
df = spark.createDataFrame(data)

# Polars automatically optimizes this entire chain
result = df.filter(age > 25).select("name", "age").withColumn("bonus", salary * 0.1)

# Materialization happens only when needed (show, collect, etc.)
result.show()  # Polars optimizes and executes the entire plan
```

### Performance Benefits

- **No SQL Generation**: Direct DataFrame operations
- **Automatic Optimization**: Polars optimizes the execution plan
- **Better Performance**: Polars is optimized for DataFrame operations
- **Thread-Safe**: No connection management overhead

## Legacy: DuckDB CTE Optimization (v2.x)

Previously, with DuckDB backend, each DataFrame operation would:
1. Create a new temporary table
2. Execute a SELECT query from the source table
3. Insert results into the new temporary table

The CTE optimization built a single SQL query with chained CTEs instead.

Example SQL generated for `df.filter(age > 25).select("name", "age").withColumn("bonus", salary * 0.1)`:

```sql
WITH cte_0 AS (SELECT * FROM source_table WHERE "age" > 25),
     cte_1 AS (SELECT "name", "age" FROM cte_0),
     cte_2 AS (SELECT "name", "age", "salary" * 0.1 AS "bonus" FROM cte_1)
SELECT * FROM cte_2
```

## Implementation Details

### Architecture

**Note**: CTE optimization is only relevant for DuckDB backend. With Polars (default in v3.0.0+), optimization is automatic.

The legacy implementation was in `sparkless/backend/duckdb/query_executor.py` with these key components:

#### 1. Updated `materialize()` Method

```python
def materialize(self, data, schema, operations):
    """Materializes operations using CTEs with fallback to table-per-operation."""
    # Create initial table with data
    source_table_name = f"temp_table_{self._temp_table_counter}"
    self._create_table_with_data(source_table_name, data)
    
    # Try CTE-based approach first
    try:
        return self._materialize_with_cte(source_table_name, operations)
    except Exception as e:
        # Fallback to old approach for complex operations
        warnings.warn(f"CTE optimization failed, falling back: {e}")
        return self._materialize_with_tables(source_table_name, operations)
```

#### 2. CTE Query Builder

```python
def _build_cte_query(self, source_table_name, operations):
    """Build a single SQL query with CTEs for all operations."""
    cte_definitions = []
    current_cte_name = source_table_name
    
    for i, (op_name, op_val) in enumerate(operations):
        cte_name = f"cte_{i}"
        
        # Build CTE SQL for each operation type
        if op_name == "filter":
            cte_sql = self._build_filter_cte(current_cte_name, op_val, source_table_obj)
        elif op_name == "select":
            cte_sql = self._build_select_cte(current_cte_name, op_val, source_table_obj)
        # ... other operations
        
        cte_definitions.append(f"{cte_name} AS ({cte_sql})")
        current_cte_name = cte_name
    
    # Build final query
    cte_clause = "WITH " + ",\n     ".join(cte_definitions)
    return f"{cte_clause}\nSELECT * FROM {current_cte_name}"
```

#### 3. Operation-Specific CTE Builders

Each operation has a corresponding `_build_*_cte()` method:

- `_build_filter_cte()` - Generates WHERE clause
- `_build_select_cte()` - Generates SELECT with column list
- `_build_with_column_cte()` - Generates SELECT with new/replaced column
- `_build_order_by_cte()` - Generates ORDER BY clause
- `_build_limit_cte()` - Generates LIMIT clause
- `_build_join_cte()` - Generates JOIN clause
- `_build_union_cte()` - Generates UNION ALL clause

### Supported Operations

✅ **Fully Supported:**
- `filter()` - With simple column comparisons
- `select()` - Column selection and aliasing
- `withColumn()` - Adding/replacing columns with expressions
- `orderBy()` - Sorting with ASC/DESC
- `limit()` - Limiting result count
- `join()` - All join types (inner, left, right, outer)
- `union()` - Union of DataFrames

✅ **Advanced Features:**
- Window functions in select/withColumn
- Aggregate functions
- String operations (upper, lower, etc.)
- Arithmetic expressions
- Complex column expressions

⚠️ **Limitations:**
- Function calls in filter conditions (e.g., `F.length(col("name")) > 3`) currently fall back to table-per-operation
- Very complex nested expressions may require fallback

### Backward Compatibility

The implementation includes a fallback mechanism:
- If CTE generation fails for any operation, it automatically falls back to the original table-per-operation approach
- This ensures 100% backward compatibility
- The original `_apply_*` methods are preserved as `_materialize_with_tables()`

## Performance Benefits

### Polars Backend (v3.0.0+)

- **No I/O for Intermediate Results**: Operations are chained in lazy evaluation
- **Automatic Optimization**: Polars optimizes the entire execution plan
- **Memory Efficient**: Only final result is materialized
- **Better Performance**: Polars is optimized for DataFrame operations

### Legacy DuckDB Backend

**I/O Reduction:**
- **Before**: N intermediate table writes for N operations
- **After (CTE)**: 1 table creation + 1 SELECT with CTEs

**Memory Efficiency:**
- **Before**: N temporary tables in memory/disk
- **After**: 1 query execution with DuckDB's optimized execution plan

**Query Optimization:**
- DuckDB can optimize the entire CTE chain at once
- Better query planning and execution strategies
- Reduced overhead from multiple separate queries

### Benchmark Results

**Polars Backend:**
- Operations are chained lazily - no intermediate materialization
- Single materialization at the end
- Automatic optimization by Polars

**Legacy DuckDB (CTE optimization):**
For a typical pipeline with 5 operations:
- **Table-per-operation**: 5 table creations + 5 SELECTs + 5 INSERTs
- **CTE optimization**: 1 table creation + 1 SELECT with CTEs

Example operations: `filter → select → withColumn → orderBy → limit`
- **Old approach**: ~5x the I/O operations
- **New approach**: Single query execution

## Usage

**Polars Backend (v3.0.0+):**
Optimization is **automatic** - no configuration needed:

```python
from sparkless.sql import SparkSession

spark = SparkSession("MyApp")  # Uses Polars by default
# All operations are automatically optimized
```

**Legacy DuckDB Backend:**
The CTE optimization is **automatically applied** when using lazy evaluation:

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("app").getOrCreate()

# Create DataFrame with lazy evaluation enabled
df = spark.createDataFrame(data).withLazy(True)

# Chain operations - they're queued, not executed
result_df = (
    df.filter(F.col("age") > 25)
    .select("name", "age", "salary")
    .withColumn("bonus", F.col("salary") * 0.1)
    .withColumn("total", F.col("salary") + F.col("bonus"))
    .orderBy(F.desc("total"))
    .limit(10)
)

# Materialize with CTE optimization when action is called
results = result_df.collect()  # Single CTE query executed here
```

## Testing

Comprehensive test suite in `tests/unit/test_cte_optimization.py`:

- ✅ Filter + Select + WithColumn chains
- ✅ OrderBy and Limit operations
- ✅ Complex expressions and nested operations
- ✅ Window functions
- ✅ Empty DataFrames
- ✅ Single operations
- ✅ String operations with upper/lower
- ✅ Fallback behavior

All existing tests pass with CTE optimization enabled, ensuring backward compatibility.

## Future Enhancements

Potential improvements for future releases:

1. **Enhanced Filter Support**: Handle function calls in filter conditions without fallback
2. **Aggregate Operations**: Optimize groupBy + aggregate patterns
3. **Complex Joins**: Multi-DataFrame joins with shared CTEs
4. **Query Plan Inspection**: Add explain() method to show CTE structure
5. **Metrics**: Track CTE vs table-per-operation usage statistics

## Technical Notes

### SQL Generation

The CTE builder reuses existing SQL generation logic from the `_apply_*` methods:
- `_expression_to_sql()` - Converts column expressions to SQL
- `_condition_to_sql()` - Converts filter conditions to SQL
- `_window_spec_to_sql()` - Converts window specifications to SQL

### Error Handling

The implementation includes robust error handling:
- Catches exceptions during CTE generation
- Falls back gracefully to table-per-operation
- Warns users when fallback occurs
- Preserves error messages for debugging

### DuckDB Integration

The CTE optimization leverages DuckDB's capabilities:
- Efficient CTE execution
- Query optimization across CTE chain
- Memory-efficient query planning
- Support for complex SQL constructs

## Conclusion

The CTE-based query optimization significantly improves performance for DataFrame operation chains while maintaining 100% backward compatibility through the fallback mechanism. The implementation is transparent to users and automatically applied when lazy evaluation is enabled.

