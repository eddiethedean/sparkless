
This guide provides comprehensive documentation for Sparkless's SQL operations, including parsing, validation, optimization, and execution.


## Overview

Sparkless provides a complete SQL processing pipeline that mirrors PySpark's SQL capabilities:

- **SQL Parser** - Converts SQL queries into Abstract Syntax Trees (AST)
- **SQL Validator** - Validates query syntax and semantics
- **SQL Optimizer** - Optimizes query execution plans
- **SQL Executor** - Executes optimized queries against data

## SQL Parser

The `SQLParser` class provides SQL parsing functionality with support for common SQL operations.

### Basic Usage

```python
from sparkless.session.sql import SQLParser

# Create parser instance
parser = SQLParser()

# Parse a simple SELECT query
ast = parser.parse("SELECT name, age FROM users WHERE age > 18")
print(ast.query_type)  # 'SELECT'
print(ast.components['select_columns'])  # ['name', 'age']
print(ast.components['from_tables'])  # ['users']
print(ast.components['where_conditions'])  # ['age > 18']
```

### Supported Query Types

The parser supports the following SQL query types:

#### SELECT Queries
```python
# Basic SELECT
ast = parser.parse("SELECT * FROM employees")

# SELECT with WHERE clause
ast = parser.parse("SELECT name, salary FROM employees WHERE department = 'Engineering'")

# SELECT with GROUP BY
ast = parser.parse("SELECT department, COUNT(*) FROM employees GROUP BY department")

# SELECT with ORDER BY
ast = parser.parse("SELECT name, salary FROM employees ORDER BY salary DESC")

# SELECT with LIMIT
ast = parser.parse("SELECT * FROM employees LIMIT 10")
```

#### DDL Operations
```python
# CREATE TABLE
ast = parser.parse("CREATE TABLE users (id INT, name STRING, age INT)")

# DROP TABLE
ast = parser.parse("DROP TABLE users")

# ALTER TABLE
ast = parser.parse("ALTER TABLE users ADD COLUMN email STRING")
```

#### DML Operations
```python
# INSERT
ast = parser.parse("INSERT INTO users VALUES (1, 'Alice', 25)")

# UPDATE
ast = parser.parse("UPDATE users SET age = 26 WHERE name = 'Alice'")

# DELETE
ast = parser.parse("DELETE FROM users WHERE age < 18")
```

### AST Structure

The `SQLAST` object contains the following components:

```python
class SQLAST:
    def __init__(self, query_type: str, components: Dict[str, Any]):
        self.query_type = query_type  # Type of SQL query
        self.components = components  # Parsed query components

# Available components for SELECT queries:
components = {
    'original_query': str,      # Original SQL query
    'query_type': str,          # Query type (SELECT, INSERT, etc.)
    'tokens': List[str],        # Tokenized query
    'tables': List[str],        # Referenced tables
    'columns': List[str],       # Referenced columns
    'conditions': List[str],    # WHERE conditions
    'joins': List[Dict],        # JOIN information
    'group_by': List[str],      # GROUP BY columns
    'order_by': List[str],      # ORDER BY columns
    'limit': Optional[int],     # LIMIT value
    'offset': Optional[int],    # OFFSET value
    'select_columns': List[str], # SELECT columns
    'from_tables': List[str],   # FROM tables
    'where_conditions': List[str], # WHERE conditions
    'group_by_columns': List[str], # GROUP BY columns
    'having_conditions': List[str], # HAVING conditions
    'order_by_columns': List[str], # ORDER BY columns
    'limit_value': Optional[int]   # LIMIT value
}
```

### Error Handling

The parser raises `ParseException` for invalid SQL:

```python
from sparkless.core.exceptions.analysis import ParseException

try:
    ast = parser.parse("INVALID SQL SYNTAX")
except ParseException as e:
    print(f"Parse error: {e}")
```

## SQL Validator

The `SQLValidator` class validates parsed SQL queries for syntax and semantic correctness.

### Basic Usage

```python
from sparkless.session.sql import SQLValidator

validator = SQLValidator()

# Validate a parsed AST
ast = parser.parse("SELECT name FROM users WHERE age > 18")
is_valid = validator.validate(ast)
print(is_valid)  # True or False

# Get validation errors
errors = validator.get_errors()
for error in errors:
    print(f"Validation error: {error}")
```

### Validation Rules

The validator checks for:

- **Syntax correctness** - Valid SQL syntax
- **Table existence** - Referenced tables exist
- **Column existence** - Referenced columns exist in tables
- **Type compatibility** - Column types are compatible
- **Join validity** - JOIN conditions are valid
- **Aggregate usage** - Aggregates used correctly with GROUP BY

### Custom Validation

```python
# Add custom validation rules
validator.add_rule("custom_rule", lambda ast: check_custom_condition(ast))

# Validate with custom rules
is_valid = validator.validate(ast, custom_rules=True)
```

## SQL Optimizer

The `SQLQueryOptimizer` class optimizes SQL queries for better performance.

### Basic Usage

```python
from sparkless.session.sql import SQLQueryOptimizer

optimizer = SQLQueryOptimizer()

# Optimize a parsed AST
ast = parser.parse("SELECT * FROM users WHERE age > 18 ORDER BY name")
optimized_ast = optimizer.optimize(ast)
print(optimized_ast.components)
```

### Optimization Strategies

The optimizer applies various optimization strategies:

- **Predicate pushdown** - Move WHERE conditions closer to data source
- **Column pruning** - Remove unused columns
- **Join reordering** - Optimize JOIN order
- **Limit pushdown** - Move LIMIT closer to data source
- **Constant folding** - Evaluate constant expressions

### Custom Optimization

```python
# Add custom optimization rules
optimizer.add_rule("custom_optimization", custom_optimization_function)

# Optimize with custom rules
optimized_ast = optimizer.optimize(ast, custom_rules=True)
```

## SQL Executor

The `SQLExecutor` class executes optimized SQL queries against data.

### Basic Usage

```python
from sparkless.session.sql import SQLExecutor

executor = SQLExecutor(spark_session)

# Execute a parsed and optimized AST
result = executor.execute(optimized_ast)
print(result.collect())
```

### Execution Context

The executor maintains execution context including:

- **Session state** - Current Spark session
- **Table registry** - Available tables
- **Schema registry** - Table schemas
- **Configuration** - Execution configuration

### Custom Execution

```python
# Add custom execution handlers
executor.add_handler("custom_operation", custom_handler)

# Execute with custom handlers
result = executor.execute(ast, custom_handlers=True)
```

## Complete SQL Pipeline Example

Here's a complete example of using the SQL pipeline:

```python
from sparkless.sql import SparkSession
from sparkless.session.sql import SQLParser, SQLValidator, SQLQueryOptimizer, SQLExecutor

# Create Spark session
spark = SparkSession("SQLExample")

# Create sample data
data = [
    {"name": "Alice", "age": 25, "department": "Engineering"},
    {"name": "Bob", "age": 30, "department": "Marketing"},
    {"name": "Charlie", "age": 35, "department": "Engineering"}
]
df = spark.createDataFrame(data)
df.createOrReplaceTempView("employees")

# SQL Pipeline
parser = SQLParser()
validator = SQLValidator()
optimizer = SQLQueryOptimizer()
executor = SQLExecutor(spark)

# Parse SQL query
query = "SELECT department, COUNT(*) as count FROM employees WHERE age > 25 GROUP BY department ORDER BY count DESC"
ast = parser.parse(query)

# Validate query
if validator.validate(ast):
    # Optimize query
    optimized_ast = optimizer.optimize(ast)
    
    # Execute query
    result = executor.execute(optimized_ast)
    
    # Display results
    result.show()
else:
    print("Query validation failed:")
    for error in validator.get_errors():
        print(f"  - {error}")
```

## Advanced Features

### Custom SQL Functions

```python
# Register custom SQL function
def custom_function(value):
    return value.upper()

spark.udf.register("custom_upper", custom_function)

# Use in SQL
result = spark.sql("SELECT custom_upper(name) FROM employees")
```

### Query Hints

```python
# Add query hints for optimization
query = """
SELECT /*+ BROADCAST(employees) */ 
    e.name, d.department_name 
FROM employees e 
JOIN departments d ON e.department_id = d.id
"""
ast = parser.parse(query)
```

### Performance Monitoring

```python
# Enable query performance monitoring
executor.enable_profiling()

# Execute query with profiling
result = executor.execute(ast)

# Get execution statistics
stats = executor.get_execution_stats()
print(f"Execution time: {stats['execution_time']}ms")
print(f"Rows processed: {stats['rows_processed']}")
```

## Best Practices

1. **Always validate queries** before execution
2. **Use parameterized queries** to prevent SQL injection
3. **Optimize queries** for better performance
4. **Monitor execution** for performance bottlenecks
5. **Handle errors gracefully** with proper exception handling

## Troubleshooting

### Common Issues

1. **Parse errors** - Check SQL syntax
2. **Validation errors** - Verify table/column existence
3. **Execution errors** - Check data types and constraints
4. **Performance issues** - Use query optimization

### Debug Mode

```python
# Enable debug mode for detailed logging
parser.set_debug(True)
validator.set_debug(True)
optimizer.set_debug(True)
executor.set_debug(True)
```

This comprehensive SQL operations guide provides everything you need to work with Sparkless's SQL capabilities effectively.
