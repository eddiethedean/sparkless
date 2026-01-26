
Sparkless configuration is managed via SparkSession constructor options and the session builder.

## Basic Configuration

```python
from sparkless.sql import SparkSession

spark = SparkSession(
    validation_mode="relaxed",           # strict | relaxed | minimal
    enable_type_coercion=True,
)
```

Key settings:
- validation_mode: controls strictness of schema/data checks
- enable_type_coercion: attempts to coerce types during DataFrame creation

## Case Sensitivity Configuration

Sparkless supports PySpark-compatible case sensitivity configuration via `spark.sql.caseSensitive`:

```python
# Default: case-insensitive (matches PySpark default)
spark = SparkSession("MyApp")
assert spark.conf.is_case_sensitive() == False

# Enable case-sensitive mode
spark = SparkSession.builder \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

assert spark.conf.is_case_sensitive() == True
```

**Default Behavior (case-insensitive):**
- Column names can be referenced in any case (e.g., `df.select("name")` matches `"Name"` column)
- Output preserves original column names from schema
- Matches PySpark's default behavior

**Case-Sensitive Mode:**
- Column names must match exactly (case-sensitive)
- Useful when working with data sources that have case-sensitive schemas
- Can be enabled via `spark.conf.set("spark.sql.caseSensitive", "true")` or builder config

**Ambiguity Detection:**
- When multiple columns differ only by case (e.g., `["Name", "name"]`) and case-insensitive mode is enabled, resolution raises `AnalysisException` due to ambiguity
- This matches PySpark behavior

Example:
```python
from sparkless.sql import SparkSession

# Case-insensitive (default)
spark = SparkSession("TestApp")
df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])
result = df.select("name").collect()  # Works - resolves to "Name"
assert result[0]["Name"] == "Alice"  # Output uses original case "Name"

# Case-sensitive
spark.conf.set("spark.sql.caseSensitive", "true")
result = df.select("Name").collect()  # Must use exact case
# df.select("name") would raise column not found error
```

## Backend Configuration (v3.0.0+)

### Default Backend (Polars)

```python
# Polars is the default backend in v3.0.0+
spark = SparkSession("MyApp")
```

### Explicit Backend Selection

```python
# Use Polars explicitly
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "polars") \
    .getOrCreate()

# Use DuckDB backend (legacy, requires duckdb package)
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "duckdb") \
    .config("spark.sparkless.backend.maxMemory", "4GB") \
    .config("spark.sparkless.backend.allowDiskSpillover", True) \
    .getOrCreate()

# Use memory backend
spark = SparkSession.builder \
    .config("spark.sparkless.backend", "memory") \
    .getOrCreate()
```

### Backend-Specific Options

**Polars Backend (default):**
- No configuration needed - Polars handles memory and performance automatically
- Thread-safe by design
- Uses Parquet files for persistence

**DuckDB Backend (legacy):**
- `spark.sparkless.backend.maxMemory`: Maximum memory (e.g., "1GB", "4GB")
- `spark.sparkless.backend.allowDiskSpillover`: Allow disk spillover when memory is full

**Note**: `maxMemory` and `allowDiskSpillover` options are ignored for Polars backend.
