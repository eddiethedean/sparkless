# Configuration


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

## Engine (v4)

Sparkless v4 has a **single execution engine** (Robin). There is no backend selection or `spark.sparkless.backend` configuration.

```python
# v4: create session (Robin engine)
spark = SparkSession.builder.appName("MyApp").getOrCreate()
```

### Optional session options

- **Logical plan** – `spark.conf.set("spark.sparkless.useLogicalPlan", "true")` (or builder config) to use the serialized logical plan path for execution (v4 uses this by default with the Robin engine).
- **Case sensitivity** – `spark.sql.caseSensitive` (see Case Sensitivity Configuration above).

## Performance knobs

- **Lazy evaluation** – Execution is deferred until an action (`collect`, `show`, `count`, etc.).
- **Profiling** – Optional profiling utilities are documented in [Profiling](../performance/profiling.md).
