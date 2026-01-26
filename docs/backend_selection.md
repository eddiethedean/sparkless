
Sparkless ships with multiple storage backends. Polars is the default engine in
v3.x, replacing the legacy DuckDB implementation used in previous releases. You
can still switch between backends when you need specific behaviour or when
benchmarks favour alternative engines.

## Available Backends

- `polars` (default) – fast, thread-safe, zero-install beyond the Sparkless
  dependency.
- `memory` – compatibility shim that keeps all data in Python collections. Useful
  for extremely small unit tests or debugging expression translation.
- `file` – persists tables under `sparkless_storage/` as JSON/CSV files. Handy
  for sharing fixtures across sessions.
- `duckdb` (optional) – legacy SQL-backed engine. Requires the
  `sparkless.backend.duckdb` modules to be installed (available in the 2.x
  releases) alongside `duckdb`/`duckdb-engine` Python packages.

Call `sparkless.backend.factory.BackendFactory.list_available_backends()` to see
which backends are currently importable in your environment.

## Selecting a Backend

Sparkless resolves the backend in the following order:

1. **Explicit constructor argument** – `SparkSession(backend_type="memory")`
2. **Environment variable** – set `SPARKLESS_BACKEND=memory` before starting
   the process. This is convenient for CI toggles.
3. **Builder configuration** – `SparkSession.builder.config("spark.sparkless.backend", "file")`
4. **Default** – falls back to `polars` when no overrides are provided.

Examples:

```
import os
from sparkless import SparkSession

# Environment variable
os.environ["SPARKLESS_BACKEND"] = "memory"
spark = SparkSession()

# Builder configuration
spark = (
    SparkSession.builder
    .config("spark.sparkless.backend", "file")
    .getOrCreate()
)

# Direct constructor
spark = SparkSession(backend_type="polars")
```

## Behavioural Notes

- Polars enforces strict schemas and eager type conversion. When migrating from
  DuckDB you may need the datetime helpers described in
  `docs/known_issues.md`.
- The memory backend is intended for developer diagnostics; it is not
  feature-complete and skips several optimisations.
- DuckDB support is best-effort. If the optional modules are missing, selecting
  `duckdb` raises a clear `ValueError` suggesting the required packages.

## Troubleshooting

- **`ValueError: Unsupported backend type`** – verify the spelling and check the
  available list via `BackendFactory.list_available_backends()`.
- **DuckDB import errors** – install Sparkless 2.x (which includes the DuckDB
  modules) or vendor the legacy backend into your project. You also need the
  `duckdb` and `duckdb-engine` pip packages.
- **Permission issues with `file` backend** – adjust the base path by passing
  `SparkSession.builder.config("spark.sparkless.backend.basePath", "/tmp/mock")` and
  ensure the process can read/write there.

