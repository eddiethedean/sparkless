# Execution architecture (v4)

## Overview

Sparkless v4 uses a **single execution path**: the [robin-sparkless](https://github.com/eddiethedean/robin-sparkless) Rust crate. There is no pluggable backend layer; the Python API builds logical plans and the crate executes them.

## v4 architecture

```
sparkless/
  session/
    core/
      session.py           # Builds session with MemoryStorageManager; no BackendFactory
      builder.py           # SparkSession.builder.appName(...).getOrCreate()
  dataframe/
    lazy.py                # execute_via_robin() only; no materializer abstraction
    logical_plan.py        # to_logical_plan(), serialize_expression(), serialize_schema()
  robin/
    execution.py           # execute_via_robin() → calls native extension
    native.py              # Wraps sparkless._robin (PyO3 module)
  storage/
    backends/
      memory.py            # MemoryStorageManager (catalog + in-memory tables)
  (Rust extension in src/)
    lib.rs                 # PyO3 bindings; depends on robin-sparkless crate
```

**Removed in v4:**

- `sparkless.backend` package (factory, protocols, polars materializer/storage, etc.)
- Backend selection (polars, memory, file, duckdb)
- `BackendFactory`, `backend_type`, `db_path`, `spark.sparkless.backend` config

## Flow

1. **Session** – `SparkSession.builder.appName(...).getOrCreate()` creates a session with `MemoryStorageManager`. No backend type is chosen; the session is always Robin.
2. **DataFrame operations** – Transformations (filter, select, withColumn, etc.) are queued as a logical plan on the DataFrame.
3. **Actions** – On `collect()`, `count()`, etc., `lazy.py` builds a logical plan (JSON) and calls `execute_via_robin(data, schema, plan)`.
4. **Robin** – The PyO3 extension (`sparkless._robin`) passes the plan to the robin-sparkless crate, which executes it and returns rows. Results are converted back to Sparkless `Row` objects.

## References

- [robin_v4_overhaul_plan.md](robin_v4_overhaul_plan.md) – v4 design and migration
- [upstream.md](upstream.md) – Robin-sparkless version and PySpark parity policy
- [robin_parity_from_skipped_tests.md](robin_parity_from_skipped_tests.md) – Parity gaps observed from skipped tests
