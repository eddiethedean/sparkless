# Migration from Sparkless v3 to v4 (Robin-only)

Sparkless v4 uses **Robin (robin-sparkless)** as the only supported execution backend. Polars, DuckDB, memory, and file backends are no longer selectable in the default v4 build.

## Backend changes

- **v3**: You could set `SPARKLESS_BACKEND` or `backend_type` to `polars`, `memory`, `file`, or (optionally) `duckdb` or `robin`. The default was `polars`.
- **v4**: Only the **Robin** backend is supported. The default backend is `robin`. Setting `SPARKLESS_BACKEND` or `backend_type` to any value other than `robin` will raise a `ValueError`.

## What you need

- **robin-sparkless**: Install with `pip install robin-sparkless>=0.5.0`. It is a required dependency of Sparkless v4.

## Configuration

- `SPARKLESS_BACKEND` (environment variable) and `spark.sparkless.backend` (session config) no longer select between multiple backends; only `robin` is valid. Omitting them uses the default (Robin).
- Session constructor: `SparkSession(backend_type="robin")` or `SparkSession()` both use Robin.

## Storage and I/O

- Catalog and table persistence (e.g. `saveAsTable`, `catalog.tableExists`) use a file-based storage implementation; Polars is no longer used for storage.
- DataFrameReader/DataFrameWriter use row-based and (where needed) pandas-based implementations for Parquet/CSV/JSON instead of Polars.

## If you were on Polars or another backend

- Your existing PySpark-style code (e.g. `SparkSession()`, `createDataFrame`, `filter`, `select`, `collect`) continues to work; execution is handled by Robin instead of Polars.
- For behavioural differences between Robin and Polars (e.g. expression semantics, supported functions), see the Robin-sparkless documentation and the v4 roadmap: `docs/sparkless_v4_roadmap.md`.

## Summary

| Item            | v3                    | v4        |
|-----------------|------------------------|-----------|
| Default backend | polars                 | robin     |
| Other backends  | memory, file, duckdb   | not supported |
| Dependency      | polars (default path)  | robin-sparkless>=0.5.0 |
