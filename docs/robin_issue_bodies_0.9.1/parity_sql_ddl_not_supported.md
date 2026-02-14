## Summary

[Parity] SQL engine should support DDL such as CREATE SCHEMA and CREATE DATABASE, or expose a clear API for schema/database creation (PySpark allows these via spark.sql()).

## Expected (PySpark)

- `spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")` succeeds.
- `spark.sql("CREATE DATABASE IF NOT EXISTS show_db")` succeeds.

## Actual (Robin 0.9.1)

- `RuntimeError: SQL: only SELECT is supported, got CreateSchema { schema_name: ... }`
- `RuntimeError: SQL: only SELECT is supported, got CreateDatabase { db_name: ... }`
- Hint: "use SELECT ... FROM ..."

## Repro

Run from sparkless repo root:

```bash
python scripts/robin_parity_repros_0.9.1/sql_ddl_not_supported.py
```

### Captured output (2026-02-13)

```
sql_ddl_not_supported: CREATE SCHEMA / CREATE DATABASE
Robin: FAILED ['CREATE SCHEMA: RuntimeError: SQL: only SELECT is supported, got CreateSchema ...', 'CREATE DATABASE: RuntimeError: SQL: only SELECT is supported, got CreateDatabase ...']
PySpark: OK ['CREATE SCHEMA succeeded', 'CREATE DATABASE succeeded']
```

## Affected Sparkless tests

- Many parity tests that create schema/database for isolation (e.g. test_show_tables_in_database, test_parquet_format_table_append, test_delta_lake_schema_evolution).
