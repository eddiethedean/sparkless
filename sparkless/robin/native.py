"""
Native Robin integration helpers backed by the ``sparkless._robin`` PyO3 extension.

This module provides a small, centralized boundary for calling into the
Rust-based ``robin-sparkless`` crate. Higher-level execution and catalog
logic should import from here rather than importing the extension directly.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

try:
    # Prefer sparkless._robin (set by sparkless __init__ from sparkless_robin)
    from sparkless import _robin as _native  # type: ignore[attr-defined]
except Exception:
    try:
        # Fallback: extension may be installed as top-level sparkless_robin
        import sparkless_robin as _native  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - import error path
        _native = None  # type: ignore[assignment]
        _import_error = exc
    else:
        _import_error = None
else:
    _import_error = None


def _ensure_native_loaded() -> None:
    """Raise a clear error if the native extension failed to import."""
    if _native is None:
        raise RuntimeError(
            "sparkless._robin native extension is not available. "
            "Ensure Sparkless was built with a Rust toolchain and maturin. "
            f"Original import error: {_import_error!r}"
        )


def execute_sql_via_robin(query: str) -> tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """
    Execute a SQL query using the Robin Rust crate via the PyO3 extension.

    Returns (rows, schema) where schema is list of {"name": str, "type": str}.
    Schema allows creating empty DataFrames when Robin returns no rows.
    """
    _ensure_native_loaded()
    result = _native.sql(query)  # type: ignore[attr-defined]
    rows = list(result[0])
    schema = [dict(s) for s in result[1]]
    return rows, schema


def read_delta_via_robin(path: str) -> List[Dict[str, Any]]:
    """
    Read a Delta table at the given path using the Robin Rust crate.

    Returns list[dict] rows.
    """
    _ensure_native_loaded()
    result = _native.read_delta(path)  # type: ignore[attr-defined]
    return list(result)


def read_delta_version_via_robin(path: str, version: int) -> List[Dict[str, Any]]:
    """
    Read a Delta table at the given path at a specific version (time travel).

    Returns list[dict] rows.
    """
    _ensure_native_loaded()
    result = _native.read_delta_version(path, version)  # type: ignore[attr-defined]
    return list(result)


def write_delta_via_robin(
    data: Sequence[Dict[str, Any]],
    schema: Sequence[Dict[str, str]],
    path: str,
    overwrite: bool,
) -> None:
    """
    Write rows to a Delta table at the given path using the Robin Rust crate.

    overwrite: if True, replace the table; if False, append.
    """
    _ensure_native_loaded()
    _native.write_delta(  # type: ignore[attr-defined]
        list(data), list(schema), path, overwrite
    )


def register_temp_view_via_robin(name: str, data: Sequence[Dict[str, Any]], schema: Sequence[Dict[str, str]]) -> None:
    """Register a temp view in Robin's session catalog (createOrReplaceTempView)."""
    _ensure_native_loaded()
    _native.register_temp_view(name, list(data), list(schema))  # type: ignore[attr-defined]


def register_global_temp_view_via_robin(
    name: str, data: Sequence[Dict[str, Any]], schema: Sequence[Dict[str, str]]
) -> None:
    """Register a global temp view in Robin's catalog (createOrReplaceGlobalTempView)."""
    _ensure_native_loaded()
    _native.register_global_temp_view(name, list(data), list(schema))  # type: ignore[attr-defined]


def get_table_via_robin(name: str) -> tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """
    Get a table or view from Robin's catalog (spark.table(name)).

    Returns:
        (rows, schema) where schema is list of {"name": str, "type": str}.
    """
    _ensure_native_loaded()
    result = _native.get_table(name)  # type: ignore[attr-defined]
    rows = list(result[0])
    schema = [dict(s) for s in result[1]]
    return rows, schema


def save_as_table_via_robin(
    name: str,
    data: Sequence[Dict[str, Any]],
    schema: Sequence[Dict[str, str]],
    mode: str = "error",
) -> None:
    """
    Save data as a table in Robin's catalog (saveAsTable).

    mode: "overwrite" | "append" | "ignore" | "error" (ErrorIfExists).
    """
    _ensure_native_loaded()
    _native.save_as_table(name, list(data), list(schema), mode)  # type: ignore[attr-defined]


def read_parquet_via_robin(path: str) -> tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """
    Read a Parquet file at path using the Robin Rust crate.

    Returns:
        (rows, schema) where schema is list of {"name": str, "type": str}.
    """
    _ensure_native_loaded()
    result = _native.read_parquet(path)  # type: ignore[attr-defined]
    rows = list(result[0])
    schema = [dict(s) for s in result[1]]
    return rows, schema


def read_csv_via_robin(path: str) -> tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """
    Read a CSV file at path using the Robin Rust crate.

    Returns:
        (rows, schema) where schema is list of {"name": str, "type": str}.
    """
    _ensure_native_loaded()
    result = _native.read_csv(path)  # type: ignore[attr-defined]
    rows = list(result[0])
    schema = [dict(s) for s in result[1]]
    return rows, schema


def read_json_via_robin(path: str) -> tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """
    Read a JSON/JSONL file at path using the Robin Rust crate.

    Returns:
        (rows, schema) where schema is list of {"name": str, "type": str}.
    """
    _ensure_native_loaded()
    result = _native.read_json(path)  # type: ignore[attr-defined]
    rows = list(result[0])
    schema = [dict(s) for s in result[1]]
    return rows, schema


def write_parquet_via_robin(
    data: Sequence[Dict[str, Any]],
    schema: Sequence[Dict[str, str]],
    path: str,
    overwrite: bool = True,
) -> None:
    """Write rows to Parquet file at path using the Robin Rust crate."""
    _ensure_native_loaded()
    _native.write_parquet(list(data), list(schema), path, overwrite)  # type: ignore[attr-defined]


def write_csv_via_robin(
    data: Sequence[Dict[str, Any]],
    schema: Sequence[Dict[str, str]],
    path: str,
    overwrite: bool = True,
) -> None:
    """Write rows to CSV file at path using the Robin Rust crate."""
    _ensure_native_loaded()
    _native.write_csv(list(data), list(schema), path, overwrite)  # type: ignore[attr-defined]


def write_json_via_robin(
    data: Sequence[Dict[str, Any]],
    schema: Sequence[Dict[str, str]],
    path: str,
    overwrite: bool = True,
) -> None:
    """Write rows to JSON/JSONL file at path using the Robin Rust crate."""
    _ensure_native_loaded()
    _native.write_json(list(data), list(schema), path, overwrite)  # type: ignore[attr-defined]


def parse_ddl_schema_via_robin(ddl: str) -> List[Dict[str, Any]]:
    """
    Parse a DDL schema string using the Rust spark-ddl-parser (robin-sparkless).

    Returns:
        List of {"name": str, "data_type": dict}. data_type is a nested dict
        with "type" one of "simple", "decimal", "array", "map", "struct", and
        type-specific keys (e.g. type_name, precision, scale, element_type, fields).
    """
    _ensure_native_loaded()
    result = _native.parse_ddl_schema(ddl)  # type: ignore[attr-defined]
    return list(result)

