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
    # The native extension is built as ``sparkless._robin`` via maturin.
    from sparkless import _robin as _native  # type: ignore[attr-defined]
except Exception as exc:  # pragma: no cover - import error path
    _native = None  # type: ignore[assignment]
    _import_error = exc
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


def execute_plan_via_robin(
    data: Sequence[Dict[str, Any]],
    schema: Sequence[Dict[str, str]],
    logical_plan: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Execute a logical plan using the Robin Rust crate via the PyO3 extension.

    Args:
        data: Eager input rows as sequence of dicts.
        schema: Sequence of ``{\"name\": str, \"type\": str}`` entries.
        logical_plan: Logical plan produced by ``sparkless.dataframe.logical_plan.to_logical_plan``.

    Returns:
        List of dict rows representing the result.
    """
    _ensure_native_loaded()
    plan_json = json.dumps(list(logical_plan))
    # The native function returns a list of dicts; we defensively cast to list.
    result = _native._execute_plan(list(data), list(schema), plan_json)  # type: ignore[attr-defined]
    return list(result)


def execute_sql_via_robin(query: str) -> List[Dict[str, Any]]:
    """
    Execute a SQL query using the Robin Rust crate via the PyO3 extension.

    Returns list[dict] rows.
    """
    _ensure_native_loaded()
    result = _native.sql(query)  # type: ignore[attr-defined]
    return list(result)


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

