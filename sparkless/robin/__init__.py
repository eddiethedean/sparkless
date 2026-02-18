"""
Robin (robin-sparkless) execution integration.

Uses the robin-sparkless Rust crate via the PyO3 extension (sparkless._robin).
No Python robin-sparkless package dependency.
"""

from __future__ import annotations

from .execution import execute_via_robin
from .native import (
    execute_plan_via_robin,
    execute_sql_via_robin,
    get_table_via_robin,
    parse_ddl_schema_via_robin,
    read_csv_via_robin,
    read_delta_via_robin,
    read_delta_version_via_robin,
    read_json_via_robin,
    read_parquet_via_robin,
    register_global_temp_view_via_robin,
    register_temp_view_via_robin,
    save_as_table_via_robin,
    write_csv_via_robin,
    write_delta_via_robin,
    write_json_via_robin,
    write_parquet_via_robin,
)

__all__ = [
    "execute_plan_via_robin",
    "execute_via_robin",
    "execute_sql_via_robin",
    "get_table_via_robin",
    "parse_ddl_schema_via_robin",
    "read_csv_via_robin",
    "read_delta_via_robin",
    "read_delta_version_via_robin",
    "read_json_via_robin",
    "read_parquet_via_robin",
    "register_global_temp_view_via_robin",
    "register_temp_view_via_robin",
    "save_as_table_via_robin",
    "write_csv_via_robin",
    "write_delta_via_robin",
    "write_json_via_robin",
    "write_parquet_via_robin",
]
