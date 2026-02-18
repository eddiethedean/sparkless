"""
Robin execution bridge for Sparkless.

This module adapts Sparkless logical plans into the LOGICAL_PLAN_FORMAT
expected by the robin-sparkless Rust crate and executes them via the
``sparkless._robin`` PyO3 extension (wrapped in ``sparkless.robin.native``).
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

from sparkless.dataframe.logical_plan import (
    _serialize_data,
    serialize_schema,
    to_logical_plan,
)
from sparkless.spark_types import Row, StructType

from . import native as _native
from .plan_adapter import adapt_plan_for_robin, resolve_plan_columns


def execute_via_robin(
    data: Sequence[Any],
    schema: StructType,
    operations_df: "Any",
) -> List[Row]:
    """
    Execute the lazy operations for a DataFrame using Robin.

    Args:
        data: Underlying eager data for the DataFrame (df.data).
        schema: Current StructType schema for the DataFrame.
        operations_df: The DataFrame instance whose operations queue should
            be converted to a logical plan (typically the same df).

    Returns:
        List[Row]: Materialized rows returned from Robin.
    """
    logical_plan = to_logical_plan(operations_df)
    if data and isinstance(data[0], dict):
        initial_schema_names = list(data[0].keys())
    elif hasattr(schema, "fieldNames"):
        initial_schema_names = list(schema.fieldNames())
    else:
        initial_schema_names = []
    case_sensitive = (
        getattr(operations_df, "_is_case_sensitive", lambda: False)()
    )
    logical_plan = resolve_plan_columns(
        logical_plan, initial_schema_names, case_sensitive
    )
    logical_plan = adapt_plan_for_robin(logical_plan)
    # Robin needs the INPUT schema (describes data), not the output schema.
    # When schema is projected (e.g. select creates [first] from [s]), schema
    # no longer matches data keys. Infer base schema from data when mismatch.
    data_list = list(data)
    schema_names = list(schema.fieldNames()) if hasattr(schema, "fieldNames") else []
    data_keys = list(data_list[0].keys()) if data_list and isinstance(data_list[0], dict) else []
    if set(schema_names) != set(data_keys) and data_keys:
        from sparkless.core.schema_inference import SchemaInferenceEngine

        input_schema, _ = SchemaInferenceEngine.infer_from_data(data_list)
        robin_schema = serialize_schema(input_schema)
    else:
        input_schema = schema
        robin_schema = serialize_schema(schema)
    robin_data: List[Dict[str, Any]] = _serialize_data(data_list, input_schema)

    result_rows: List[Dict[str, Any]] = _native.execute_plan_via_robin(
        robin_data,
        robin_schema,
        logical_plan,
    )

    return [Row(row, schema=schema) for row in result_rows]

