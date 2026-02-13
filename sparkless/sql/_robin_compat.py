"""
Minimal compatibility layer for using Robin (robin-sparkless) as the Sparkless engine.

Provides conversion from PySpark-style createDataFrame(data, schema=None) to
Robin's _create_dataframe_from_rows(data, schema) where schema is list of (name, dtype_str).
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple, Union

from sparkless.spark_types import StructType, StructField
from sparkless.core.schema_inference import SchemaInferenceEngine

# Map Sparkless/PySpark DataType.typeName() to Robin dtype strings.
_SPARK_TYPE_TO_ROBIN_DTYPE: dict[str, str] = {
    "string": "string",
    "int": "int",
    "long": "bigint",
    "bigint": "bigint",
    "double": "double",
    "float": "float",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_ntz": "timestamp",
    "datetime": "timestamp",
    "str": "string",
    "varchar": "string",
}


def _spark_type_to_robin_dtype(data_type: Any) -> str:
    """Map Sparkless/PySpark DataType to Robin schema dtype string."""
    name = getattr(data_type, "typeName", None)
    if callable(name):
        name = name()
    else:
        name = getattr(data_type, "__class__", None)
        name = getattr(name, "__name__", "string") if name else "string"
    return _SPARK_TYPE_TO_ROBIN_DTYPE.get(name, "string")


def _struct_type_to_robin_schema(schema: StructType) -> List[Tuple[str, str]]:
    """Convert StructType to list of (name, dtype_str) for Robin."""
    if not hasattr(schema, "fields"):
        return []
    return [
        (getattr(f, "name", ""), _spark_type_to_robin_dtype(getattr(f, "dataType", None)))
        for f in schema.fields
    ]


def _row_value_for_robin(val: Any) -> Any:
    """Convert a single cell value for Robin (e.g. datetime -> ISO string)."""
    if val is None or isinstance(val, (int, float, bool, str)):
        return val
    if isinstance(val, dict):
        return {k: _row_value_for_robin(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_row_value_for_robin(v) for v in val]
    if hasattr(val, "isoformat") and callable(getattr(val, "isoformat")):
        return val.isoformat()
    return str(val)


def _data_to_robin_rows(
    data: List[Any],
    names: List[str],
    schema: Optional[Any] = None,
) -> List[dict]:
    """Convert data to list of dicts for Robin _create_dataframe_from_rows."""
    bool_cols: set = set()
    if schema is not None and hasattr(schema, "fields"):
        for f in schema.fields:
            dt = getattr(f, "dataType", None)
            if dt is not None:
                nm = getattr(dt, "__class__", None)
                nm = getattr(nm, "__name__", "") if nm else ""
                if nm == "BooleanType":
                    bool_cols.add(getattr(f, "name", ""))

    def _convert_val(val: Any, col_name: str) -> Any:
        v = _row_value_for_robin(val)
        if col_name in bool_cols and isinstance(v, int) and v in (0, 1):
            return bool(v)
        return v

    rows: List[dict] = []
    for row in data:
        if isinstance(row, dict):
            rows.append({n: _convert_val(row.get(n), n) for n in names})
        elif isinstance(row, (list, tuple)):
            values = list(row) + [None] * (len(names) - len(row))
            rows.append(
                dict(zip(names, (_convert_val(v, n) for v, n in zip(values, names))))
            )
        else:
            rows.append({n: _convert_val(getattr(row, n, None), n) for n in names})
    return rows


def create_dataframe_via_robin(
    robin_session: Any,
    data: Union[List[dict], List[tuple], Any],
    schema: Optional[Union[StructType, List[str], str]] = None,
) -> Any:
    """
    Create a Robin DataFrame from data and optional schema (PySpark-style).

    Uses the session's _create_dataframe_from_rows or create_dataframe_from_rows.
    """
    create_fn = getattr(
        robin_session, "create_dataframe_from_rows", None
    ) or getattr(robin_session, "_create_dataframe_from_rows", None)
    if create_fn is None:
        raise RuntimeError(
            "Robin session has no create_dataframe_from_rows / _create_dataframe_from_rows"
        )

    # Normalize data to list of dicts and get schema
    if hasattr(data, "to_dict") and callable(getattr(data, "to_dict")):
        # Pandas-like
        data = data.to_dict(orient="records")  # type: ignore[union-attr]
    if hasattr(data, "collect"):
        data = [r.asDict() if hasattr(r, "asDict") else dict(r) for r in data.collect()]

    if not isinstance(data, list):
        data = list(data)

    if schema is None:
        if not data:
            raise ValueError("Cannot infer schema from empty data")
        if not all(isinstance(r, dict) for r in data):
            raise ValueError("When schema is None, data must be a list of dicts")
        inferred_schema, normalized_data = SchemaInferenceEngine.infer_from_data(data)
        data = normalized_data
        schema = inferred_schema
        names = inferred_schema.fieldNames()
        robin_schema = _struct_type_to_robin_schema(inferred_schema)
    elif isinstance(schema, (list, tuple)) and schema and all(
        isinstance(x, str) for x in schema
    ):
        names = list(schema)
        robin_schema = [(n, "string") for n in names]
        # Convert list of tuples to list of dicts if needed
        if data and not isinstance(data[0], dict):
            data = [dict(zip(names, row)) for row in data]
        schema = None
    else:
        # StructType
        names = schema.fieldNames()
        robin_schema = _struct_type_to_robin_schema(schema)

    rows = _data_to_robin_rows(data, names, schema)
    return create_fn(rows, robin_schema)
