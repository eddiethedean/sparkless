"""
Schema and data serialization for Robin FFI helpers.

Extracted from logical_plan for writer and storage backends.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List

from ..spark_types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)


def _json_safe_value(value: Any) -> Any:
    """Convert a value to a JSON-serializable form for row data."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return list(value)
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    return value


def serialize_schema(schema: Any) -> List[Dict[str, str]]:
    """Serialize a StructType to a list of {name, type} dicts."""
    if not hasattr(schema, "fields"):
        return []
    return [
        {"name": f.name, "type": f.dataType.simpleString()}
        for f in schema.fields
    ]


def schema_from_robin_list(entries: List[Dict[str, str]]) -> StructType:
    """Convert Robin schema format [{\"name\": str, \"type\": str}] to StructType."""
    from ..spark_types import StringType

    _simple_type: Dict[str, Any] = {
        "string": StringType,
        "integer": IntegerType,
        "int": IntegerType,
        "long": LongType,
        "bigint": LongType,
        "double": DoubleType,
        "float": FloatType,
        "boolean": BooleanType,
        "bool": BooleanType,
        "date": DateType,
        "timestamp": TimestampType,
    }

    def _dt(t: str) -> Any:
        cls = _simple_type.get((t or "string").lower(), StringType)
        return cls() if callable(cls) else cls

    fields = [
        StructField(e["name"], _dt(e.get("type", "string")), nullable=True)
        for e in entries
    ]
    return StructType(fields)


def serialize_data(data: List[Any], schema: Any) -> List[Dict[str, Any]]:
    """Serialize DataFrame data (list of dict/Row) to list of dicts for Robin."""
    if not data:
        return []
    from sparkless.spark_types import get_row_value, row_keys

    field_names = (
        list(schema.fieldNames())
        if schema and hasattr(schema, "fieldNames")
        else []
    )
    out: List[Dict[str, Any]] = []
    for row in data:
        if isinstance(row, dict):
            out.append({k: _json_safe_value(v) for k, v in row.items()})
            continue
        if hasattr(row, "asDict"):
            d = row.asDict()
            out.append({k: _json_safe_value(v) for k, v in d.items()})
            continue
        if field_names:
            out.append(
                {n: _json_safe_value(get_row_value(row, n)) for n in field_names}
            )
            continue
        keys = row_keys(row)
        if keys:
            out.append({k: _json_safe_value(get_row_value(row, k)) for k in keys})
        else:
            out.append({"_value": _json_safe_value(row)})
    return out
