"""
Adapter to convert DDL schema (from Robin's native parser) to StructType.

This module parses DDL via the Rust spark-ddl-parser (robin-sparkless) and
converts the result to Sparkless's internal type system.
"""

from __future__ import annotations

from typing import Any, Dict, List

from ..spark_types import (
    ArrayType,
    BooleanType,
    BinaryType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def parse_ddl_schema(ddl_string: str) -> StructType:
    """Parse DDL and convert to StructType.

    Args:
        ddl_string: DDL schema string (e.g., "id long, name string")

    Returns:
        StructType with parsed fields

    Raises:
        ValueError: If DDL string is invalid
        RuntimeError: If the native extension is not available
    """
    from sparkless.robin.native import parse_ddl_schema_via_robin

    raw = parse_ddl_schema_via_robin(ddl_string)
    fields = [_field_from_dict(f) for f in raw]
    return StructType(fields)


def _field_from_dict(field: Dict[str, Any]) -> StructField:
    name = field["name"]
    data_type = _data_type_from_dict(field["data_type"])
    return StructField(name=name, dataType=data_type, nullable=True)


def _data_type_from_dict(d: Dict[str, Any]) -> DataType:
    kind = d.get("type")
    if kind == "simple":
        return _simple_type(d.get("type_name", "string"))
    if kind == "decimal":
        return DecimalType(
            precision=int(d.get("precision", 10)),
            scale=int(d.get("scale", 0)),
        )
    if kind == "array":
        return ArrayType(_data_type_from_dict(d["element_type"]))
    if kind == "map":
        return MapType(
            _data_type_from_dict(d["key_type"]),
            _data_type_from_dict(d["value_type"]),
        )
    if kind == "struct":
        fields_list: List[Dict[str, Any]] = d.get("fields", [])
        struct_fields = [_field_from_dict(f) for f in fields_list]
        return StructType(struct_fields)
    return StringType()


def _simple_type(type_name: str) -> DataType:
    type_mapping = {
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
        "binary": BinaryType,
        "short": ShortType,
        "byte": ByteType,
    }
    type_class = type_mapping.get((type_name or "string").lower(), StringType)
    return type_class()
