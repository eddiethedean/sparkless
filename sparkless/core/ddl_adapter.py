"""
Adapter to convert DDL schema strings to StructType.

This module parses DDL strings (e.g., "id long, name string") and converts
them to Sparkless's internal type system using pure Python parsing.
"""

from __future__ import annotations

import re
from typing import List

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
    """
    ddl_string = ddl_string.strip()
    if not ddl_string:
        return StructType([])

    fields = _parse_fields(ddl_string)
    return StructType(fields)


def _parse_fields(ddl: str) -> List[StructField]:
    """Parse comma-separated field definitions, respecting nested types."""
    fields = []
    depth = 0
    current = []

    for char in ddl:
        if char in ("<", "("):
            depth += 1
            current.append(char)
        elif char in (">", ")"):
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            field_str = "".join(current).strip()
            if field_str:
                fields.append(_parse_single_field(field_str))
            current = []
        else:
            current.append(char)

    field_str = "".join(current).strip()
    if field_str:
        fields.append(_parse_single_field(field_str))

    return fields


def _parse_single_field(field_str: str) -> StructField:
    """Parse a single field definition like 'name string' or 'id long NOT NULL'."""
    field_str = field_str.strip().strip("`")

    # Check for NOT NULL
    nullable = True
    if field_str.upper().endswith("NOT NULL"):
        nullable = False
        field_str = field_str[:-8].strip()

    # Split into name and type
    parts = field_str.split(None, 1)
    if len(parts) == 1:
        return StructField(
            name=parts[0].strip("`"), dataType=StringType(), nullable=nullable
        )

    name = parts[0].strip("`")
    type_str = parts[1].strip()

    data_type = _parse_type(type_str)
    return StructField(name=name, dataType=data_type, nullable=nullable)


def _parse_type(type_str: str) -> DataType:
    """Parse a type string into a DataType."""
    type_str = type_str.strip()
    type_upper = type_str.upper()

    # Decimal with precision/scale
    decimal_match = re.match(r"DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", type_upper)
    if decimal_match:
        return DecimalType(int(decimal_match.group(1)), int(decimal_match.group(2)))

    # Array type
    array_match = re.match(r"ARRAY\s*<\s*(.+)\s*>", type_str, re.IGNORECASE)
    if array_match:
        return ArrayType(_parse_type(array_match.group(1)))

    # Map type
    map_match = re.match(r"MAP\s*<\s*(.+)\s*>", type_str, re.IGNORECASE)
    if map_match:
        inner = map_match.group(1)
        # Split on first comma at depth 0
        depth = 0
        for i, c in enumerate(inner):
            if c in ("<", "("):
                depth += 1
            elif c in (">", ")"):
                depth -= 1
            elif c == "," and depth == 0:
                key_type = _parse_type(inner[:i])
                value_type = _parse_type(inner[i + 1 :])
                return MapType(key_type, value_type)
        return MapType(StringType(), StringType())

    # Struct type
    struct_match = re.match(r"STRUCT\s*<\s*(.+)\s*>", type_str, re.IGNORECASE)
    if struct_match:
        fields = _parse_fields(struct_match.group(1))
        return StructType(fields)

    # Simple types
    return _simple_type(type_str)


def _simple_type(type_name: str) -> DataType:
    type_mapping = {
        "string": StringType,
        "varchar": StringType,
        "char": StringType,
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
        "smallint": ShortType,
        "byte": ByteType,
        "tinyint": ByteType,
        "decimal": DecimalType,
    }
    type_class = type_mapping.get((type_name or "string").lower().strip(), StringType)
    result: DataType = type_class()
    return result
