"""
Schema Inference Engine

Provides automatic schema inference from Python data structures,
matching PySpark 3.2.4 behavior exactly.

Key behaviors:
- int → LongType (not IntegerType)
- float → DoubleType (not FloatType)
- Columns sorted alphabetically
- All inferred fields are nullable=True
- Raises ValueError for all-null columns
- Raises TypeError for type conflicts
- Supports sparse data (different keys per row)
"""

from typing import Any, Dict, List, Set, Tuple, Union

from ..spark_types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    FloatType,
    StringType,
    BooleanType,
    ArrayType,
    MapType,
    BinaryType,
    TimestampType,
    DateType,
    NullType,
)


class SchemaInferenceEngine:
    """Engine for inferring schemas from Python data structures."""

    @staticmethod
    def infer_from_data(
        data: List[Dict[str, Any]],
    ) -> Tuple[StructType, List[Dict[str, Any]]]:
        """
        Infer schema from a list of dictionaries.

        Matches PySpark behavior:
        - Scans all rows to collect all unique keys
        - Infers type from non-null values
        - Raises ValueError if all values for a column are null
        - Raises TypeError if type conflicts exist
        - Sorts columns alphabetically
        - Sets all fields as nullable=True
        - Fills missing keys with None

        Args:
            data: List of dictionaries representing rows

        Returns:
            Tuple of (inferred_schema, normalized_data)
            - inferred_schema: StructType with inferred fields
            - normalized_data: Data with all keys present, alphabetically ordered

        Raises:
            ValueError: If any column has all null values
            TypeError: If type conflicts exist across rows
        """
        if not data:
            return StructType([]), []

        # Collect all unique keys from all rows (sparse data support)
        all_keys: Set[str] = set()
        for row in data:
            if isinstance(row, dict):
                all_keys.update(row.keys())

        # Sort keys alphabetically (PySpark behavior)
        sorted_keys = sorted(all_keys)

        # Infer type for each key
        fields = []
        for key in sorted_keys:
            # Collect all non-null values for this key
            values_for_key = []
            for row in data:
                if isinstance(row, dict) and key in row and row[key] is not None:
                    values_for_key.append(row[key])

            # Check if all values are null (PySpark raises ValueError)
            if not values_for_key:
                raise ValueError("Some of types cannot be determined after inferring")

            # Infer type from first non-null value
            field_type = SchemaInferenceEngine._infer_type(values_for_key[0])

            # Check for type conflicts across rows
            # Note: PySpark does NOT promote types in createDataFrame - it raises TypeError
            # Type promotion (int+float -> DoubleType) only happens in CSV reading with inferSchema=True
            for value in values_for_key[1:]:
                inferred_type = SchemaInferenceEngine._infer_type(value)
                if type(field_type) is not type(inferred_type):
                    # PySpark raises TypeError for all type conflicts
                    raise TypeError(
                        f"field {key}: Can not merge type {type(field_type).__name__} "
                        f"and {type(inferred_type).__name__}"
                    )

            # Use the nullable property from the field type if available, otherwise default to True
            nullable = getattr(field_type, "nullable", True)
            fields.append(StructField(key, field_type, nullable=nullable))

        schema = StructType(fields)

        # Normalize data: fill missing keys with None and reorder alphabetically
        normalized_data = []
        for row in data:
            if isinstance(row, dict):
                # Create row with all keys, using None for missing ones
                normalized_row = {key: row.get(key, None) for key in sorted_keys}
                normalized_data.append(normalized_row)
            else:
                normalized_data.append(row)  # type: ignore[unreachable]

        return schema, normalized_data

    @staticmethod
    def _infer_type(value: Any) -> Any:
        """
        Infer Sparkless data type from a Python value.

        Type mapping (matching PySpark):
        - None → NullType
        - bool → BooleanType
        - int → LongType (NOT IntegerType!)
        - float → DoubleType (NOT FloatType!)
        - str → StringType
        - list → ArrayType (with element type inferred)
        - dict → MapType (string keys and values)

        Args:
            value: Python value to infer type from

        Returns:
            Sparkless data type
        """
        # Handle None values first (Issue #1 fix)
        if value is None:
            return NullType()
        # Check bool BEFORE int (bool is subclass of int in Python)
        if isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            return LongType()  # PySpark uses Long for all Python ints
        elif isinstance(value, float):
            return DoubleType()  # PySpark uses Double for all Python floats
        elif isinstance(value, bytes):
            return BinaryType()
        elif isinstance(value, list):
            # ArrayType - infer element type from first non-null element
            element_type = StringType()  # Default
            for item in value:
                if item is not None:
                    element_type = SchemaInferenceEngine._infer_type(item)
                    break
            return ArrayType(element_type)
        elif isinstance(value, dict):
            # MapType - PySpark infers dicts as MapType (not StructType!)
            # Assume string keys and string values for simplicity
            return MapType(StringType(), StringType())
        elif isinstance(value, str):
            # PySpark treats all strings as StringType, regardless of content
            # It does NOT infer DateType or TimestampType from string patterns
            # Users must explicitly cast strings to date/timestamp types
            return StringType()
        else:
            # Check for date/datetime objects
            import datetime as dt_module

            if isinstance(value, dt_module.date) and not isinstance(
                value, dt_module.datetime
            ):
                # Pure date object (not datetime)
                return DateType()
            elif isinstance(value, dt_module.datetime):
                # datetime object
                return TimestampType()
            elif hasattr(value, "date") and hasattr(value, "time"):
                # Other datetime-like objects
                return TimestampType()
            return StringType()  # Default fallback

    @staticmethod
    def _promote_types(type1: Any, type2: Any) -> Union[Any, None]:
        """Promote types to a common type if possible (matching PySpark behavior).

        PySpark promotes:
        - int + float → DoubleType
        - int + double → DoubleType
        - float + double → DoubleType

        Args:
            type1: First type
            type2: Second type

        Returns:
            Promoted type if promotion is possible, None otherwise
        """
        from ..spark_types import LongType, DoubleType

        # Check if both are numeric types that can be promoted
        numeric_types = {LongType, DoubleType, FloatType}
        type1_class = type(type1)
        type2_class = type(type2)

        if type1_class in numeric_types and type2_class in numeric_types:
            # If either is DoubleType, promote to DoubleType
            if type1_class is DoubleType or type2_class is DoubleType:
                return DoubleType()
            # If either is FloatType, promote to FloatType (but PySpark uses DoubleType)
            # Actually, PySpark promotes int+float to DoubleType, not FloatType
            if type1_class is FloatType or type2_class is FloatType:
                return DoubleType()
            # LongType + LongType stays LongType
            if type1_class is LongType and type2_class is LongType:
                return LongType()

        return None

    @staticmethod
    def _is_date_string(value: str) -> bool:
        """Check if string looks like a date."""
        import re

        # Common date patterns
        date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",  # YYYY-MM-DD
            r"^\d{2}/\d{2}/\d{4}$",  # MM/DD/YYYY
            r"^\d{2}-\d{2}-\d{4}$",  # MM-DD-YYYY
            r"^\d{4}/\d{2}/\d{2}$",  # YYYY/MM/DD
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)

    @staticmethod
    def _is_timestamp_string(value: str) -> bool:
        """Check if string looks like a timestamp."""
        import re

        # Common timestamp patterns
        timestamp_patterns = [
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",  # YYYY-MM-DD HH:MM:SS
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$",  # YYYY-MM-DDTHH:MM:SS
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$",  # YYYY-MM-DD HH:MM:SS.microseconds
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+$",  # YYYY-MM-DDTHH:MM:SS.microseconds
        ]
        return any(re.match(pattern, value) for pattern in timestamp_patterns)


# Convenience functions for external use
def infer_schema_from_data(data: List[Dict[str, Any]]) -> StructType:
    """
    Infer schema from data (convenience function).

    Args:
        data: List of dictionaries

    Returns:
        Inferred StructType schema
    """
    schema, _ = SchemaInferenceEngine.infer_from_data(data)
    return schema


def normalize_data_for_schema(
    data: List[Dict[str, Any]], schema: StructType
) -> List[Dict[str, Any]]:
    """
    Normalize data to match schema (fill missing keys, reorder).

    Args:
        data: List of dictionaries
        schema: Target schema

    Returns:
        Normalized data with all schema keys present
    """
    if not data or not schema.fields:
        return data

    sorted_keys = [field.name for field in schema.fields]

    normalized_data = []
    for row in data:
        if isinstance(row, dict):
            normalized_row = {key: row.get(key, None) for key in sorted_keys}
            normalized_data.append(normalized_row)
        else:
            normalized_data.append(row)  # type: ignore[unreachable]

    return normalized_data
