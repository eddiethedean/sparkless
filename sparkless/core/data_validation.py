"""
Data Validation and Coercion Engine

Provides validation and type coercion for DataFrame data against schemas.
Supports different validation modes (strict, relaxed, minimal) and optional
type coercion for better usability.
"""

from typing import Any, Dict, List

from ..spark_types import StructType
from .exceptions.validation import IllegalArgumentException


class DataValidator:
    """
    Validates and coerces data to match a given schema.

    Supports multiple validation modes:
    - strict: All fields required, strict type matching
    - relaxed: Missing fields allowed, basic type coercion
    - minimal: No validation, coercion only if enabled
    """

    def __init__(
        self,
        schema: StructType,
        validation_mode: str = "relaxed",
        enable_coercion: bool = True,
    ):
        """
        Initialize DataValidator.

        Args:
            schema: The schema to validate/coerce against
            validation_mode: "strict", "relaxed", or "minimal"
            enable_coercion: Whether to enable type coercion
        """
        self.schema = schema
        self.validation_mode = validation_mode
        self.enable_coercion = enable_coercion
        self._field_types = {
            f.name: f.dataType.__class__.__name__ for f in schema.fields
        }

    def validate(self, data: List[Dict[str, Any]]) -> None:
        """
        Validate data against schema.

        Args:
            data: List of dictionaries representing rows

        Raises:
            IllegalArgumentException: If validation fails in strict mode
        """
        if self.validation_mode != "strict":
            return  # Only validate in strict mode

        for row in data:
            if not isinstance(row, dict):
                raise IllegalArgumentException(
                    "Strict mode requires dict rows after normalization"
                )

            # Ensure all schema fields present
            for field_name in self._field_types:
                if field_name not in row:
                    raise IllegalArgumentException(
                        f"Missing required field '{field_name}' in row"
                    )

            # Type check each field
            for field_name, value in row.items():
                if field_name not in self._field_types:
                    raise IllegalArgumentException(
                        f"Unexpected field '{field_name}' in row"
                    )

                expected_type = self._field_types[field_name]

                if value is None:
                    continue  # Nulls are allowed

                # Validate type matches
                self._validate_value_type(field_name, value, expected_type)

    def _validate_value_type(
        self, field_name: str, value: Any, expected_type: str
    ) -> None:
        """Validate a single value matches expected type."""
        actual_type = type(value).__name__

        # Accept numeric widenings (int -> LongType, float -> DoubleType)
        if expected_type in ("LongType", "IntegerType") and isinstance(value, int):
            return

        if expected_type in ("DoubleType", "FloatType") and isinstance(
            value, (int, float)
        ):
            return

        if expected_type == "StringType" and isinstance(value, str):
            return

        if expected_type == "BooleanType" and isinstance(value, bool):
            return

        # For complex types (ArrayType, MapType, StructType), skip deep validation
        if expected_type in ("ArrayType", "MapType", "StructType"):
            return

        # Type mismatch
        raise IllegalArgumentException(
            f"Type mismatch for field '{field_name}': "
            f"expected {expected_type}, got {actual_type}"
        )

    def coerce(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Coerce data types to match schema (best-effort).

        Args:
            data: List of dictionaries representing rows

        Returns:
            Coerced data with types matching schema where possible
        """
        if not self.enable_coercion:
            return data

        coerced: List[Dict[str, Any]] = []

        for row in data:
            if not isinstance(row, dict):
                # Leave non-dict rows as-is
                coerced.append(row)  # type: ignore[unreachable]
                continue

            new_row: Dict[str, Any] = {}
            for field_name in self._field_types:
                value = row.get(field_name)
                expected_type = self._field_types[field_name]
                new_row[field_name] = self._coerce_value(value, expected_type)

            coerced.append(new_row)

        return coerced

    def _coerce_value(self, value: Any, expected_type_name: str) -> Any:
        """
        Coerce a single value to match expected type.

        Args:
            value: The value to coerce
            expected_type_name: Expected type name (e.g., "LongType")

        Returns:
            Coerced value, or original if coercion fails
        """
        if value is None:
            return None

        try:
            if expected_type_name in ("LongType", "IntegerType"):
                return int(value)

            if expected_type_name in ("DoubleType", "FloatType"):
                return float(value)

            if expected_type_name == "StringType":
                return str(value)

            if expected_type_name == "BooleanType":
                if isinstance(value, bool):
                    return value
                # Simple string coercion
                if str(value).lower() in ("true", "1"):
                    return True
                if str(value).lower() in ("false", "0"):
                    return False
                return bool(value)

        except (ValueError, TypeError):
            # If coercion fails, return original value
            pass

        return value


# Convenience functions
def validate_data(
    data: List[Dict[str, Any]], schema: StructType, mode: str = "strict"
) -> None:
    """
    Validate data against schema (convenience function).

    Args:
        data: List of dictionaries
        schema: Schema to validate against
        mode: Validation mode ("strict", "relaxed", "minimal")

    Raises:
        IllegalArgumentException: If validation fails
    """
    validator = DataValidator(schema, validation_mode=mode, enable_coercion=False)
    validator.validate(data)


def coerce_data(data: List[Dict[str, Any]], schema: StructType) -> List[Dict[str, Any]]:
    """
    Coerce data to match schema (convenience function).

    Args:
        data: List of dictionaries
        schema: Schema to coerce to

    Returns:
        Coerced data
    """
    validator = DataValidator(schema, validation_mode="minimal", enable_coercion=True)
    return validator.coerce(data)
