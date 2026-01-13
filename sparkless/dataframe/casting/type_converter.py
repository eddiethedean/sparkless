"""Type conversion utilities for DataFrame operations."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Union

from ...spark_types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    TimestampType,
)


class TypeConverter:
    """Handles type conversion operations for DataFrame."""

    @staticmethod
    def cast_to_type(
        value: Any, target_type: DataType
    ) -> Union[
        str,
        int,
        float,
        bool,
        None,
        date,
        datetime,
        Decimal,
        List[Any],
        Dict[Any, Any],
        Any,
    ]:
        """Cast a value to the specified target type."""
        if value is None:
            return None

        if isinstance(target_type, StringType):
            return str(value)
        elif isinstance(target_type, (IntegerType, LongType)):
            # Handle string to int conversion (e.g., "10.5" -> 10)
            # PySpark converts to float first, then truncates to int
            try:
                if isinstance(value, str):
                    # Try float first, then convert to int (truncates decimal)
                    return int(float(value))
                return int(value)
            except (ValueError, TypeError):
                # Return None for invalid values (PySpark behavior)
                return None
        elif isinstance(target_type, (FloatType, DoubleType)):
            try:
                return float(value)
            except (ValueError, TypeError):
                # Return None for invalid values (PySpark behavior)
                return None
        elif isinstance(target_type, BooleanType):
            return bool(value)
        elif isinstance(target_type, DateType):
            # Handle date conversion
            import datetime as dt_module

            if isinstance(value, dt_module.date) and not isinstance(
                value, dt_module.datetime
            ):
                return value
            if isinstance(value, dt_module.datetime):
                return value.date()
            if isinstance(value, str):
                try:
                    # Try ISO format first
                    return dt_module.date.fromisoformat(
                        value.split("T")[0].split(" ")[0]
                    )
                except (ValueError, AttributeError):
                    try:
                        # Try parsing as datetime and extract date
                        dt = dt_module.datetime.fromisoformat(
                            value.replace("Z", "+00:00").replace(" ", "T")
                        )
                        return dt.date()
                    except (ValueError, AttributeError):
                        return None
            return None
        elif isinstance(target_type, TimestampType):
            # Handle timestamp conversion
            import datetime as dt_module

            if isinstance(value, dt_module.datetime):
                return value
            if isinstance(value, dt_module.date) and not isinstance(
                value, dt_module.datetime
            ):
                return dt_module.datetime.combine(value, dt_module.time())
            if isinstance(value, str):
                try:
                    return dt_module.datetime.fromisoformat(
                        value.replace("Z", "+00:00").replace(" ", "T")
                    )
                except (ValueError, AttributeError):
                    return None
            return None
        elif isinstance(target_type, DecimalType):
            # Handle decimal conversion with precision/scale preservation
            from decimal import Decimal

            # Get scale from DecimalType
            scale = getattr(target_type, "scale", 0)

            # Create Decimal with proper precision/scale
            decimal_value = Decimal(str(value))

            # Adjust precision/scale if needed
            if hasattr(target_type, "precision") and hasattr(target_type, "scale"):
                # Quantize to match the target scale
                from decimal import ROUND_HALF_UP

                quantize_exponent = Decimal(10) ** (-scale)
                decimal_value = decimal_value.quantize(
                    quantize_exponent, rounding=ROUND_HALF_UP
                )

            return decimal_value
        elif isinstance(target_type, ArrayType):
            # Handle array conversion
            if isinstance(value, (list, tuple)):
                return [
                    TypeConverter.cast_to_type(item, target_type.element_type)
                    for item in value
                ]
            return [value]
        elif isinstance(target_type, MapType):
            # Handle map conversion
            if isinstance(value, dict):
                return {
                    TypeConverter.cast_to_type(
                        k, target_type.key_type
                    ): TypeConverter.cast_to_type(v, target_type.value_type)
                    for k, v in value.items()
                }
            return {value: None}
        else:
            # Unknown type - return value as-is (could be any type)
            # Any is already included in Union type, so this is acceptable
            return value

    @staticmethod
    def infer_type(value: Any) -> DataType:
        """Infer the data type of a value."""
        if value is None:
            return StringType()
        elif isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            return LongType()
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, str):
            return StringType()
        elif isinstance(value, (list, tuple)):
            if value:
                element_type = TypeConverter.infer_type(value[0])
                return ArrayType(element_type)
            return ArrayType(StringType())
        elif isinstance(value, dict):
            if value:
                key_type = TypeConverter.infer_type(next(iter(value.keys())))
                value_type = TypeConverter.infer_type(next(iter(value.values())))
                return MapType(key_type, value_type)
            return MapType(StringType(), StringType())
        else:
            return StringType()
