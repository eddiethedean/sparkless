"""
Condition evaluation utilities for Sparkless.

This module provides shared condition evaluation logic to avoid duplication
between DataFrame and conditional function modules.
"""

from typing import Any, Dict, List, Optional, Tuple, Union, cast
from ..functions.base import Column, ColumnOperation


class ConditionEvaluator:
    """Shared condition evaluation logic."""

    @staticmethod
    def evaluate_expression(row: Dict[str, Any], expression: Any) -> Any:
        """Evaluate an expression (arithmetic, function, etc.) for a given row.

        Args:
            row: The data row to evaluate against.
            expression: The expression to evaluate.

        Returns:
            The evaluated result.
        """
        if isinstance(expression, ColumnOperation):
            return ConditionEvaluator._evaluate_column_operation_value(row, expression)
        elif hasattr(expression, "evaluate"):
            return expression.evaluate(row)
        elif hasattr(expression, "value"):
            return expression.value
        else:
            return expression

    @staticmethod
    def evaluate_condition(row: Dict[str, Any], condition: Any) -> Optional[bool]:
        """Evaluate a condition for a given row.

        Args:
            row: The data row to evaluate against.
            condition: The condition to evaluate.

        Returns:
            True if condition is met, False otherwise.
        """
        # Check ColumnOperation BEFORE Column since ColumnOperation is a subclass of Column
        # This ensures comparison operations (==, !=, etc.) are properly evaluated
        if isinstance(condition, ColumnOperation):
            return ConditionEvaluator._evaluate_column_operation(row, condition)

        if isinstance(condition, Column):
            return row.get(condition.name) is not None

        # For simple values, check if truthy
        return bool(condition) if condition is not None else False

    @staticmethod
    def _evaluate_column_operation_value(
        row: Dict[str, Any], operation: ColumnOperation
    ) -> Optional[Any]:
        """Evaluate a column operation and return the value (not boolean).

        Args:
            row: The data row to evaluate against.
            operation: The column operation to evaluate.

        Returns:
            The evaluated result value.
        """
        operation_type = operation.operation

        # Arithmetic operations
        if operation_type in ["+", "-", "*", "/", "%"]:
            left_value = ConditionEvaluator._get_column_value(row, operation.column)
            right_value = ConditionEvaluator._get_column_value(row, operation.value)

            if left_value is None or right_value is None:
                return None

            try:
                if operation_type == "+":
                    # PySpark compatibility: String concatenation with + operator returns None
                    # when DataFrame is cached. Check if both operands are strings.
                    is_string_concatenation = isinstance(
                        left_value, str
                    ) and isinstance(right_value, str)
                    if is_string_concatenation and row.get(
                        "__dataframe_is_cached__", False
                    ):
                        # Check if we're in a cached DataFrame context
                        return None
                    result: Any = left_value + right_value
                    return cast("Optional[bool]", result)
                elif operation_type == "-":
                    return cast("Optional[bool]", left_value - right_value)
                elif operation_type == "*":
                    return cast("bool", left_value * right_value)
                elif operation_type == "/":
                    if right_value == 0:
                        return None
                    return cast("bool", left_value / right_value)
                elif operation_type == "%":
                    if right_value == 0:
                        return None
                    return cast("bool", left_value % right_value)
            except (TypeError, ValueError):
                return None

        # Cast operations
        elif operation_type == "cast":
            value = ConditionEvaluator._get_column_value(row, operation.column)
            target_type = operation.value

            if value is None:
                return None

            try:
                # Handle DataType objects (Issue #5 fix)
                from sparkless.dataframe.casting.type_converter import TypeConverter
                from sparkless.spark_types import DataType

                if isinstance(target_type, DataType):
                    # Use TypeConverter for proper DataType handling
                    return TypeConverter.cast_to_type(value, target_type)

                # Handle string type names (legacy support)
                if target_type == "long" or target_type == "bigint":
                    # Convert to Unix timestamp if it's a datetime string
                    if isinstance(value, str) and ("-" in value or ":" in value):
                        from datetime import datetime

                        dt = datetime.fromisoformat(value.replace(" ", "T"))
                        return int(dt.timestamp())
                    return int(float(value))
                elif target_type == "int":
                    return int(float(value))
                elif target_type == "double" or target_type == "float":
                    return float(value)
                elif target_type == "string":
                    return str(value)
                elif target_type == "boolean":
                    return bool(value)
                else:
                    return value
            except (TypeError, ValueError):
                return None

        # Function operations
        elif operation_type in [
            "md5",
            "sha1",
            "crc32",
            "upper",
            "lower",
            "length",
            "trim",
            "abs",
            "round",
            "log10",
            "log",
            "log2",
            "concat",
            "split",
            "regexp_replace",
            "coalesce",
            "ceil",
            "floor",
            "sqrt",
            "exp",
            "sin",
            "cos",
            "tan",
            "asin",
            "acos",
            "atan",
            "sinh",
            "cosh",
            "tanh",
            "degrees",
            "radians",
            "sign",
            "greatest",
            "least",
            "when",
            "otherwise",
            "isnull",
            "isnotnull",
            "isnan",
            "nvl",
            "nvl2",
            "current_date",
            "current_timestamp",
            "to_date",
            "to_timestamp",
            "hour",
            "day",
            "dayofmonth",
            "month",
            "year",
            "dayofweek",
            "dayofyear",
            "weekofyear",
            "quarter",
            "minute",
            "second",
            "date_add",
            "date_sub",
            "datediff",
            "months_between",
            "unix_timestamp",
            "from_unixtime",
            "array_distinct",
            "array_sort",
            "sort_array",
            "initcap",
            "concat_ws",
            "pi",
            "e",
        ]:
            return ConditionEvaluator._evaluate_function_operation_value(row, operation)

        # Comparison operations (return boolean)
        elif operation_type in [
            "==",
            "!=",
            "<",
            ">",
            "<=",
            ">=",
            "eq",
            "ne",
            "lt",
            "le",
            "gt",
            "ge",
        ]:
            return ConditionEvaluator._evaluate_comparison_operation(row, operation)

        # Logical operations (return boolean)
        elif operation_type in ["and", "&", "or", "|", "not", "!"]:
            return ConditionEvaluator._evaluate_logical_operation(row, operation)

        # Default fallback
        return None

    @staticmethod
    def _evaluate_function_operation_value(
        row: Dict[str, Any], operation: ColumnOperation
    ) -> Any:
        """Evaluate a function operation and return the value.

        Args:
            row: The data row to evaluate against.
            operation: The function operation to evaluate.

        Returns:
            The evaluated result value.
        """
        operation_type = operation.operation

        # Handle constant functions that don't need column values
        if operation_type == "pi":
            import math

            return math.pi
        elif operation_type == "e":
            import math

            return math.e

        col_value = ConditionEvaluator._get_column_value(row, operation.column)

        # Handle function operations that return values
        if operation_type == "upper":
            return str(col_value).upper() if col_value is not None else None
        elif operation_type == "lower":
            return str(col_value).lower() if col_value is not None else None
        elif operation_type == "length":
            return len(str(col_value)) if col_value is not None else None
        elif operation_type == "trim":
            # PySpark trim only removes ASCII space characters (0x20), not tabs/newlines
            return str(col_value).strip(" ") if col_value is not None else None
        elif operation_type == "ltrim":
            # PySpark ltrim only removes ASCII space characters (0x20), not tabs/newlines
            return str(col_value).lstrip(" ") if col_value is not None else None
        elif operation_type == "rtrim":
            # PySpark rtrim only removes ASCII space characters (0x20), not tabs/newlines
            return str(col_value).rstrip(" ") if col_value is not None else None
        elif operation_type == "initcap":
            # Capitalize first letter of each word
            if col_value is None:
                return None
            return " ".join(word.capitalize() for word in str(col_value).split())
        elif operation_type == "concat_ws":
            # Concatenate with separator - operation.value is (sep, [columns])
            # For concat_ws, we need to get values from multiple columns
            # The operation.value should be a tuple: (separator, [column1, column2, ...])
            if not hasattr(operation, "value") or not isinstance(
                operation.value, tuple
            ):
                return None
            sep, columns = operation.value
            # Get values for all columns (including the first column from operation.column)
            values = []
            # First column is in operation.column
            first_val = ConditionEvaluator._get_column_value(row, operation.column)
            if first_val is not None:
                values.append(str(first_val))
            # Additional columns are in the tuple
            for col in columns:
                col_val = ConditionEvaluator._get_column_value(row, col)
                if col_val is not None:
                    values.append(str(col_val))
            # Join with separator
            return sep.join(values) if values else None
        elif operation_type == "regexp_replace":
            # Regex replace - operation.value is (pattern, replacement)
            if col_value is None:
                return None
            import re

            if not hasattr(operation, "value") or not isinstance(
                operation.value, tuple
            ):
                return str(col_value)
            pattern, replacement = operation.value
            try:
                return re.sub(pattern, replacement, str(col_value))
            except Exception:
                return str(col_value)
        elif operation_type == "abs":
            return abs(float(col_value)) if col_value is not None else None
        elif operation_type == "round":
            return round(float(col_value)) if col_value is not None else None
        elif operation_type == "ceil":
            import math

            return math.ceil(float(col_value)) if col_value is not None else None
        elif operation_type == "floor":
            import math

            return math.floor(float(col_value)) if col_value is not None else None
        elif operation_type == "sqrt":
            import math

            return (
                math.sqrt(float(col_value))
                if col_value is not None and float(col_value) >= 0
                else None
            )
        elif operation_type == "exp":
            import math

            return math.exp(float(col_value)) if col_value is not None else None
        elif operation_type == "sin":
            import math

            return math.sin(float(col_value)) if col_value is not None else None
        elif operation_type == "cos":
            import math

            return math.cos(float(col_value)) if col_value is not None else None
        elif operation_type == "tan":
            import math

            return math.tan(float(col_value)) if col_value is not None else None
        elif operation_type == "asin":
            import math

            return math.asin(float(col_value)) if col_value is not None else None
        elif operation_type == "acos":
            import math

            return math.acos(float(col_value)) if col_value is not None else None
        elif operation_type == "atan":
            import math

            return math.atan(float(col_value)) if col_value is not None else None
        elif operation_type == "sinh":
            import math

            return math.sinh(float(col_value)) if col_value is not None else None
        elif operation_type == "cosh":
            import math

            return math.cosh(float(col_value)) if col_value is not None else None
        elif operation_type == "tanh":
            import math

            return math.tanh(float(col_value)) if col_value is not None else None
        elif operation_type == "degrees":
            import math

            return math.degrees(float(col_value)) if col_value is not None else None
        elif operation_type == "radians":
            import math

            return math.radians(float(col_value)) if col_value is not None else None
        elif operation_type == "sign":
            return (
                1
                if col_value > 0
                else (-1 if col_value < 0 else 0)
                if col_value is not None
                else None
            )
        elif operation_type == "current_date":
            from datetime import date

            return date.today()
        elif operation_type == "current_timestamp":
            from datetime import datetime

            return datetime.now()
        elif operation_type == "unix_timestamp":
            if isinstance(col_value, str) and ("-" in col_value or ":" in col_value):
                from datetime import datetime

                dt = datetime.fromisoformat(col_value.replace(" ", "T"))
                return int(dt.timestamp())
            return None
        elif operation_type == "datediff":
            # For datediff, we need two dates - get both values
            end_date = ConditionEvaluator._get_column_value(row, operation.column)
            start_date = ConditionEvaluator._get_column_value(row, operation.value)

            if end_date is None or start_date is None:
                return None

            try:
                from datetime import datetime

                # Parse end date
                if isinstance(end_date, str):
                    if " " in end_date:  # Has time component
                        end_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                    else:
                        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                else:
                    end_dt = end_date

                # Parse start date
                if isinstance(start_date, str):
                    if " " in start_date:  # Has time component
                        start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                    else:
                        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                else:
                    start_dt = start_date

                # Calculate difference in days
                return (end_dt - start_dt).days
            except (ValueError, AttributeError):
                return None
        elif operation_type == "months_between":
            # For months_between, we need two dates - get both values
            end_date = ConditionEvaluator._get_column_value(row, operation.column)
            start_date = ConditionEvaluator._get_column_value(row, operation.value)

            if end_date is None or start_date is None:
                return None

            try:
                from datetime import datetime

                # Parse end date
                if isinstance(end_date, str):
                    if " " in end_date:  # Has time component
                        end_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                    else:
                        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                else:
                    end_dt = end_date

                # Parse start date
                if isinstance(start_date, str):
                    if " " in start_date:  # Has time component
                        start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                    else:
                        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                else:
                    start_dt = start_date

                # Calculate difference in months using PySpark formula:
                # (year1 - year2) * 12 + (month1 - month2) + (day1 - day2) / 31.0
                year_diff = end_dt.year - start_dt.year
                month_diff = end_dt.month - start_dt.month
                day_diff = end_dt.day - start_dt.day
                return year_diff * 12 + month_diff + day_diff / 31.0
            except (ValueError, AttributeError):
                return None
        elif operation_type == "array_distinct":
            # Remove duplicate elements from an array, preserving insertion order
            if not isinstance(col_value, list):
                return None
            seen = set()
            result = []
            for item in col_value:
                # For hashable types, use the item directly
                # For unhashable types (like lists), convert to tuple or use repr
                try:
                    # Try to use item as-is if it's hashable
                    item_key: Any
                    if isinstance(item, (int, float, str, bool, type(None))):
                        item_key = item
                    elif isinstance(item, list):
                        # Convert list to tuple for hashing
                        item_key = tuple(item)
                    else:
                        # Try to hash directly
                        item_key = item

                    if item_key not in seen:
                        seen.add(item_key)
                        result.append(item)
                except TypeError:
                    # Unhashable type - use string representation as fallback
                    item_str = repr(item)
                    if item_str not in seen:
                        seen.add(item_str)
                        result.append(item)
            return result
        elif operation_type == "array_sort" or operation_type == "sort_array":
            # Sort array elements - operation.value contains asc boolean
            if not isinstance(col_value, list):
                return None
            asc = True
            if hasattr(operation, "value") and operation.value is not None:
                asc = operation.value
            # Sort while preserving type
            try:
                return sorted(col_value, reverse=not asc)
            except TypeError:
                # If items are not directly comparable, convert to strings
                return sorted(col_value, key=str, reverse=not asc)
        else:
            # For other functions, delegate to the existing function evaluation
            # operation_type is guaranteed to be a string in ColumnOperation
            op_str: str = cast("str", operation_type)
            return ConditionEvaluator._evaluate_function_operation(col_value, op_str)

    @staticmethod
    def _evaluate_comparison_operation(
        row: Dict[str, Any], operation: ColumnOperation
    ) -> bool:
        """Evaluate a comparison operation.

        Args:
            row: The data row to evaluate against.
            operation: The comparison operation to evaluate.

        Returns:
            The comparison result.
        """
        left_value = ConditionEvaluator._get_column_value(row, operation.column)
        right_value = ConditionEvaluator._get_column_value(row, operation.value)

        if left_value is None or right_value is None:
            return False

        operation_type = operation.operation
        if operation_type in ["==", "eq"]:
            return cast("bool", left_value == right_value)
        elif operation_type in ["!=", "ne"]:
            return cast("bool", left_value != right_value)
        elif operation_type in ["<", "lt"]:
            return cast("bool", left_value < right_value)
        elif operation_type in ["<=", "le"]:
            return cast("bool", left_value <= right_value)
        elif operation_type in [">", "gt"]:
            return cast("bool", left_value > right_value)
        elif operation_type in [">=", "ge"]:
            return cast("bool", left_value >= right_value)
        else:
            return False

    @staticmethod
    def _evaluate_logical_operation(
        row: Dict[str, Any], operation: ColumnOperation
    ) -> Optional[bool]:
        """Evaluate a logical operation.

        Args:
            row: The data row to evaluate against.
            operation: The logical operation to evaluate.

        Returns:
            The logical result.
        """
        operation_type = operation.operation

        if operation_type in ["and", "&"]:
            left_result = ConditionEvaluator.evaluate_condition(row, operation.column)
            right_result = ConditionEvaluator.evaluate_condition(row, operation.value)
            return left_result and right_result
        elif operation_type in ["or", "|"]:
            left_result = ConditionEvaluator.evaluate_condition(row, operation.column)
            right_result = ConditionEvaluator.evaluate_condition(row, operation.value)
            return left_result or right_result
        elif operation_type in ["not", "!"]:
            return not ConditionEvaluator.evaluate_condition(row, operation.column)
        else:
            return False

    @staticmethod
    def _evaluate_column_operation(
        row: Dict[str, Any], operation: ColumnOperation
    ) -> Optional[bool]:
        """Evaluate a column operation.

        Args:
            row: The data row to evaluate against.
            operation: The column operation to evaluate.

        Returns:
            True if operation evaluates to true, False otherwise.
        """
        operation_type = operation.operation
        col_value = ConditionEvaluator._get_column_value(row, operation.column)

        # Null checks
        if operation_type in ["isNotNull", "isnotnull"]:
            return col_value is not None
        elif operation_type in ["isNull", "isnull"]:
            return col_value is None

        # Comparison operations
        if operation_type in ["==", "!=", ">", ">=", "<", "<="]:
            # operation_type is guaranteed to be a string in ColumnOperation
            op_str: str = cast("str", operation_type)
            return ConditionEvaluator._evaluate_comparison(
                col_value, op_str, operation.value
            )

        # String operations
        if operation_type == "like":
            if operation.value is None:
                return False
            return ConditionEvaluator._evaluate_like_operation(
                col_value, operation.value
            )
        elif operation_type == "isin":
            if operation.value is None:
                return False
            return ConditionEvaluator._evaluate_isin_operation(
                col_value, operation.value
            )
        elif operation_type == "between":
            if operation.value is None:
                return False
            return ConditionEvaluator._evaluate_between_operation(
                col_value, operation.value
            )

        # Function operations (hash, math, string functions)
        if operation_type in [
            "md5",
            "sha1",
            "crc32",
            "upper",
            "lower",
            "length",
            "trim",
            "abs",
            "round",
            "log10",
            "log",
            "log2",
            "concat",
            "split",
            "regexp_replace",
            "coalesce",
            "ceil",
            "floor",
            "sqrt",
            "exp",
            "sin",
            "cos",
            "tan",
            "asin",
            "acos",
            "atan",
            "sinh",
            "cosh",
            "tanh",
            "degrees",
            "radians",
            "sign",
            "greatest",
            "least",
            "when",
            "otherwise",
            "isnull",
            "isnotnull",
            "isnan",
            "nvl",
            "nvl2",
            "current_date",
            "current_timestamp",
            "to_date",
            "to_timestamp",
            "hour",
            "day",
            "dayofmonth",
            "month",
            "year",
            "dayofweek",
            "dayofyear",
            "weekofyear",
            "quarter",
            "minute",
            "second",
            "date_add",
            "date_sub",
            "datediff",
            "unix_timestamp",
            "from_unixtime",
        ]:
            # operation_type is guaranteed to be a string in ColumnOperation
            op_str2: str = cast("str", operation_type)
            return cast(
                "bool",
                ConditionEvaluator._evaluate_function_operation(col_value, op_str2),
            )
        elif operation_type == "transform":
            return cast(
                "bool",
                ConditionEvaluator._evaluate_transform_operation(col_value, operation),
            )

        # Arithmetic operations
        if operation_type in ["+", "-", "*", "/", "%"]:
            left_value = ConditionEvaluator._get_column_value(row, operation.column)
            right_value = ConditionEvaluator._get_column_value(row, operation.value)

            if left_value is None or right_value is None:
                return None

            try:
                if operation_type == "+":
                    # PySpark compatibility: String concatenation with + operator returns None
                    # when DataFrame is cached. Check if both operands are strings.
                    is_string_concatenation = isinstance(
                        left_value, str
                    ) and isinstance(right_value, str)
                    if is_string_concatenation and row.get(
                        "__dataframe_is_cached__", False
                    ):
                        # Check if we're in a cached DataFrame context
                        return None
                    result: Any = left_value + right_value
                    return cast("Optional[bool]", result)
                elif operation_type == "-":
                    return cast("Optional[bool]", left_value - right_value)
                elif operation_type == "*":
                    return cast("bool", left_value * right_value)
                elif operation_type == "/":
                    if right_value == 0:
                        return None
                    return cast("bool", left_value / right_value)
                elif operation_type == "%":
                    if right_value == 0:
                        return None
                    return cast("bool", left_value % right_value)
            except (TypeError, ValueError, ZeroDivisionError):
                return None

        # Logical operations
        if operation_type in ["and", "&"]:
            left_result = ConditionEvaluator.evaluate_condition(row, operation.column)
            right_result = ConditionEvaluator.evaluate_condition(row, operation.value)
            return left_result and right_result
        elif operation_type in ["or", "|"]:
            left_result = ConditionEvaluator.evaluate_condition(row, operation.column)
            right_result = ConditionEvaluator.evaluate_condition(row, operation.value)
            return left_result or right_result
        elif operation_type in ["not", "!"]:
            return not ConditionEvaluator.evaluate_condition(row, operation.column)

        return False

    @staticmethod
    def _evaluate_function_operation(value: Any, operation_type: str) -> Any:
        """Evaluate function operations like md5, sha1, crc32, etc.

        Args:
            value: The input value to the function
            operation_type: The function name (md5, sha1, etc.)

        Returns:
            The result of the function operation, or None if input is None
        """
        # Handle null input - most functions return None for null input
        if value is None:
            return None

        # Hash functions
        if operation_type == "md5":
            import hashlib

            return hashlib.md5(str(value).encode()).hexdigest()
        elif operation_type == "sha1":
            import hashlib

            return hashlib.sha1(str(value).encode()).hexdigest()
        elif operation_type == "crc32":
            import zlib

            return zlib.crc32(str(value).encode()) & 0xFFFFFFFF

        # String functions
        elif operation_type == "upper":
            return str(value).upper()
        elif operation_type == "lower":
            return str(value).lower()
        elif operation_type == "length":
            return len(str(value))
        elif operation_type == "trim":
            return str(value).strip()

        # Math functions
        elif operation_type == "abs":
            return abs(float(value)) if value is not None else None
        elif operation_type == "round":
            return round(float(value)) if value is not None else None
        elif operation_type == "log10":
            import math

            return (
                math.log10(float(value))
                if value is not None and float(value) > 0
                else None
            )
        elif operation_type == "log":
            import math

            return (
                math.log(float(value))
                if value is not None and float(value) > 0
                else None
            )
        elif operation_type == "log2":
            import math

            return (
                math.log2(float(value))
                if value is not None and float(value) > 0
                else None
            )
        elif operation_type == "ceil":
            import math

            return math.ceil(float(value)) if value is not None else None
        elif operation_type == "floor":
            import math

            return math.floor(float(value)) if value is not None else None
        elif operation_type == "sqrt":
            import math

            return (
                math.sqrt(float(value))
                if value is not None and float(value) >= 0
                else None
            )
        elif operation_type == "exp":
            import math

            return math.exp(float(value)) if value is not None else None
        elif operation_type == "sin":
            import math

            return math.sin(float(value)) if value is not None else None
        elif operation_type == "cos":
            import math

            return math.cos(float(value)) if value is not None else None
        elif operation_type == "tan":
            import math

            return math.tan(float(value)) if value is not None else None
        elif operation_type == "asin":
            import math

            return (
                math.asin(float(value))
                if value is not None and -1 <= float(value) <= 1
                else None
            )
        elif operation_type == "acos":
            import math

            return (
                math.acos(float(value))
                if value is not None and -1 <= float(value) <= 1
                else None
            )
        elif operation_type == "atan":
            import math

            return math.atan(float(value)) if value is not None else None
        elif operation_type == "sinh":
            import math

            return math.sinh(float(value)) if value is not None else None
        elif operation_type == "cosh":
            import math

            return math.cosh(float(value)) if value is not None else None
        elif operation_type == "tanh":
            import math

            return math.tanh(float(value)) if value is not None else None
        elif operation_type == "degrees":
            import math

            return math.degrees(float(value)) if value is not None else None
        elif operation_type == "radians":
            import math

            return math.radians(float(value)) if value is not None else None
        elif operation_type == "sign":
            if value is None:
                return None
            val = float(value)
            if val > 0:
                return 1
            elif val < 0:
                return -1
            else:
                return 0

        # String functions
        elif operation_type == "concat":
            # For concat, we need to handle multiple arguments
            # This is a simplified version - in practice, concat might need special handling
            return str(value) if value is not None else None
        elif operation_type == "split":
            # For split, we need the delimiter - this is a simplified version
            return str(value).split() if value is not None else None
        elif operation_type == "regexp_replace":
            # For regexp_replace, we need pattern and replacement - this is a simplified version
            return str(value) if value is not None else None

        # Conditional functions
        elif operation_type == "coalesce":
            # For coalesce, we need multiple values - this is a simplified version
            return value if value is not None else None
        elif operation_type == "isnull":
            return value is None
        elif operation_type == "isnotnull":
            return value is not None
        elif operation_type == "isnan":
            if value is None:
                return False
            try:
                import math

                return math.isnan(float(value))
            except (ValueError, TypeError):
                return False
        elif operation_type == "nvl":
            # For nvl, we need a default value - this is a simplified version
            return value if value is not None else None
        elif operation_type == "nvl2":
            # For nvl2, we need two default values - this is a simplified version
            return value if value is not None else None

        # Comparison functions
        elif operation_type == "greatest":
            # For greatest, we need multiple values - this is a simplified version
            return value if value is not None else None
        elif operation_type == "least":
            # For least, we need multiple values - this is a simplified version
            return value if value is not None else None

        # Datetime functions
        elif operation_type == "current_date":
            from datetime import date

            return date.today()
        elif operation_type == "current_timestamp":
            from datetime import datetime

            return datetime.now()
        elif operation_type == "to_date":
            # For to_date, we need a format - this is a simplified version
            if value is None:
                return None
            try:
                from datetime import datetime

                return datetime.strptime(str(value), "%Y-%m-%d").date()
            except ValueError:
                return None
        elif operation_type == "to_timestamp":
            # For to_timestamp, we need a format - this is a simplified version
            if value is None:
                return None
            try:
                from datetime import datetime

                return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        elif operation_type == "hour":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return dt.hour
            except (ValueError, AttributeError):
                return None
        elif operation_type == "day" or operation_type == "dayofmonth":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return dt.day
            except (ValueError, AttributeError):
                return None
        elif operation_type == "month":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return dt.month
            except (ValueError, AttributeError):
                return None
        elif operation_type == "year":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return dt.year
            except (ValueError, AttributeError):
                return None
        elif operation_type == "dayofweek":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return dt.weekday() + 1  # PySpark uses 1-based weekday
            except (ValueError, AttributeError):
                return None
        elif operation_type == "dayofyear":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return dt.timetuple().tm_yday
            except (ValueError, AttributeError):
                return None
        elif operation_type == "weekofyear":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return dt.isocalendar()[1]
            except (ValueError, AttributeError):
                return None
        elif operation_type == "quarter":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return (dt.month - 1) // 3 + 1
            except (ValueError, AttributeError):
                return None
        elif operation_type == "minute":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return dt.minute
            except (ValueError, AttributeError):
                return None
        elif operation_type == "second":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return dt.second
            except (ValueError, AttributeError):
                return None
        elif operation_type == "date_add":
            # For date_add, we need days to add - this is a simplified version
            if value is None:
                return None
            try:
                from datetime import datetime, timedelta

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d")
                else:
                    dt = value
                return dt + timedelta(days=1)  # Simplified: always add 1 day
            except (ValueError, AttributeError):
                return None
        elif operation_type == "date_sub":
            # For date_sub, we need days to subtract - this is a simplified version
            if value is None:
                return None
            try:
                from datetime import datetime, timedelta

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d")
                else:
                    dt = value
                return dt - timedelta(days=1)  # Simplified: always subtract 1 day
            except (ValueError, AttributeError):
                return None
        elif operation_type == "datediff":
            # For datediff, we need two dates - this is a simplified version
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d")
                else:
                    dt = value
                return (datetime.now() - dt).days
            except (ValueError, AttributeError):
                return None
        elif operation_type == "unix_timestamp":
            if value is None:
                return None
            try:
                from datetime import datetime

                if isinstance(value, str):
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                else:
                    dt = value
                return dt.timestamp()
            except (ValueError, AttributeError):
                return None
        elif operation_type == "from_unixtime":
            if value is None:
                return None
            try:
                from datetime import datetime

                return datetime.fromtimestamp(float(value))
            except (ValueError, AttributeError):
                return None

        # Default fallback
        return None

    @staticmethod
    def _get_column_value(row: Dict[str, Any], column: Union[Column, str, Any]) -> Any:
        """Get column value from row.

        Args:
            row: Data row.
            column: Column reference.

        Returns:
            Column value.
        """
        # Check ColumnOperation BEFORE Column since ColumnOperation is a subclass of Column
        # This ensures nested operations are properly evaluated, not treated as simple column references
        if isinstance(column, ColumnOperation):
            # Recursively evaluate the operation
            return ConditionEvaluator._evaluate_column_operation_value(row, column)
        elif isinstance(column, Column):
            return row.get(column.name)
        elif isinstance(column, str):
            return row.get(column)
        elif hasattr(column, "value"):
            # Literal or similar object with a value attribute
            return column.value
        else:
            return column

    @staticmethod
    def _coerce_for_comparison(left_val: Any, right_val: Any) -> Tuple[Any, Any]:
        """Coerce string to numeric for comparison if one is numeric and other is string.

        PySpark behavior: when comparing string with numeric, try to cast string to numeric.

        Args:
            left_val: Left value
            right_val: Right value

        Returns:
            Tuple of (coerced_left, coerced_right)
        """
        # Left is string, right is numeric: convert left to numeric
        if isinstance(left_val, str) and isinstance(right_val, (int, float)):
            try:
                if isinstance(right_val, int):
                    # Try integer first, then float
                    try:
                        left_num: Union[int, float] = int(float(left_val))
                    except (ValueError, TypeError):
                        left_num = float(left_val)
                else:
                    left_num = float(left_val)
                return left_num, right_val
            except (ValueError, TypeError):
                # Conversion failed, return original values
                return left_val, right_val
        # Right is string, left is numeric: convert right to numeric
        elif isinstance(right_val, str) and isinstance(left_val, (int, float)):
            try:
                if isinstance(left_val, int):
                    try:
                        right_num: Union[int, float] = int(float(right_val))
                    except (ValueError, TypeError):
                        right_num = float(right_val)
                else:
                    right_num = float(right_val)
                return left_val, right_num
            except (ValueError, TypeError):
                # Conversion failed, return original values
                return left_val, right_val
        # No coercion needed
        return left_val, right_val

    @staticmethod
    def _evaluate_comparison(
        col_value: Any, operation: str, condition_value: Any
    ) -> bool:
        """Evaluate comparison operations.

        Args:
            col_value: Column value.
            operation: Comparison operation.
            condition_value: Value to compare against.

        Returns:
            True if comparison is true.
        """
        if col_value is None or condition_value is None:
            return operation == "!="  # Only != returns True for null values

        # Apply coercion if types are different
        coerced_left, coerced_right = ConditionEvaluator._coerce_for_comparison(
            col_value, condition_value
        )

        if operation == "==":
            return bool(coerced_left == coerced_right)
        elif operation == "!=":
            return bool(coerced_left != coerced_right)
        elif operation == ">":
            return bool(coerced_left > coerced_right)
        elif operation == ">=":
            return bool(coerced_left >= coerced_right)
        elif operation == "<":
            return bool(coerced_left < coerced_right)
        elif operation == "<=":
            return bool(coerced_left <= coerced_right)

        return False

    @staticmethod
    def _evaluate_like_operation(col_value: Any, pattern: str) -> bool:
        """Evaluate LIKE operation.

        Args:
            col_value: Column value.
            pattern: LIKE pattern.

        Returns:
            True if pattern matches.
        """
        if col_value is None:
            return False

        import re

        value = str(col_value)
        regex_pattern = str(pattern).replace("%", ".*")
        return bool(re.match(regex_pattern, value))

    @staticmethod
    def _evaluate_isin_operation(col_value: Any, values: List[Any]) -> bool:
        """Evaluate IN operation.

        Args:
            col_value: Column value.
            values: List of values to check against.

        Returns:
            True if value is in list.
        """
        return col_value in values if col_value is not None else False

    @staticmethod
    def _evaluate_between_operation(col_value: Any, bounds: Tuple[Any, Any]) -> bool:
        """Evaluate BETWEEN operation.

        Args:
            col_value: Column value.
            bounds: Tuple of (lower, upper) bounds.

        Returns:
            True if value is between bounds.
        """
        if col_value is None:
            return False

        lower, upper = bounds
        return bool(lower <= col_value <= upper)

    @staticmethod
    def _evaluate_transform_operation(value: Any, operation: Any) -> Any:
        """Evaluate transform operations for higher-order array functions.

        Args:
            value: The input value (array) to transform
            operation: The ColumnOperation containing the transform operation

        Returns:
            The transformed array, or None if input is None
        """
        # Handle null input
        if value is None:
            return None

        # Get the lambda function from the operation
        lambda_expr = operation.value

        # Apply the transform using Python lambda evaluation
        try:
            # If lambda_expr is callable, apply it directly
            if callable(lambda_expr):
                if isinstance(value, list):
                    return [lambda_expr(x) for x in value]
                else:
                    return value
            else:
                # Return original value if lambda is not callable
                return value
        except Exception as e:
            print(f"Warning: Failed to evaluate transform lambda: {e}")
            return value  # Return original value if evaluation fails
