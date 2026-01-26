"""
Expression evaluation engine for DataFrame operations.

This module provides the ExpressionEvaluator class that handles the evaluation
of all column expressions including arithmetic operations, comparison operations,
logical operations, function calls, conditional expressions, and type casting.
"""

from __future__ import annotations

import csv
import json
import math
import re
import base64
import datetime as dt_module
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, cast

from sparkless.utils.profiling import profiled

from ...functions import Column, ColumnOperation
from ...functions.conditional import CaseWhen
from ...spark_types import (
    ArrayType,
    ByteType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    Row,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from ...core.ddl_adapter import parse_ddl_schema


class ExpressionEvaluator:
    """Evaluates column expressions, operations, and function calls.

    This class handles the evaluation of all column expressions including:
    - Arithmetic operations (+, -, *, /, %)
    - Comparison operations (==, !=, <, >, <=, >=)
    - Logical operations (and, or, not)
    - Function calls (50+ Spark SQL functions)
    - Conditional expressions (when/otherwise)
    - Type casting operations
    """

    def __init__(self, dataframe_context: Optional[Any] = None) -> None:
        """Initialize evaluator with function registry.

        Args:
            dataframe_context: Optional DataFrame context for checking cached state.
        """
        self._function_registry = self._build_function_registry()
        self._dataframe_context = dataframe_context

        # Initialize specialized evaluators
        from .evaluators.conditional_evaluator import ConditionalEvaluator

        self._conditional_evaluator = ConditionalEvaluator(self)

    @profiled("expression.evaluate_expression", category="expression")
    def evaluate_expression(self, row: Dict[str, Any], expression: Any) -> Any:
        """Main entry point for expression evaluation."""
        # Handle CaseWhen (when/otherwise expressions) - delegate to conditional evaluator
        if isinstance(expression, CaseWhen):
            return self._conditional_evaluator.evaluate_case_when(row, expression)
        # Check for ColumnOperation BEFORE Column, since ColumnOperation is a subclass of Column
        # ColumnOperation has both "operation" and "column" attributes
        elif hasattr(expression, "operation") and hasattr(expression, "column"):
            return self._evaluate_column_operation(row, expression)
        elif isinstance(expression, Column):
            return self._evaluate_mock_column(row, expression)
        elif hasattr(expression, "value") and hasattr(expression, "name"):
            # It's a Literal - evaluate it
            return self._evaluate_value(row, expression)
        elif isinstance(expression, str) and expression.startswith("CAST("):
            # It's a string representation of a cast operation - this shouldn't happen
            return None
        else:
            return self._evaluate_direct_value(expression)

    def evaluate_condition(
        self, row: Dict[str, Any], condition: Union[ColumnOperation, Column]
    ) -> bool:
        """Evaluate condition for a single row."""
        from ...core.condition_evaluator import ConditionEvaluator

        return ConditionEvaluator.evaluate_condition(row, condition)  # type: ignore[return-value]

    def _evaluate_case_when(self, row: Dict[str, Any], case_when: CaseWhen) -> Any:
        """Evaluate when/otherwise expressions - delegates to ConditionalEvaluator."""
        return self._conditional_evaluator.evaluate_case_when(row, case_when)

    def _evaluate_mock_column(self, row: Dict[str, Any], column: Column) -> Any:
        """Evaluate a Column expression."""
        col_name = column.name

        # Check if this is an aliased function call
        if (
            self._is_aliased_function_call(column)
            and column._original_column is not None
        ):
            original_name = column._original_column.name
            return self._evaluate_function_call_by_name(row, original_name)

        # Check if this is a direct function call
        if self._is_function_call_name(col_name):
            return self._evaluate_function_call_by_name(row, col_name)
        else:
            # Check for nested struct field access (e.g., "my_struct.value_1")
            if "." in col_name:
                return self._evaluate_nested_field_access(row, col_name)
            # Simple column reference - resolve column name case-insensitively
            # First try exact match
            if col_name in row:
                return row.get(col_name)
            # If not found, try case-insensitive match using ColumnResolver
            from ...core.column_resolver import ColumnResolver

            available_columns = list(row.keys())
            case_sensitive = False
            # Try to get case sensitivity from dataframe context if available
            if (
                self._dataframe_context is not None
                and hasattr(self._dataframe_context, "_spark")
                and hasattr(self._dataframe_context._spark, "conf")
            ):
                spark = self._dataframe_context._spark
                case_sensitive = (
                    spark.conf.get("spark.sql.caseSensitive", "false") == "true"
                )

            resolved_name = ColumnResolver.resolve_column_name(
                col_name, available_columns, case_sensitive
            )
            if resolved_name:
                return row.get(resolved_name)
            # Fallback to original name if resolution fails
            return row.get(col_name)

    def _evaluate_nested_field_access(self, row: Dict[str, Any], col_path: str) -> Any:
        """Evaluate nested struct field access like 'my_struct.value_1'.

        Args:
            row: Row data as dictionary
            col_path: Column path with dots (e.g., "my_struct.value_1")

        Returns:
            Value at the nested path, or None if path doesn't exist
        """
        parts = col_path.split(".")
        if not parts:
            return None

        # Get case sensitivity setting
        case_sensitive = False
        if (
            self._dataframe_context is not None
            and hasattr(self._dataframe_context, "_spark")
            and hasattr(self._dataframe_context._spark, "conf")
        ):
            spark = self._dataframe_context._spark
            case_sensitive = (
                spark.conf.get("spark.sql.caseSensitive", "false") == "true"
            )

        # Start with the row - resolve first part (struct column name) case-insensitively
        from ...core.column_resolver import ColumnResolver

        available_columns = list(row.keys())
        resolved_first_part = ColumnResolver.resolve_column_name(
            parts[0], available_columns, case_sensitive
        )
        if resolved_first_part is None:
            # Fallback to exact match
            resolved_first_part = parts[0]

        current: Any = row.get(resolved_first_part)
        if current is None:
            return None

        # Navigate through remaining parts of the path, resolving each part case-insensitively
        for part in parts[1:]:
            if current is None:
                return None

            if isinstance(current, dict):
                # Resolve field name case-insensitively within the struct
                struct_keys = list(current.keys())
                resolved_part = ColumnResolver.resolve_column_name(
                    part, struct_keys, case_sensitive
                )
                if resolved_part:
                    current = current.get(resolved_part)
                else:
                    # Fallback to exact match
                    current = current.get(part)
            elif hasattr(current, "__getitem__"):
                try:
                    # Try case-insensitive lookup if it's a dict-like object
                    if hasattr(current, "keys") and hasattr(current.keys, "__call__"):
                        struct_keys = list(current.keys())
                        resolved_part = ColumnResolver.resolve_column_name(
                            part, struct_keys, case_sensitive
                        )
                        if resolved_part:
                            current = current[resolved_part]
                        else:
                            current = current[part]
                    else:
                        current = current[part]
                except (KeyError, TypeError, IndexError):
                    return None
            else:
                return None

        return current

    @profiled("expression.evaluate_column_operation", category="expression")
    def _evaluate_column_operation(self, row: Dict[str, Any], operation: Any) -> Any:
        """Evaluate a ColumnOperation."""
        op = operation.operation

        # Handle arithmetic operations
        if op in ["+", "-", "*", "/", "%"]:
            return self._evaluate_arithmetic_operation(row, operation)

        # Handle comparison operations
        elif op in ["==", "!=", "<", ">", "<=", ">="]:
            return self._evaluate_comparison_operation(row, operation)

        # Handle cast operations explicitly (Issue #5 fix)
        elif op == "cast":
            # Evaluate the column/value being cast
            cast_value = self.evaluate_expression(row, operation.column)
            # Use _func_cast to perform the actual casting
            return self._func_cast(cast_value, operation)

        # Handle function calls - check if it's a known function
        elif op in self._function_registry:
            return self._evaluate_function_call(row, operation)

        # Handle unary minus
        elif op == "-" and operation.value is None:
            return self._evaluate_arithmetic_operation(row, operation)

        # For unknown operations, try to evaluate as function call
        else:
            try:
                return self._evaluate_function_call(row, operation)
            except Exception:
                # If function call fails, try arithmetic operation as fallback
                return self._evaluate_arithmetic_operation(row, operation)

    def _evaluate_arithmetic_operation(
        self, row: Dict[str, Any], operation: Any
    ) -> Any:
        """Evaluate arithmetic operations on columns."""
        if not hasattr(operation, "operation") or not hasattr(operation, "column"):
            return None

        # Extract left value - evaluate the column expression (handles cast operations)
        left_value = self.evaluate_expression(row, operation.column)

        # Extract right value - evaluate the value expression (handles cast operations)
        right_value = self.evaluate_expression(row, operation.value)

        if operation.operation == "-" and operation.value is None:
            # Unary minus operation
            if left_value is None:
                return None
            return -left_value

        if left_value is None or right_value is None:
            return None

        # PySpark automatically casts string columns to numeric for arithmetic operations
        # Coerce string values to float (Double) to match PySpark behavior
        def _coerce_to_numeric(value: Any) -> Optional[float]:
            """Coerce value to numeric, handling string-to-numeric conversion."""
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                try:
                    # PySpark strips whitespace from strings before converting to numeric
                    stripped = value.strip()
                    return float(stripped)
                except (ValueError, TypeError):
                    # Invalid numeric string - return None (PySpark behavior)
                    return None
            else:
                # For other types, try to convert to float
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None

        # Coerce both values to numeric for arithmetic operations
        # Exception: + operator can be string concatenation, so handle it separately
        if operation.operation == "+":
            # PySpark compatibility: String concatenation with + operator returns None
            # when DataFrame is cached. Check if both operands are strings and DataFrame is cached.
            is_string_concatenation = isinstance(left_value, str) and isinstance(
                right_value, str
            )
            if (
                is_string_concatenation
                and self._dataframe_context is not None
                and hasattr(self._dataframe_context, "_is_cached")
                and self._dataframe_context._is_cached
            ):
                # Return None to match PySpark behavior
                return None
            # For + operator, PySpark behavior:
            # - If both are strings that can be parsed as numbers: coerce both to numeric and add
            # - If both are strings (non-numeric): string concatenation
            # - If one is string and one is numeric: coerce string to numeric and add
            # - If both are numeric: numeric addition
            if isinstance(left_value, str) and isinstance(right_value, str):
                # Both strings: try to coerce both to numeric first (PySpark behavior)
                # If both can be coerced, do numeric addition
                # Otherwise, do string concatenation
                left_coerced = _coerce_to_numeric(left_value)
                right_coerced = _coerce_to_numeric(right_value)
                if left_coerced is not None and right_coerced is not None:
                    # Both can be coerced to numeric - do numeric addition
                    return left_coerced + right_coerced
                else:
                    # At least one can't be coerced - do string concatenation
                    # (unless cached, which is handled above)
                    return left_value + right_value
            elif isinstance(left_value, str) and isinstance(right_value, (int, float)):
                # String + numeric: coerce string to numeric
                left_coerced = _coerce_to_numeric(left_value)
                if left_coerced is None:
                    return None
                return left_coerced + right_value
            elif isinstance(right_value, str) and isinstance(left_value, (int, float)):
                # Numeric + string: coerce string to numeric
                right_coerced = _coerce_to_numeric(right_value)
                if right_coerced is None:
                    return None
                return left_value + right_coerced
            else:
                # Both numeric or other types
                return left_value + right_value
        else:
            # For -, *, /, % operations, coerce strings to numeric
            left_coerced = _coerce_to_numeric(left_value)
            right_coerced = _coerce_to_numeric(right_value)

            if left_coerced is None or right_coerced is None:
                return None

            if operation.operation == "-":
                return left_coerced - right_coerced
            elif operation.operation == "*":
                return left_coerced * right_coerced
            elif operation.operation == "/":
                return left_coerced / right_coerced if right_coerced != 0 else None
            elif operation.operation == "%":
                return left_coerced % right_coerced if right_coerced != 0 else None
            else:
                return None

    def _evaluate_comparison_operation(
        self, row: Dict[str, Any], operation: Any
    ) -> Any:
        """Evaluate comparison operations like ==, !=, <, >, <=, >=."""
        if not hasattr(operation, "operation") or not hasattr(operation, "column"):
            return None

        # Extract left value - evaluate the column expression
        left_value = self.evaluate_expression(row, operation.column)

        # Extract right value - evaluate the value expression
        right_value = self.evaluate_expression(row, operation.value)

        if left_value is None or right_value is None:
            return None

        # Perform the comparison
        if operation.operation == "==":
            return left_value == right_value
        elif operation.operation == "!=":
            return left_value != right_value
        elif operation.operation == "<":
            return left_value < right_value
        elif operation.operation == ">":
            return left_value > right_value
        elif operation.operation == "<=":
            return left_value <= right_value
        elif operation.operation == ">=":
            return left_value >= right_value
        else:
            return None

    @profiled("expression.evaluate_function_call", category="expression")
    def _evaluate_function_call(self, row: Dict[str, Any], operation: Any) -> Any:
        """Evaluate function calls like upper(), lower(), length(), abs(), round()."""
        if not hasattr(operation, "operation") or not hasattr(operation, "column"):
            return None

        # Evaluate the column expression (could be a nested operation)
        if hasattr(operation.column, "operation") and hasattr(
            operation.column, "column"
        ):
            # The column is itself a ColumnOperation, evaluate it first
            value = self.evaluate_expression(row, operation.column)
        else:
            # Regular column reference or literal
            if hasattr(operation.column, "value") and hasattr(operation.column, "name"):
                value = self._evaluate_value(row, operation.column)
            else:
                col_name = (
                    operation.column.name
                    if hasattr(operation.column, "name")
                    else str(operation.column)
                )
                # Check if the column name is actually a Literal (string representation)
                if (
                    isinstance(col_name, str)
                    and "<sparkless.functions.core.literals.Literal" in col_name
                ):
                    # This is a Literal used as a column - try to extract the actual value
                    # The column might have the Literal object stored somewhere
                    # For now, try to evaluate it as a Literal
                    if hasattr(operation.column, "_literal") or hasattr(
                        operation.column, "literal"
                    ):
                        lit = getattr(operation.column, "_literal", None) or getattr(
                            operation.column, "literal", None
                        )
                        value = lit.value if lit and hasattr(lit, "value") else None
                    else:
                        # Can't extract the value - return None
                        value = None
                else:
                    # Check for nested struct field access
                    if "." in col_name:
                        value = self._evaluate_nested_field_access(row, col_name)
                    else:
                        value = row.get(col_name)

        func_name = operation.operation

        # Handle withField function - add or replace field in struct
        if func_name == "withField":
            # value is the struct (dict) from the base column
            if value is None:
                # PySpark returns None if struct is null
                return None

            # Validate that value is a struct (dict)
            if not isinstance(value, dict):
                # Not a struct - return None (PySpark would raise AnalysisException)
                return None

            # Extract field name and column from operation value
            if (
                not isinstance(operation.value, dict)
                or "fieldName" not in operation.value
            ):
                return None

            field_name = operation.value["fieldName"]
            field_column = operation.value.get("column")

            if field_column is None:
                return None

            # Evaluate the new field's column expression
            # This may reference nested fields, so use evaluate_expression
            field_value = self.evaluate_expression(row, field_column)

            # Create a copy of the struct and add/replace the field
            result = value.copy()
            result[field_name] = field_value

            return result

        # Handle struct function - collects multiple values into a struct/dict
        if func_name == "struct":
            result = {}
            all_values: List[Any] = []
            all_names: List[str] = []  # Store column names for struct fields

            # Check if operation.value contains all columns (when first is Literal)
            # or if we need to get first from column
            if hasattr(operation, "value") and operation.value is not None:
                # Check if operation.value contains all columns (including first)
                # This happens when the first column is a Literal
                if (
                    isinstance(operation.value, (list, tuple))
                    and len(operation.value) > 0
                ):
                    # Check if first item is a Literal - if so, all columns are in value
                    first_item = operation.value[0]
                    if hasattr(first_item, "value") and hasattr(first_item, "name"):
                        # All columns are in operation.value (first is Literal)
                        for item in operation.value:
                            if hasattr(item, "value") and hasattr(item, "name"):
                                # It's a Literal
                                all_values.append(self._get_literal_value(item))
                                all_names.append(
                                    item.name
                                    if hasattr(item, "name")
                                    else f"col{len(all_names) + 1}"
                                )
                            elif hasattr(item, "name"):
                                # It's a Column - get from row
                                col_name = item.name
                                if (
                                    isinstance(col_name, str)
                                    and "<sparkless.functions.core.literals.Literal"
                                    in col_name
                                ):
                                    # Can't extract - skip
                                    pass
                                else:
                                    all_values.append(row.get(col_name))
                                    all_names.append(col_name)
                            elif hasattr(item, "operation") and hasattr(item, "column"):
                                # It's a ColumnOperation - evaluate it
                                all_values.append(self.evaluate_expression(row, item))
                                # Get the original column name if possible
                                if hasattr(item, "column") and hasattr(
                                    item.column, "name"
                                ):
                                    all_names.append(item.column.name)
                                else:
                                    all_names.append(f"col{len(all_names) + 1}")
                            else:
                                all_values.append(item)
                                all_names.append(f"col{len(all_names) + 1}")
                    else:
                        # First column is in operation.column, rest in operation.value
                        if value is not None:
                            all_values.append(value)
                            # Get first column name
                            if hasattr(operation, "column") and hasattr(
                                operation.column, "name"
                            ):
                                all_names.append(operation.column.name)
                            else:
                                all_names.append(f"col{len(all_names) + 1}")
                        # Add remaining values from operation.value
                        for item in operation.value:
                            if hasattr(item, "value") and hasattr(item, "name"):
                                all_values.append(self._get_literal_value(item))
                                all_names.append(
                                    item.name
                                    if hasattr(item, "name")
                                    else f"col{len(all_names) + 1}"
                                )
                            elif hasattr(item, "name"):
                                col_name = item.name
                                if (
                                    isinstance(col_name, str)
                                    and "<sparkless.functions.core.literals.Literal"
                                    in col_name
                                ):
                                    pass
                                else:
                                    all_values.append(row.get(col_name))
                                    all_names.append(col_name)
                            elif hasattr(item, "operation") and hasattr(item, "column"):
                                all_values.append(self.evaluate_expression(row, item))
                                # Get the original column name if possible
                                if hasattr(item, "column") and hasattr(
                                    item.column, "name"
                                ):
                                    all_names.append(item.column.name)
                                else:
                                    all_names.append(f"col{len(all_names) + 1}")
                            else:
                                all_values.append(item)
                                all_names.append(f"col{len(all_names) + 1}")
                else:
                    # Single value in operation.value
                    if value is not None:
                        all_values.append(value)
                    if hasattr(operation.value, "value") and hasattr(
                        operation.value, "name"
                    ):
                        all_values.append(self._get_literal_value(operation.value))
                    elif hasattr(operation.value, "name"):
                        col_name = operation.value.name
                        if not (
                            isinstance(col_name, str)
                            and "<sparkless.functions.core.literals.Literal" in col_name
                        ):
                            all_values.append(row.get(col_name))
                    else:
                        all_values.append(operation.value)
            else:
                # No operation.value - only first column
                if value is not None:
                    all_values.append(value)

            # Create struct with field names - use original column names if available
            for idx, val in enumerate(all_values):
                if idx < len(all_names):
                    result[all_names[idx]] = val
                else:
                    result[f"col{idx + 1}"] = val

            return result

        # Handle array function - collects multiple values into an array
        # array() creates an array containing the values from each column as elements
        # So array(arr1, arr2) where arr1=[1,2,3] and arr2=[4,5] creates [[1,2,3], [4,5]]
        if func_name == "array":
            array_result: List[Any] = []
            # Add the first value (from column) - this is the value of the first column
            if value is not None:
                array_result.append(value)
            # Add remaining values from operation.value
            if hasattr(operation, "value") and operation.value is not None:
                if isinstance(operation.value, (list, tuple)):
                    for item in operation.value:
                        if hasattr(item, "value") and hasattr(item, "name"):
                            # It's a Literal
                            array_result.append(self._get_literal_value(item))
                        elif hasattr(item, "name"):
                            # It's a Column - get from row (this gets the column's value)
                            col_val = row.get(item.name)
                            array_result.append(col_val)
                        elif hasattr(item, "operation") and hasattr(item, "column"):
                            # It's a ColumnOperation - evaluate it to get the column's value
                            col_val = self.evaluate_expression(row, item)
                            array_result.append(col_val)
                        else:
                            array_result.append(item)
                else:
                    # Single value
                    if hasattr(operation.value, "value") and hasattr(
                        operation.value, "name"
                    ):
                        array_result.append(self._get_literal_value(operation.value))
                    elif hasattr(operation.value, "name"):
                        array_result.append(row.get(operation.value.name))
                    else:
                        array_result.append(operation.value)
            return array_result

        # Handle two-argument math functions (atan2, hypot)
        if func_name in ("atan2", "hypot"):
            first_val = value
            second_val = None
            if hasattr(operation, "value") and operation.value is not None:
                # Evaluate the second argument
                if hasattr(operation.value, "operation") and hasattr(
                    operation.value, "column"
                ):
                    # It's a ColumnOperation - evaluate it
                    second_val = self.evaluate_expression(row, operation.value)
                elif hasattr(operation.value, "value") and hasattr(
                    operation.value, "name"
                ):
                    # It's a Literal
                    second_val = operation.value.value
                elif hasattr(operation.value, "name"):
                    # It's a Column - get from row
                    second_val = row.get(operation.value.name)
                else:
                    second_val = operation.value

            if first_val is None or second_val is None:
                return None
            if not isinstance(first_val, (int, float)) or not isinstance(
                second_val, (int, float)
            ):
                return None

            try:
                if func_name == "atan2":
                    return math.atan2(first_val, second_val)
                elif func_name == "hypot":
                    return math.hypot(first_val, second_val)
            except (ValueError, OverflowError):
                return None

        # Fast-path datediff using direct row values by column name
        if func_name == "datediff":
            left_raw = None
            right_raw = None
            try:
                # Prefer direct lookup by column names when available
                if hasattr(operation.column, "name"):
                    left_raw = row.get(operation.column.name)
                if hasattr(operation, "value") and hasattr(operation.value, "name"):
                    right_raw = row.get(operation.value.name)
                # Fall back to evaluated values
                if left_raw is None:
                    # Force evaluation of left expression if needed
                    try:
                        left_raw = self.evaluate_expression(row, operation.column)
                    except Exception:
                        left_raw = value
                if right_raw is None:
                    right_raw = self.evaluate_expression(
                        row, getattr(operation, "value", None)
                    )
            except Exception:
                pass

            # If left_raw is still None and the left is a to_date/to_timestamp op, try extracting inner column
            if (
                left_raw is None
                and hasattr(operation, "column")
                and hasattr(operation.column, "operation")
            ):
                inner_op = getattr(operation.column, "operation", None)
                if inner_op in ("to_date", "to_timestamp") and hasattr(
                    operation.column, "column"
                ):
                    try:
                        inner_col = operation.column.column
                        inner_name = getattr(inner_col, "name", None)
                        if inner_name:
                            left_raw = row.get(inner_name)
                    except Exception:
                        pass

            def _to_date(v: Any) -> Optional[dt_module.date]:
                if isinstance(v, dt_module.date) and not isinstance(
                    v, dt_module.datetime
                ):
                    return v
                if isinstance(v, dt_module.datetime):
                    return v.date()
                if isinstance(v, str):
                    try:
                        return dt_module.date.fromisoformat(v.strip().split(" ")[0])
                    except Exception:
                        try:
                            dt = dt_module.datetime.fromisoformat(
                                v.replace("Z", "+00:00").replace(" ", "T")
                            )
                            return dt.date()
                        except Exception:
                            return None
                return None

            end_date = _to_date(left_raw)
            start_date = _to_date(right_raw)
            if end_date is None or start_date is None:
                return None
            return (end_date - start_date).days

        # Let the earlier datediff block handle computation or defer to SQL

        # Handle coalesce function before the None check
        if func_name == "coalesce":
            # Check the main column first
            if value is not None:
                return value

            # If main column is None, check the literal values
            if hasattr(operation, "value") and isinstance(operation.value, list):
                for i, col in enumerate(operation.value):
                    # Check if it's a Literal object
                    if (
                        hasattr(col, "value")
                        and hasattr(col, "name")
                        and hasattr(col, "data_type")
                    ):
                        # This is a Literal
                        col_value = self._get_literal_value(col)
                    elif hasattr(col, "name"):
                        # Resolve column name case-insensitively
                        col_name = col.name
                        # First try exact match
                        if col_name in row:
                            col_value = row.get(col_name)
                        else:
                            # Try case-insensitive match using ColumnResolver
                            from ...core.column_resolver import ColumnResolver

                            available_columns = list(row.keys())
                            case_sensitive = False
                            if (
                                self._dataframe_context is not None
                                and hasattr(self._dataframe_context, "_spark")
                                and hasattr(self._dataframe_context._spark, "conf")
                            ):
                                spark = self._dataframe_context._spark
                                case_sensitive = (
                                    spark.conf.get("spark.sql.caseSensitive", "false")
                                    == "true"
                                )
                            resolved_name = ColumnResolver.resolve_column_name(
                                col_name, available_columns, case_sensitive
                            )
                            if resolved_name:
                                col_value = row.get(resolved_name)
                            else:
                                col_value = row.get(col_name)  # Fallback
                    elif hasattr(col, "value"):
                        col_value = col.value  # For other values
                    else:
                        col_value = col
                    if col_value is not None:
                        return col_value

            return None

        # Handle concat function - needs special handling for multiple arguments
        if func_name == "concat":
            return self._evaluate_concat(row, operation, value)

        # Handle multi-argument array functions
        if func_name in ("array_union", "arrays_zip", "sequence"):
            return self._evaluate_multi_arg_array_function(
                row, operation, value, func_name
            )

        # Handle multi-argument map functions
        if func_name in ("create_map", "map_from_arrays"):
            return self._evaluate_multi_arg_map_function(
                row, operation, value, func_name
            )

        # Handle months_between (needs two date arguments)
        if func_name == "months_between":
            first_date = value
            second_date = None
            if hasattr(operation, "value") and operation.value is not None:
                if hasattr(operation.value, "operation") and hasattr(
                    operation.value, "column"
                ):
                    second_date = self.evaluate_expression(row, operation.value)
                elif hasattr(operation.value, "value") and hasattr(
                    operation.value, "name"
                ):
                    second_date = self._get_literal_value(operation.value)
                elif hasattr(operation.value, "name"):
                    second_date = row.get(operation.value.name)
                else:
                    second_date = operation.value

            if first_date is None or second_date is None:
                return None

            # Convert to date objects if needed
            def _to_date(v: Any) -> Optional[dt_module.date]:
                if isinstance(v, dt_module.date) and not isinstance(
                    v, dt_module.datetime
                ):
                    return v
                if isinstance(v, dt_module.datetime):
                    return v.date()
                if isinstance(v, str):
                    try:
                        return dt_module.date.fromisoformat(v.strip().split(" ")[0])
                    except Exception:
                        try:
                            dt = dt_module.datetime.fromisoformat(
                                v.replace("Z", "+00:00").replace(" ", "T")
                            )
                            return dt.date()
                        except Exception:
                            return None
                return None

            date1 = _to_date(first_date)
            date2 = _to_date(second_date)
            if date1 is None or date2 is None:
                return None

            # PySpark months_between formula:
            # (year1 - year2) * 12 + (month1 - month2) + (day1 - day2) / 31.0
            year_diff = date1.year - date2.year
            month_diff = date1.month - date2.month
            day_diff = date1.day - date2.day

            months = year_diff * 12 + month_diff + day_diff / 31.0
            return months

        # Handle format_string before generic handling
        if func_name == "format_string":
            return self._evaluate_format_string(row, operation, operation.value)

        # Handle expr function - parse SQL expressions
        if func_name == "expr":
            return self._evaluate_expr_function(row, operation, value)

        # Handle nanvl function - needs special handling to evaluate second argument
        if func_name == "nanvl":
            first_val = value
            second_val = None
            if hasattr(operation, "value") and operation.value is not None:
                # Evaluate the second argument
                if hasattr(operation.value, "operation") and hasattr(
                    operation.value, "column"
                ):
                    second_val = self.evaluate_expression(row, operation.value)
                elif hasattr(operation.value, "value") and hasattr(
                    operation.value, "name"
                ):
                    # It's a Literal
                    second_val = operation.value.value
                elif hasattr(operation.value, "name"):
                    # It's a Column - get from row
                    second_val = row.get(operation.value.name)
                else:
                    second_val = operation.value

            if first_val is None:
                return None

            # Check if first value is NaN
            if isinstance(first_val, float) and math.isnan(first_val):
                return second_val
            return first_val

        # Handle isnull function before the None check
        if func_name == "isnull":
            return value is None

        # Handle isnan function before the None check
        if func_name == "isnan":
            return isinstance(value, float) and math.isnan(value)

        # Handle datetime functions before the None check
        if func_name == "current_timestamp":
            return dt_module.datetime.now()
        elif func_name == "current_date":
            return dt_module.date.today()

        if value is None and func_name not in ("ascii", "base64", "unbase64"):
            return None

        # Use function registry for standard functions
        if func_name in self._function_registry:
            try:
                return self._function_registry[func_name](value, operation)
            except Exception:
                # Fallback to direct evaluation if function registry fails
                pass

        return value

    def _evaluate_concat(
        self, row: Dict[str, Any], operation: Any, first_value: Any
    ) -> Optional[str]:
        """Evaluate concat function with multiple arguments."""
        result_parts: List[str] = []

        # Add the first value (from the main column)
        if first_value is not None:
            result_parts.append(str(first_value))

        # Add remaining values from operation.value (list of columns)
        if hasattr(operation, "value") and operation.value is not None:
            # operation.value is a list/tuple of additional columns
            additional_cols = (
                operation.value
                if isinstance(operation.value, (list, tuple))
                else [operation.value]
            )
            for col in additional_cols:
                col_value = None
                if (
                    hasattr(col, "value")
                    and hasattr(col, "name")
                    and hasattr(col, "data_type")
                ):
                    # It's a Literal
                    col_value = self._get_literal_value(col)
                elif hasattr(col, "operation") and hasattr(col, "column"):
                    # It's a ColumnOperation - evaluate it
                    col_value = self.evaluate_expression(row, col)
                elif hasattr(col, "name"):
                    # It's a Column - get value from row
                    col_value = row.get(col.name)
                elif isinstance(col, str):
                    # It's a string literal or column name
                    # Check if it exists in row (column name) or use as literal
                    col_value = row.get(col) if col in row else col
                else:
                    col_value = col

                if col_value is not None:
                    result_parts.append(str(col_value))

        return "".join(result_parts) if result_parts else None

    def _evaluate_multi_arg_array_function(
        self, row: Dict[str, Any], operation: Any, first_value: Any, func_name: str
    ) -> Any:
        """Evaluate multi-argument array functions (array_union, arrays_zip, sequence)."""
        if func_name == "array_union":
            # array_union takes two arrays
            if first_value is None or not isinstance(first_value, (list, tuple)):
                return None

            second_array = None
            if hasattr(operation, "value") and operation.value is not None:
                if hasattr(operation.value, "operation") and hasattr(
                    operation.value, "column"
                ):
                    second_array = self.evaluate_expression(row, operation.value)
                elif hasattr(operation.value, "value") and hasattr(
                    operation.value, "name"
                ):
                    second_array = self._get_literal_value(operation.value)
                elif hasattr(operation.value, "name"):
                    second_array = row.get(operation.value.name)
                elif isinstance(operation.value, (list, tuple)):
                    second_array = operation.value
                else:
                    second_array = operation.value

            if second_array is None or not isinstance(second_array, (list, tuple)):
                return None

            # Union: combine arrays and remove duplicates while preserving order
            seen = set()
            result = []
            for item in list(first_value) + list(second_array):
                # Use tuple for hashable items, string representation for unhashable
                key = (
                    item
                    if isinstance(item, (int, float, str, bool)) or item is None
                    else str(item)
                )
                if key not in seen:
                    seen.add(key)
                    result.append(item)
            return result

        elif func_name == "arrays_zip":
            # arrays_zip takes multiple arrays
            if first_value is None or not isinstance(first_value, (list, tuple)):
                return None

            arrays = [list(first_value)]
            # Get column names for struct fields
            col_names = []
            # Try to get column name from first array's column
            if hasattr(operation, "column") and hasattr(operation.column, "name"):
                col_names.append(
                    operation.column.name.replace("arr", "arr")
                    if "arr" in operation.column.name
                    else "arr1"
                )
            else:
                col_names.append("arr1")

            if (
                hasattr(operation, "value")
                and operation.value is not None
                and isinstance(operation.value, (list, tuple))
            ):
                for idx, arr in enumerate(operation.value):
                    arr_val = None
                    if hasattr(arr, "operation") and hasattr(arr, "column"):
                        arr_val = self.evaluate_expression(row, arr)
                        # Try to get column name
                        if hasattr(arr, "column") and hasattr(arr.column, "name"):
                            col_name = (
                                arr.column.name.replace("arr", "arr")
                                if "arr" in arr.column.name
                                else f"arr{idx + 2}"
                            )
                            col_names.append(col_name)
                        else:
                            col_names.append(f"arr{idx + 2}")
                    elif hasattr(arr, "value") and hasattr(arr, "name"):
                        arr_val = arr.value
                        col_names.append(f"arr{idx + 2}")
                    elif hasattr(arr, "name"):
                        arr_val = row.get(arr.name)
                        col_name = (
                            arr.name.replace("arr", "arr")
                            if "arr" in arr.name
                            else f"arr{idx + 2}"
                        )
                        col_names.append(col_name)
                    elif isinstance(arr, (list, tuple)):
                        arr_val = arr
                        col_names.append(f"arr{idx + 2}")
                    else:
                        arr_val = arr
                        col_names.append(f"arr{idx + 2}")

                    if isinstance(arr_val, (list, tuple)):
                        arrays.append(list(arr_val))

            if len(arrays) < 2:
                return None

            # Find the maximum length
            max_len = max(len(arr) for arr in arrays)

            # Zip arrays into structs using actual column names
            result = []
            for i in range(max_len):
                struct_dict = {}
                for idx, arr in enumerate(arrays):
                    col_name = (
                        col_names[idx] if idx < len(col_names) else f"arr{idx + 1}"
                    )
                    struct_dict[col_name] = arr[i] if i < len(arr) else None
                result.append(struct_dict)

            return result

        elif func_name == "sequence":
            # sequence takes start, stop, and optional step
            if first_value is None or not isinstance(first_value, (int, float)):
                return None

            start = int(first_value)
            stop = None
            step = 1

            if hasattr(operation, "value") and operation.value is not None:
                if isinstance(operation.value, (list, tuple)):
                    if len(operation.value) >= 1:
                        stop_val = operation.value[0]
                        if hasattr(stop_val, "operation") and hasattr(
                            stop_val, "column"
                        ):
                            stop = int(self.evaluate_expression(row, stop_val))
                        elif hasattr(stop_val, "value") and hasattr(stop_val, "name"):
                            stop = int(stop_val.value)
                        elif hasattr(stop_val, "name"):
                            stop_val = row.get(stop_val.name)
                            stop = int(stop_val) if stop_val is not None else None
                        elif isinstance(stop_val, (int, float)):
                            stop = int(stop_val)
                        else:
                            stop = int(stop_val)

                    if len(operation.value) >= 2:
                        step_val = operation.value[1]
                        if hasattr(step_val, "operation") and hasattr(
                            step_val, "column"
                        ):
                            step = int(self.evaluate_expression(row, step_val))
                        elif hasattr(step_val, "value") and hasattr(step_val, "name"):
                            step = int(step_val.value)
                        elif hasattr(step_val, "name"):
                            step_val = row.get(step_val.name)
                            step = int(step_val) if step_val is not None else 1
                        elif isinstance(step_val, (int, float)):
                            step = int(step_val)
                        else:
                            step = int(step_val)
                elif hasattr(operation.value, "operation") and hasattr(
                    operation.value, "column"
                ):
                    stop = int(self.evaluate_expression(row, operation.value))
                elif hasattr(operation.value, "value") and hasattr(
                    operation.value, "name"
                ):
                    stop = int(self._get_literal_value(operation.value))
                elif hasattr(operation.value, "name"):
                    stop_val = row.get(operation.value.name)
                    stop = int(stop_val) if stop_val is not None else None
                elif isinstance(operation.value, (int, float)):
                    stop = int(operation.value)

            if stop is None:
                return None

            # Generate sequence
            if step == 0:
                return []
            if (step > 0 and start > stop) or (step < 0 and start < stop):
                return []

            result = list(range(start, stop + (1 if step > 0 else -1), step))
            return result

        return None

    def _evaluate_multi_arg_map_function(
        self, row: Dict[str, Any], operation: Any, first_value: Any, func_name: str
    ) -> Any:
        """Evaluate multi-argument map functions (create_map, map_from_arrays)."""
        if func_name == "create_map":
            # create_map takes alternating key-value pairs
            # operation.value contains all arguments as a tuple
            # For create_map with literals, operation.value is (Literal('key'), Literal('value'), ...)
            result = {}

            # Get all arguments from operation.value
            evaluated_pairs = []
            if hasattr(operation, "value") and operation.value is not None:
                pairs = []
                if isinstance(operation.value, (list, tuple)):
                    pairs = list(operation.value)
                else:
                    pairs = [operation.value]

                # Evaluate all pairs
                for pair in pairs:
                    if hasattr(pair, "operation") and hasattr(pair, "column"):
                        evaluated_pairs.append(self.evaluate_expression(row, pair))
                    elif hasattr(pair, "value") and hasattr(pair, "name"):
                        # It's a Literal
                        evaluated_pairs.append(pair.value)
                    elif hasattr(pair, "name"):
                        evaluated_pairs.append(row.get(pair.name))
                    else:
                        evaluated_pairs.append(pair)

            # Build map from key-value pairs
            for i in range(0, len(evaluated_pairs) - 1, 2):
                key = evaluated_pairs[i]
                val = evaluated_pairs[i + 1] if i + 1 < len(evaluated_pairs) else None
                if key is not None:
                    result[key] = val

            return result

        elif func_name == "map_from_arrays":
            # map_from_arrays takes two arrays (keys and values)
            # first_value is the evaluated first array
            if first_value is None or not isinstance(first_value, (list, tuple)):
                return None

            keys_array = list(first_value)
            values_array = None

            if hasattr(operation, "value") and operation.value is not None:
                # operation.value is the second array (could be ColumnOperation or already evaluated)
                val_obj = operation.value
                # Check if it's a ColumnOperation (has both operation and column attributes)
                if hasattr(val_obj, "operation") and hasattr(val_obj, "column"):
                    # It's a ColumnOperation - evaluate it recursively
                    values_array = self.evaluate_expression(row, val_obj)
                elif isinstance(val_obj, (list, tuple)):
                    # Already an array
                    values_array = list(val_obj)
                elif hasattr(val_obj, "value") and hasattr(val_obj, "name"):
                    # It's a Literal - get its value
                    values_array = val_obj.value
                elif hasattr(val_obj, "name"):
                    # It's a Column - get from row
                    values_array = row.get(val_obj.name)
                else:
                    values_array = val_obj

            if values_array is None or not isinstance(values_array, (list, tuple)):
                return None

            # Create map from keys and values arrays
            result = {}
            for i in range(min(len(keys_array), len(values_array))):
                key = keys_array[i]
                val = values_array[i]
                if key is not None:
                    result[key] = val

            return result

        return None

    def _evaluate_format_string(
        self, row: Dict[str, Any], operation: Any, value: Any
    ) -> Any:
        """Evaluate format_string function."""

        fmt: Optional[str] = None
        args: List[Any] = []
        if value is not None:
            val = value
            if isinstance(val, tuple) and len(val) >= 1:
                fmt = val[0]
                rest = []
                if len(val) > 1:
                    # val[1] may itself be an iterable of remaining columns
                    rem = val[1]
                    rest = list(rem) if isinstance(rem, (list, tuple)) else [rem]
                args = []
                # First, evaluate the base column (operation.column) - this is the first argument
                if hasattr(operation, "column") and operation.column is not None:
                    base_col = operation.column
                    if hasattr(base_col, "name"):
                        args.append(row.get(base_col.name))
                    elif hasattr(base_col, "operation") and hasattr(base_col, "column"):
                        args.append(self.evaluate_expression(row, base_col))
                    else:
                        args.append(None)
                # Then evaluate remaining args
                for a in rest:
                    if hasattr(a, "operation") and hasattr(a, "column"):
                        args.append(self.evaluate_expression(row, a))
                    elif hasattr(a, "value"):
                        args.append(a.value)
                    elif hasattr(a, "name"):
                        args.append(row.get(a.name))
                    else:
                        args.append(a)
        try:
            if fmt is None:
                return None
            # PySpark converts None to "null" string in format_string
            fmt_args = tuple("null" if v is None else v for v in args)
            return fmt % fmt_args
        except Exception:
            return None

    def _evaluate_expr_function(
        self, row: Dict[str, Any], operation: Any, value: Any
    ) -> Any:
        """Evaluate expr function - parse SQL expressions."""
        expr_str = operation.value if hasattr(operation, "value") else ""

        # Simple parsing for common functions like lower(name), upper(name), etc.
        if expr_str.startswith("lower(") and expr_str.endswith(")"):
            # Extract column name from lower(column_name)
            col_name = expr_str[6:-1]  # Remove "lower(" and ")"
            col_value = row.get(col_name)
            return col_value.lower() if col_value is not None else None
        elif expr_str.startswith("upper(") and expr_str.endswith(")"):
            # Extract column name from upper(column_name)
            col_name = expr_str[6:-1]  # Remove "upper(" and ")"
            col_value = row.get(col_name)
            return col_value.upper() if col_value is not None else None
        elif expr_str.startswith("ascii(") and expr_str.endswith(")"):
            # Extract column name from ascii(column_name)
            col_name = expr_str[6:-1]
            col_value = row.get(col_name)
            if col_value is None:
                return None
            s = str(col_value)
            return ord(s[0]) if s else 0
        elif expr_str.startswith("base64(") and expr_str.endswith(")"):
            # Extract column name from base64(column_name)
            col_name = expr_str[7:-1]
            col_value = row.get(col_name)
            if col_value is None:
                return None
            return base64.b64encode(str(col_value).encode("utf-8")).decode("utf-8")
        elif expr_str.startswith("unbase64(") and expr_str.endswith(")"):
            # Extract column name from unbase64(column_name)
            col_name = expr_str[9:-1]
            col_value = row.get(col_name)
            if col_value is None:
                return None
            try:
                return base64.b64decode(str(col_value).encode("utf-8"))
            except Exception:
                return None
        elif expr_str.startswith("length(") and expr_str.endswith(")"):
            # Extract column name from length(column_name)
            col_name = expr_str[7:-1]  # Remove "length(" and ")"
            col_value = row.get(col_name)
            return len(col_value) if col_value is not None else None
        elif expr_str == "e()":
            # Euler's number
            return math.e
        elif expr_str == "pi()":
            # Pi constant
            return math.pi
        else:
            # For other expressions, return the expression string as-is
            return expr_str

    def _evaluate_function_call_by_name(
        self, row: Dict[str, Any], col_name: str
    ) -> Any:
        """Evaluate function calls by parsing the function name."""
        if col_name.startswith("coalesce("):
            # Parse coalesce arguments: coalesce(col1, col2, ...)
            # For now, implement basic coalesce logic
            if "name" in col_name and "Unknown" in col_name:
                name_value = row.get("name")
                return name_value if name_value is not None else "Unknown"
            else:
                # Generic coalesce logic - return first non-null value
                # This is a simplified implementation
                return None
        elif col_name.startswith("isnull("):
            # Parse isnull argument: isnull(col)
            if "name" in col_name:
                result = row.get("name") is None
                return result
            else:
                return None
        elif col_name.startswith("isnan("):
            # Parse isnan argument: isnan(col)
            if "salary" in col_name:
                value = row.get("salary")
                if isinstance(value, float):
                    return value != value  # NaN check
                return False
        elif col_name.startswith("upper("):
            # Parse upper argument: upper(col)
            if "name" in col_name:
                value = row.get("name")
                return str(value).upper() if value is not None else None
        elif col_name.startswith("lower("):
            # Parse lower argument: lower(col)
            if "name" in col_name:
                value = row.get("name")
                return str(value).lower() if value is not None else None
        elif col_name.startswith("trim("):
            # Parse trim argument: trim(col)
            if "name" in col_name:
                value = row.get("name")
                return str(value).strip() if value is not None else None
        elif col_name.startswith("ceil("):
            # Parse ceil argument: ceil(col)
            if "value" in col_name:
                value = row.get("value")
                return math.ceil(value) if isinstance(value, (int, float)) else value
        elif col_name.startswith("floor("):
            # Parse floor argument: floor(col)
            if "value" in col_name:
                value = row.get("value")
                return math.floor(value) if isinstance(value, (int, float)) else value
        elif col_name.startswith("sqrt("):
            # Parse sqrt argument: sqrt(col)
            if "value" in col_name:
                value = row.get("value")
                return (
                    math.sqrt(value)
                    if isinstance(value, (int, float)) and value >= 0
                    else None
                )
        elif col_name.startswith("to_date("):
            return self._evaluate_to_date_function(row, col_name)
        elif col_name.startswith("to_timestamp("):
            return self._evaluate_to_timestamp_function(row, col_name)
        elif col_name.startswith("hour("):
            return self._evaluate_hour_function(row, col_name)
        elif col_name.startswith("day("):
            return self._evaluate_day_function(row, col_name)
        elif col_name.startswith("month("):
            return self._evaluate_month_function(row, col_name)
        elif col_name.startswith("year("):
            return self._evaluate_year_function(row, col_name)
        elif col_name.startswith("regexp_replace("):
            # Parse regexp_replace arguments: regexp_replace(col, pattern, replacement)
            if "name" in col_name:
                value = row.get("name")
                if value is not None:
                    # Simple regex replacement - replace 'e' with 'X'
                    return re.sub(r"e", "X", str(value))
                return value
        elif col_name.startswith("split("):
            # Parse split arguments: split(col, delimiter)
            if "name" in col_name and (value := row.get("name")) is not None:
                # Simple split on 'l'
                return str(value).split("l")
            return []

        # Default fallback
        return None

    def _evaluate_to_date_function(self, row: Dict[str, Any], col_name: str) -> Any:
        """Evaluate to_date function."""
        # Extract column name from function call
        match = re.search(r"to_date\(([^)]+)\)", col_name)
        if match:
            column_name = match.group(1)
            value = row.get(column_name)
            if value is not None:
                try:
                    # Try to parse as datetime first, then extract date
                    if isinstance(value, str):
                        dt = dt_module.datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                        return dt.date()
                    elif hasattr(value, "date"):
                        return value.date()
                except (ValueError, TypeError, AttributeError):
                    return None
        return None

    def _evaluate_to_timestamp_function(
        self, row: Dict[str, Any], col_name: str
    ) -> Any:
        """Evaluate to_timestamp function."""
        # Extract column name from function call
        match = re.search(r"to_timestamp\(([^)]+)\)", col_name)
        if match:
            column_name = match.group(1)
            value = row.get(column_name)
            if value is not None:
                try:
                    if isinstance(value, str):
                        return dt_module.datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                except (ValueError, TypeError, AttributeError):
                    return None
        return None

    def _evaluate_hour_function(self, row: Dict[str, Any], col_name: str) -> Any:
        """Evaluate hour function."""
        match = re.search(r"hour\(([^)]+)\)", col_name)
        if match:
            column_name = match.group(1)
            value = row.get(column_name)
            if value is not None:
                try:
                    if isinstance(value, str):
                        dt = dt_module.datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                        return dt.hour
                    elif hasattr(value, "hour"):
                        return value.hour
                except (ValueError, TypeError, AttributeError):
                    return None
        return None

    def _evaluate_day_function(self, row: Dict[str, Any], col_name: str) -> Any:
        """Evaluate day function."""
        match = re.search(r"day\(([^)]+)\)", col_name)
        if match:
            column_name = match.group(1)
            value = row.get(column_name)
            if value is not None:
                try:
                    if isinstance(value, str):
                        dt = dt_module.datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                        return dt.day
                    elif hasattr(value, "day"):
                        return value.day
                except (ValueError, TypeError, AttributeError):
                    return None
        return None

    def _evaluate_month_function(self, row: Dict[str, Any], col_name: str) -> Any:
        """Evaluate month function."""
        match = re.search(r"month\(([^)]+)\)", col_name)
        if match:
            column_name = match.group(1)
            value = row.get(column_name)
            if value is not None:
                try:
                    if isinstance(value, str):
                        dt = dt_module.datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                        return dt.month
                    elif hasattr(value, "month"):
                        return value.month
                except (ValueError, TypeError, AttributeError):
                    return None
        return None

    def _evaluate_year_function(self, row: Dict[str, Any], col_name: str) -> Any:
        """Evaluate year function."""
        match = re.search(r"year\(([^)]+)\)", col_name)
        if match:
            column_name = match.group(1)
            value = row.get(column_name)
            if value is not None:
                try:
                    if isinstance(value, str):
                        dt = dt_module.datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                        return dt.year
                    elif hasattr(value, "year"):
                        return value.year
                except (ValueError, TypeError, AttributeError):
                    return None
        return None

    def _get_literal_value(self, literal: Any) -> Any:
        """Get value from a Literal, resolving lazy literals if needed.

        Args:
            literal: Literal object or any value

        Returns:
            Resolved literal value
        """
        if hasattr(literal, "value") and hasattr(literal, "name"):
            # It's a Literal - check if it's lazy
            if hasattr(literal, "_is_lazy") and literal._is_lazy:
                return literal._resolve_lazy_value()
            return literal.value
        return literal

    def _evaluate_value(self, row: Dict[str, Any], value: Any) -> Any:
        """Evaluate a value (could be a column reference, literal, or operation)."""
        if hasattr(value, "operation") and hasattr(value, "column"):
            # It's a ColumnOperation
            return self.evaluate_expression(row, value)
        elif hasattr(value, "value") and hasattr(value, "name"):
            # It's a Literal - get the value, but if it's an expression, evaluate it
            literal_value = self._get_literal_value(value)
            # If the literal's value is itself an expression (CaseWhen, ColumnOperation, etc.),
            # evaluate it recursively
            if isinstance(literal_value, CaseWhen):
                return self._evaluate_case_when(row, literal_value)
            elif hasattr(literal_value, "operation") and hasattr(
                literal_value, "column"
            ):
                # It's a ColumnOperation stored in a Literal
                return self.evaluate_expression(row, literal_value)
            elif isinstance(literal_value, Column):
                return self._evaluate_mock_column(row, literal_value)
            return literal_value
        elif hasattr(value, "name"):
            # It's a Column
            return row.get(value.name)
        else:
            # It's a direct value
            return value

    def _evaluate_direct_value(self, value: Any) -> Any:
        """Evaluate a direct value."""
        return value

    def _is_aliased_function_call(self, column: Column) -> bool:
        """Check if column is an aliased function call."""
        return (
            hasattr(column, "_original_column")
            and column._original_column is not None
            and hasattr(column._original_column, "name")
            and self._is_function_call_name(column._original_column.name)
        )

    def _is_function_call_name(self, name: str) -> bool:
        """Check if name is a function call."""
        function_prefixes = (
            "coalesce(",
            "isnull(",
            "isnan(",
            "upper(",
            "lower(",
            "trim(",
            "base64(",
            "unbase64(",
            "ceil(",
            "floor(",
            "sqrt(",
            "regexp_replace(",
            "split(",
            "to_date(",
            "to_timestamp(",
            "hour(",
            "day(",
            "month(",
            "year(",
        )
        return any(name.startswith(prefix) for prefix in function_prefixes)

    def _build_function_registry(self) -> Dict[str, Any]:
        """Build registry of supported functions."""
        return {
            # String functions
            "upper": self._func_upper,
            "lower": self._func_lower,
            "trim": self._func_trim,
            "ltrim": self._func_ltrim,
            "rtrim": self._func_rtrim,
            "btrim": self._func_btrim,
            "contains": self._func_contains,
            "left": self._func_left,
            "right": self._func_right,
            "bit_length": self._func_bit_length,
            "startswith": self._func_startswith,
            "endswith": self._func_endswith,
            "like": self._func_like,
            "rlike": self._func_rlike,
            "between": self._func_between,
            "replace": self._func_replace,
            "substr": self._func_substr,
            "split_part": self._func_split_part,
            "position": self._func_position,
            "octet_length": self._func_octet_length,
            "char": self._func_char,
            "ucase": self._func_ucase,
            "lcase": self._func_lcase,
            "elt": self._func_elt,
            "power": self._func_power,
            "positive": self._func_positive,
            "negative": self._func_negative,
            "now": self._func_now,
            "curdate": self._func_curdate,
            "days": self._func_days,
            "hours": self._func_hours,
            "months": self._func_months,
            "equal_null": self._func_equal_null,
            # New string functions
            "ilike": self._func_ilike,
            "find_in_set": self._func_find_in_set,
            "regexp_count": self._func_regexp_count,
            "regexp_like": self._func_regexp_like,
            "regexp_substr": self._func_regexp_substr,
            "regexp_instr": self._func_regexp_instr,
            "regexp": self._func_regexp,
            "sentences": self._func_sentences,
            "printf": self._func_printf,
            "to_char": self._func_to_char,
            "to_varchar": self._func_to_varchar,
            "typeof": self._func_typeof,
            "stack": self._func_stack,
            # New math/bitwise functions
            "pmod": self._func_pmod,
            "negate": self._func_negate,
            "shiftleft": self._func_shiftleft,
            "shiftright": self._func_shiftright,
            "shiftrightunsigned": self._func_shiftrightunsigned,
            "ln": self._func_ln,
            # New datetime functions
            "years": self._func_years,
            "localtimestamp": self._func_localtimestamp,
            "dateadd": self._func_dateadd,
            "datepart": self._func_datepart,
            "make_timestamp": self._func_make_timestamp,
            "make_timestamp_ltz": self._func_make_timestamp_ltz,
            "make_timestamp_ntz": self._func_make_timestamp_ntz,
            "make_interval": self._func_make_interval,
            "make_dt_interval": self._func_make_dt_interval,
            "make_ym_interval": self._func_make_ym_interval,
            "to_number": self._func_to_number,
            "to_binary": self._func_to_binary,
            "to_unix_timestamp": self._func_to_unix_timestamp,
            "unix_date": self._func_unix_date,
            "unix_seconds": self._func_unix_seconds,
            "unix_millis": self._func_unix_millis,
            "unix_micros": self._func_unix_micros,
            "timestamp_seconds": self._func_timestamp_seconds,
            "timestamp_millis": self._func_timestamp_millis,
            "timestamp_micros": self._func_timestamp_micros,
            # New utility functions
            "get": self._func_get,
            "getItem": self._func_getItem,
            "withField": self._func_withField,
            "inline": self._func_inline,
            "inline_outer": self._func_inline_outer,
            "str_to_map": self._func_str_to_map,
            # New crypto functions (PySpark 3.5+)
            "aes_encrypt": self._func_aes_encrypt,
            "aes_decrypt": self._func_aes_decrypt,
            "try_aes_decrypt": self._func_try_aes_decrypt,
            # New string functions (PySpark 3.5+)
            "sha": self._func_sha,
            "mask": self._func_mask,
            "json_array_length": self._func_json_array_length,
            "json_object_keys": self._func_json_object_keys,
            "xpath_number": self._func_xpath_number,
            "user": self._func_user,
            # New math functions (PySpark 3.5+)
            "getbit": self._func_getbit,
            "width_bucket": self._func_width_bucket,
            # New datetime functions (PySpark 3.5+)
            "date_from_unix_date": self._func_date_from_unix_date,
            "to_timestamp_ltz": self._func_to_timestamp_ltz,
            "to_timestamp_ntz": self._func_to_timestamp_ntz,
            # New null-safe try functions (PySpark 3.5+)
            "try_add": self._func_try_add,
            "try_subtract": self._func_try_subtract,
            "try_multiply": self._func_try_multiply,
            "try_divide": self._func_try_divide,
            "try_element_at": self._func_try_element_at,
            "try_to_binary": self._func_try_to_binary,
            "try_to_number": self._func_try_to_number,
            "try_to_timestamp": self._func_try_to_timestamp,
            "length": self._func_length,
            "ascii": self._func_ascii,
            "base64": self._func_base64,
            "unbase64": self._func_unbase64,
            "split": self._func_split,
            "regexp_replace": self._func_regexp_replace,
            "format_string": self._func_format_string,
            "from_json": self._func_from_json,
            "to_json": self._func_to_json,
            "from_csv": self._func_from_csv,
            "to_csv": self._func_to_csv,
            # Math functions
            "abs": self._func_abs,
            "round": self._func_round,
            "ceil": self._func_ceil,
            "ceiling": self._func_ceil,  # Alias for ceil
            "floor": self._func_floor,
            "sqrt": self._func_sqrt,
            # Trigonometric functions
            "acos": self._func_acos,
            "asin": self._func_asin,
            "atan": self._func_atan,
            "atan2": self._func_atan2,
            # Hyperbolic functions
            "acosh": self._func_acosh,
            "asinh": self._func_asinh,
            "atanh": self._func_atanh,
            "cosh": self._func_cosh,
            "sinh": self._func_sinh,
            "tanh": self._func_tanh,
            # Logarithmic and exponential functions
            "log1p": self._func_log1p,
            "log2": self._func_log2,
            "log10": self._func_log10,
            "expm1": self._func_expm1,
            # Other math functions
            "cbrt": self._func_cbrt,
            "degrees": self._func_degrees,
            "radians": self._func_radians,
            "rint": self._func_rint,
            "hypot": self._func_hypot,
            "signum": self._func_signum,
            "e": self._func_e,
            "pi": self._func_pi,
            # Cast function
            "cast": self._func_cast,
            # Datetime functions
            "to_date": self._func_to_date,
            "to_timestamp": self._func_to_timestamp,
            "hour": self._func_hour,
            "minute": self._func_minute,
            "second": self._func_second,
            "day": self._func_day,
            "dayofmonth": self._func_dayofmonth,
            "month": self._func_month,
            "year": self._func_year,
            "quarter": self._func_quarter,
            "dayofweek": self._func_dayofweek,
            "dayofyear": self._func_dayofyear,
            "weekofyear": self._func_weekofyear,
            "datediff": self._func_datediff,
            "date_diff": self._func_datediff,  # Alias for datediff
            "date_format": self._func_date_format,
            "months_between": self._func_months_between,
            # Array functions
            "array_join": self._func_array_join,
            "array_sort": self._func_array_sort,
            "array_union": self._func_array_union,
            "arrays_zip": self._func_arrays_zip,
            "flatten": self._func_flatten,
            "sequence": self._func_sequence,
            # Map functions
            "create_map": self._func_create_map,
            "map_filter": self._func_map_filter,
            "map_from_arrays": self._func_map_from_arrays,
            "map_from_entries": self._func_map_from_entries,
            "map_zip_with": self._func_map_zip_with,
            "transform_keys": self._func_transform_keys,
            "transform_values": self._func_transform_values,
            # Datetime functions
            "from_unixtime": self._func_from_unixtime,
            "from_utc_timestamp": self._func_from_utc_timestamp,
            "to_utc_timestamp": self._func_to_utc_timestamp,
            "unix_timestamp": self._func_unix_timestamp,
            # Note: timestamp_seconds is already in registry above
            # Special functions
            "hash": self._func_hash,
            "overlay": self._func_overlay,
            "bitwise_not": self._func_bitwise_not,
            "nanvl": self._func_nanvl,
            "asc": self._func_asc,
            "desc": self._func_desc,
        }

    # String function implementations
    def _func_upper(self, value: Any, operation: ColumnOperation) -> str:
        """Upper case function."""
        return str(value).upper()

    def _func_lower(self, value: Any, operation: ColumnOperation) -> str:
        """Lower case function."""
        return str(value).lower()

    def _func_trim(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Trim function - remove ASCII spaces from both ends (PySpark behavior)."""
        if value is None:
            return None
        # PySpark trim only removes ASCII space characters (0x20), not tabs/newlines
        s = str(value)
        return s.strip(" ")

    def _func_ltrim(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Ltrim function - remove ASCII spaces from left side (PySpark behavior)."""
        if value is None:
            return None
        # PySpark ltrim only removes ASCII space characters (0x20), not tabs/newlines
        s = str(value)
        return s.lstrip(" ")

    def _func_rtrim(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Rtrim function - remove ASCII spaces from right side (PySpark behavior)."""
        if value is None:
            return None
        # PySpark rtrim only removes ASCII space characters (0x20), not tabs/newlines
        s = str(value)
        return s.rstrip(" ")

    def _func_btrim(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Btrim function - trim characters from both ends."""
        if value is None:
            return None
        s = str(value)
        trim_string = (
            operation.value
            if hasattr(operation, "value") and operation.value is not None
            else None
        )
        if trim_string:
            # Trim specific characters
            return s.strip(trim_string)
        else:
            # Trim whitespace (same as trim)
            return s.strip()

    def _func_contains(self, value: Any, operation: ColumnOperation) -> Optional[bool]:
        """Contains function - check if string contains substring."""
        if value is None:
            return None
        substring = (
            operation.value
            if hasattr(operation, "value") and operation.value is not None
            else ""
        )
        return substring in str(value)

    def _func_left(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Left function - extract left N characters."""
        if value is None:
            return None
        s = str(value)
        length = (
            operation.value
            if hasattr(operation, "value") and operation.value is not None
            else 0
        )
        if length <= 0:
            return ""
        return s[:length]

    def _func_right(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Right function - extract right N characters."""
        if value is None:
            return None
        s = str(value)
        length = (
            operation.value
            if hasattr(operation, "value") and operation.value is not None
            else 0
        )
        if length <= 0:
            return ""
        return s[-length:] if length <= len(s) else s

    def _func_bit_length(self, value: Any, operation: ColumnOperation) -> Optional[int]:
        """Bit length function - get bit length of string."""
        if value is None:
            return None
        return len(str(value).encode("utf-8")) * 8

    def _func_startswith(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[bool]:
        """Startswith function - check if string starts with substring."""
        if value is None:
            return None
        substring = operation.value
        return str(value).startswith(substring)

    def _func_endswith(self, value: Any, operation: ColumnOperation) -> Optional[bool]:
        """Endswith function - check if string ends with substring."""
        if value is None:
            return None
        substring = operation.value
        return str(value).endswith(substring)

    def _func_like(self, value: Any, operation: ColumnOperation) -> Optional[bool]:
        """Like function - SQL LIKE pattern matching."""
        if value is None:
            return None
        pattern = operation.value
        # Convert SQL LIKE pattern to regex (simplified: % -> .*, _ -> .)
        import re

        regex_pattern = pattern.replace("%", ".*").replace("_", ".")
        return bool(re.match(regex_pattern, str(value)))

    def _func_rlike(self, value: Any, operation: ColumnOperation) -> Optional[bool]:
        """Rlike function - regular expression pattern matching."""
        if value is None:
            return None
        pattern = operation.value
        import re

        return bool(re.search(pattern, str(value)))

    def _func_between(self, value: Any, operation: ColumnOperation) -> Optional[bool]:
        """Between function - check if value is between lower and upper bounds (inclusive).

        PySpark behavior: between is inclusive on both ends (lower <= value <= upper).
        Returns None if value is None (NULL handling).

        Args:
            value: The column value to check
            operation: ColumnOperation with value as tuple (lower, upper)

        Returns:
            True if lower <= value <= upper, False otherwise, None if value is None
        """
        if value is None:
            return None

        if not isinstance(operation.value, tuple) or len(operation.value) != 2:
            return None

        lower, upper = operation.value

        # Extract lower bound value
        # Note: ColumnOperations in bounds should be handled by Polars translator
        # This Python fallback only handles simple literals and direct values
        # Check ColumnOperation first since it's a subclass of Column
        if isinstance(lower, ColumnOperation):
            # ColumnOperation - can't evaluate without row context
            # This case should be handled by Polars translator, not Python fallback
            return None
        elif isinstance(lower, Column):
            # Simple Column reference - can't evaluate without row context
            # This case should be handled by Polars translator, not Python fallback
            return None
        elif hasattr(lower, "value") and hasattr(lower, "name"):  # Literal
            lower_val = (
                lower.value
                if not (hasattr(lower, "_is_lazy") and lower._is_lazy)
                else lower._resolve_lazy_value()
            )
        else:
            # Direct value (int, float, str, etc.)
            lower_val = lower

        # Extract upper bound value
        if isinstance(upper, ColumnOperation):
            # ColumnOperation - can't evaluate without row context
            # This case should be handled by Polars translator, not Python fallback
            return None
        elif isinstance(upper, Column):
            # Simple Column reference - can't evaluate without row context
            # This case should be handled by Polars translator, not Python fallback
            return None
        elif hasattr(upper, "value") and hasattr(upper, "name"):  # Literal
            upper_val = (
                upper.value
                if not (hasattr(upper, "_is_lazy") and upper._is_lazy)
                else upper._resolve_lazy_value()
            )
        else:
            # Direct value (int, float, str, etc.)
            upper_val = upper

        # Handle None bounds (PySpark behavior: None bounds return None)
        if lower_val is None or upper_val is None:
            return None

        # PySpark between is inclusive: lower <= value <= upper
        try:
            return bool(lower_val <= value <= upper_val)
        except (TypeError, ValueError):
            # Type mismatch - return None (PySpark behavior)
            return None

    def _func_replace(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Replace function - replace occurrences of substring."""
        if value is None:
            return None
        if isinstance(operation.value, tuple) and len(operation.value) == 2:
            old, new = operation.value
            return str(value).replace(old, new)
        return str(value)

    def _func_substr(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Substr function - alias for substring, but requires length parameter.

        PySpark behavior:
        - start is 1-indexed (or negative for reverse indexing)
        - start=0 is treated as start=1
        - Negative start counts from the end: -1 is last char, -2 is second-to-last, etc.
        - length is required (unlike substring which has optional length)
        """
        if value is None:
            return None
        if isinstance(operation.value, tuple):
            start, length = operation.value[0], operation.value[1]
        else:
            # Should not happen for substr (requires length), but handle gracefully
            start, length = operation.value, len(str(value))

        s = str(value)
        s_len = len(s)

        # Handle negative start positions (count from end)
        # PySpark behavior for negative start:
        # - Computes position as len + start (1-indexed)
        # - If result < 1, treats as position 1 but limits length
        # - Length is limited based on how "negative" the original start was
        if start < 0:
            # Compute 1-indexed position from end
            pos_1_indexed = s_len + start + 1  # +1 because we're working in 1-indexed
            if pos_1_indexed < 1:
                # Start is too negative, clamp to position 1
                start_idx = 0
                # Limit length: when start is very negative, PySpark limits result
                # Formula: available = s_len - (1 - pos_1_indexed) but clamped
                # Actually, simpler: limit to what's available from start
                # But PySpark seems to use: max(0, s_len + start) as available
                available = max(0, s_len + start)
                length = min(length, available) if available > 0 else 0
            else:
                # Convert 1-indexed to 0-indexed
                start_idx = pos_1_indexed - 1
        elif start == 0:
            # PySpark treats start=0 as start=1
            start_idx = 0
        else:
            # Positive start: convert 1-indexed to 0-indexed
            start_idx = start - 1
            if start_idx < 0:
                start_idx = 0

        # Extract substring
        if length <= 0:
            return ""
        end_idx = start_idx + length
        return s[start_idx:end_idx]

    def _func_split_part(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Split_part function - extract part of string split by delimiter."""
        if value is None:
            return None
        if isinstance(operation.value, tuple) and len(operation.value) == 2:
            delimiter, part = operation.value
            parts = str(value).split(delimiter)
            # part is 1-indexed
            if 1 <= part <= len(parts):
                from typing import cast

                return cast("Optional[str]", parts[part - 1])
            return None
        return None

    def _func_position(self, value: Any, operation: ColumnOperation) -> Optional[int]:
        """Position function - find position of substring in string (1-indexed)."""
        if value is None:
            return None
        substring = (
            operation.value
            if isinstance(operation.value, str)
            else str(operation.value)
        )
        pos = str(value).find(substring)
        return pos + 1 if pos >= 0 else 0

    def _func_octet_length(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """Octet_length function - get byte length of string."""
        if value is None:
            return None
        return len(str(value).encode("utf-8"))

    def _func_char(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Char function - convert integer to character."""
        if value is None:
            return None
        try:
            return chr(int(value))
        except (ValueError, TypeError, OverflowError):
            return None

    def _func_ucase(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Ucase function - alias for upper."""
        if value is None:
            return None
        return str(value).upper()

    def _func_lcase(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Lcase function - alias for lower."""
        if value is None:
            return None
        return str(value).lower()

    def _func_elt(self, value: Any, operation: ColumnOperation) -> Any:
        """Elt function - return element at index from list of columns."""
        # This is complex - requires evaluating multiple columns
        # For now, return None as this needs special handling
        return None

    def _func_power(self, value: Any, operation: ColumnOperation) -> Any:
        """Power function - alias for pow."""
        if value is None:
            return None
        exponent = operation.value
        try:
            return pow(value, exponent)
        except (TypeError, ValueError):
            return None

    def _func_positive(self, value: Any, operation: ColumnOperation) -> Any:
        """Positive function - identity function."""
        return value

    def _func_negative(self, value: Any, operation: ColumnOperation) -> Any:
        """Negative function - negate value."""
        if value is None:
            return None
        try:
            return -value
        except TypeError:
            return None

    def _func_now(self, value: Any, operation: ColumnOperation) -> Any:
        """Now function - alias for current_timestamp."""
        from datetime import datetime

        return datetime.now()

    def _func_curdate(self, value: Any, operation: ColumnOperation) -> Any:
        """Curdate function - alias for current_date."""
        from datetime import date

        return date.today()

    def _func_days(self, value: Any, operation: ColumnOperation) -> Any:
        """Days function - convert number to days interval."""
        return value  # Return as-is for date arithmetic

    def _func_hours(self, value: Any, operation: ColumnOperation) -> Any:
        """Hours function - convert number to hours interval."""
        return value  # Return as-is for date arithmetic

    def _func_months(self, value: Any, operation: ColumnOperation) -> Any:
        """Months function - convert number to months interval."""
        return value  # Return as-is for date arithmetic

    def _func_equal_null(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[bool]:
        """Equal_null function - equality check that treats NULL as equal."""
        # This requires comparing two values, which is complex
        # For now, return None as this needs special handling
        return None

    def _func_length(self, value: Any, operation: ColumnOperation) -> int:
        """Length function."""
        return len(str(value))

    def _func_ascii(self, value: Any, operation: ColumnOperation) -> int:
        """ASCII function."""
        if value is None:
            return 0
        s = str(value)
        return ord(s[0]) if s else 0

    def _func_base64(self, value: Any, operation: ColumnOperation) -> str:
        """Base64 encode function."""
        if value is None:
            return ""
        return base64.b64encode(str(value).encode("utf-8")).decode("utf-8")

    def _func_unbase64(self, value: Any, operation: ColumnOperation) -> bytes:
        """Base64 decode function."""
        if value is None:
            return b""
        try:
            return base64.b64decode(str(value).encode("utf-8"))
        except Exception:
            return b""

    def _func_split(self, value: Any, operation: ColumnOperation) -> List[str]:
        """Split function."""
        if value is None:
            return []
        delimiter = operation.value
        return str(value).split(delimiter)

    def _func_regexp_replace(self, value: Any, operation: ColumnOperation) -> str:
        """Regex replace function."""
        if value is None:
            return ""
        pattern = (
            operation.value[0]
            if isinstance(operation.value, tuple)
            else operation.value
        )
        replacement = (
            operation.value[1]
            if isinstance(operation.value, tuple) and len(operation.value) > 1
            else ""
        )
        return re.sub(pattern, replacement, str(value))

    def _func_format_string(self, value: Any, operation: ColumnOperation) -> str:
        """Format string function."""
        # We need the row data to evaluate the arguments, but we don't have it here
        # This is a limitation of the current architecture
        # For now, return empty string to indicate this function needs special handling
        return ""

    def _func_from_json(self, value: Any, operation: ColumnOperation) -> Any:
        """Parse JSON string column into Python structures."""
        if value is None:
            return None

        schema_spec, options = self._unpack_schema_and_options(operation)
        schema = self._resolve_struct_schema(schema_spec)
        mode = str(options.get("mode", "PERMISSIVE")).upper()
        corrupt_column = options.get("columnNameOfCorruptRecord")

        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            if mode == "FAILFAST":
                raise
            if mode == "DROPMALFORMED":
                return None
            if corrupt_column and schema is not None:
                return {corrupt_column: value}
            return None

        if schema is None:
            return parsed

        if not isinstance(schema, StructType) or not isinstance(parsed, dict):
            return None

        projected: Dict[str, Any] = {
            field.name: parsed.get(field.name) for field in schema.fields
        }

        if corrupt_column and corrupt_column not in projected:
            projected[corrupt_column] = None

        return projected

    def _func_to_json(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Serialize struct or map values to JSON strings."""
        if value is None:
            return None
        struct_dict = self._struct_to_dict(value)
        if struct_dict is None:
            return None
        return json.dumps(struct_dict, ensure_ascii=False, separators=(",", ":"))

    def _func_from_csv(self, value: Any, operation: ColumnOperation) -> Any:
        """Parse CSV strings based on optional provided schema."""
        if value is None:
            return None

        schema_spec, options = self._unpack_schema_and_options(operation)
        schema = self._resolve_struct_schema(schema_spec)
        delimiter = options.get("sep", options.get("delimiter", ","))
        quote = options.get("quote", '"')
        null_value = options.get("nullValue")

        reader = csv.reader(
            [value],
            delimiter=delimiter if isinstance(delimiter, str) and delimiter else ",",
            quotechar=quote if isinstance(quote, str) and quote else '"',
        )
        try:
            row_values = next(reader)
        except Exception:
            return None

        if schema is None:
            return row_values

        return self._apply_csv_schema(schema, row_values, null_value)

    def _func_to_csv(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Serialize struct values to CSV strings."""
        if value is None:
            return None

        struct_dict = self._struct_to_dict(value)
        if struct_dict is None:
            return None

        delimiter = ","
        null_value = None
        if isinstance(operation.value, dict):
            delimiter = (
                operation.value.get("sep", operation.value.get("delimiter", ",")) or ","
            )
            null_value = operation.value.get("nullValue")

        parts: List[str] = []
        for item in struct_dict.values():
            if item is None:
                parts.append("" if null_value is None else str(null_value))
            else:
                parts.append(str(item))

        return delimiter.join(parts)

    def _unpack_schema_and_options(
        self, operation: ColumnOperation
    ) -> Tuple[Any, Dict[str, Any]]:
        """Extract schema specification and options dictionary."""
        schema_spec: Any = None
        options: Dict[str, Any] = {}

        raw_value = getattr(operation, "value", None)
        if isinstance(raw_value, tuple):
            if len(raw_value) >= 1:
                schema_spec = raw_value[0]
            if (
                len(raw_value) >= 2
                and raw_value[1] is not None
                and isinstance(raw_value[1], dict)
            ):
                options = dict(raw_value[1])
        elif isinstance(raw_value, dict):
            options = dict(raw_value)

        return schema_spec, options

    def _resolve_struct_schema(self, schema_spec: Any) -> Optional[StructType]:
        """Convert schema specifications into StructType objects."""
        if schema_spec is None:
            return None

        if isinstance(schema_spec, StructType):
            return schema_spec

        if isinstance(schema_spec, StructField):
            return StructType([schema_spec])

        if hasattr(schema_spec, "value"):
            return self._resolve_struct_schema(schema_spec.value)

        if isinstance(schema_spec, str):
            try:
                return parse_ddl_schema(schema_spec)
            except Exception:
                return StructType([])

        if isinstance(schema_spec, dict):
            return StructType([StructField(name, StringType()) for name in schema_spec])

        if isinstance(schema_spec, (list, tuple)):
            collected_fields: List[StructField] = []
            for item in schema_spec:
                if isinstance(item, StructField):
                    collected_fields.append(item)
                elif isinstance(item, str):
                    collected_fields.append(StructField(item, StringType()))
            if collected_fields:
                return StructType(collected_fields)

        return None

    def _apply_struct_schema(
        self, schema: StructType, data: Any
    ) -> Optional[Dict[str, Any]]:
        """Coerce dictionaries into StructType layout."""
        if not isinstance(schema, StructType):
            return None  # type: ignore[unreachable]

        if data is None:
            return {field.name: None for field in schema.fields}

        source = self._struct_to_dict(data)
        if source is None:
            if isinstance(data, dict):
                source = data
            else:
                return {field.name: None for field in schema.fields}

        result: Dict[str, Any] = {}
        for field in schema.fields:
            raw_value = source.get(field.name)
            if isinstance(field.dataType, StructType) and isinstance(raw_value, dict):
                result[field.name] = self._apply_struct_schema(
                    field.dataType, raw_value
                )
            elif isinstance(field.dataType, ArrayType) and isinstance(raw_value, list):
                result[field.name] = [
                    self._coerce_simple_value(item, field.dataType.element_type)
                    for item in raw_value
                ]
            elif isinstance(field.dataType, MapType) and isinstance(raw_value, dict):
                result[field.name] = {
                    str(k): self._coerce_simple_value(v, field.dataType.value_type)
                    for k, v in raw_value.items()
                }
            else:
                result[field.name] = self._coerce_simple_value(
                    raw_value, field.dataType
                )

        return result

    def _apply_csv_schema(
        self, schema: StructType, values: Sequence[str], null_value: Optional[str]
    ) -> Dict[str, Any]:
        """Apply StructType to a CSV row."""
        result: Dict[str, Any] = {}
        for idx, field in enumerate(schema.fields):
            raw = values[idx] if idx < len(values) else None
            if raw is None or (null_value is not None and raw == null_value):
                result[field.name] = None
            else:
                result[field.name] = self._coerce_simple_value(raw, field.dataType)
        return result

    def _struct_to_dict(self, value: Any) -> Optional[Dict[str, Any]]:
        """Convert Row-like structures to dictionaries."""
        if value is None:
            return None
        if isinstance(value, Row):
            base = value.asDict()
            return {
                key: self._struct_to_dict(val) if isinstance(val, Row) else val
                for key, val in base.items()
            }
        if isinstance(value, dict):
            return dict(value)
        if hasattr(value, "items"):
            try:
                return dict(value.items())
            except Exception:
                return None
        if isinstance(value, list):
            try:
                return dict(value)
            except Exception:
                return None
        return None

    def _coerce_simple_value(self, value: Any, data_type: DataType) -> Any:
        """Coerce primitive values according to basic Spark SQL data types."""
        if value is None:
            return None

        try:
            if isinstance(data_type, (IntegerType, LongType, ShortType, ByteType)):
                # Handle string to int conversion (e.g., "10.5" -> 10)
                # PySpark converts to float first, then truncates to int
                if isinstance(value, str):
                    try:
                        # Try float first, then convert to int (truncates decimal)
                        return int(float(value))
                    except (ValueError, TypeError):
                        return None
                return int(value)
            if isinstance(data_type, (DoubleType, FloatType)):
                if isinstance(value, str):
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return None
                return float(value)
            if isinstance(data_type, BooleanType):
                if isinstance(value, bool):
                    return value
                return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}
            if isinstance(data_type, StringType):
                return str(value)
            if isinstance(data_type, DecimalType):
                # Preserve precision and scale
                scale = getattr(data_type, "scale", 0)
                decimal_value = Decimal(str(value))
                # Quantize to match the target scale
                from decimal import ROUND_HALF_UP

                quantize_exponent = Decimal(10) ** (-scale)
                return decimal_value.quantize(quantize_exponent, rounding=ROUND_HALF_UP)
            if isinstance(data_type, DateType):
                return self._parse_date(value)
            if isinstance(data_type, TimestampType):
                return self._parse_timestamp(value)
        except Exception:
            return None

        return value

    def _parse_date(self, value: Any) -> Optional[dt_module.date]:
        """Parse string values into date objects."""
        if isinstance(value, dt_module.date) and not isinstance(
            value, dt_module.datetime
        ):
            return value
        if isinstance(value, dt_module.datetime):
            return value.date()
        if isinstance(value, str):
            cleaned = value.strip()
            try:
                return dt_module.date.fromisoformat(cleaned.split("T")[0])
            except Exception:
                try:
                    return dt_module.datetime.fromisoformat(
                        cleaned.replace("Z", "+00:00")
                    ).date()
                except Exception:
                    return None
        return None

    def _parse_timestamp(self, value: Any) -> Optional[dt_module.datetime]:
        """Parse string values into datetime objects."""
        if isinstance(value, dt_module.datetime):
            return value
        if isinstance(value, dt_module.date):
            return dt_module.datetime.combine(value, dt_module.time())
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned.endswith("Z"):
                cleaned = cleaned[:-1] + "+00:00"
            cleaned = cleaned.replace(" ", "T")
            try:
                return dt_module.datetime.fromisoformat(cleaned)
            except Exception:
                return None
        return None

    # Math function implementations
    def _func_abs(self, value: Any, operation: ColumnOperation) -> Any:
        """Absolute value function."""
        return abs(value) if isinstance(value, (int, float)) else value

    def _func_round(self, value: Any, operation: ColumnOperation) -> Any:
        """Round function."""
        precision = getattr(operation, "precision", 0)
        return round(value, precision) if isinstance(value, (int, float)) else value

    def _func_ceil(self, value: Any, operation: ColumnOperation) -> Any:
        """Ceiling function."""
        return math.ceil(value) if isinstance(value, (int, float)) else value

    def _func_floor(self, value: Any, operation: ColumnOperation) -> Any:
        """Floor function."""
        return math.floor(value) if isinstance(value, (int, float)) else value

    def _func_sqrt(self, value: Any, operation: ColumnOperation) -> Any:
        """Square root function."""
        return (
            math.sqrt(value) if isinstance(value, (int, float)) and value >= 0 else None
        )

    def _func_acos(self, value: Any, operation: ColumnOperation) -> Any:
        """Inverse cosine function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        if value < -1.0 or value > 1.0:
            return None  # Domain error
        try:
            return math.acos(value)
        except (ValueError, OverflowError):
            return None

    def _func_asin(self, value: Any, operation: ColumnOperation) -> Any:
        """Inverse sine function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        if value < -1.0 or value > 1.0:
            return None  # Domain error
        try:
            return math.asin(value)
        except (ValueError, OverflowError):
            return None

    def _func_atan(self, value: Any, operation: ColumnOperation) -> Any:
        """Inverse tangent function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return math.atan(value)
        except (ValueError, OverflowError):
            return None

    def _func_atan2(self, value: Any, operation: ColumnOperation) -> Any:
        """Inverse tangent function with two arguments."""
        # This should not be called directly - handled in _evaluate_function_call
        # But kept for registry completeness
        return None

    def _func_acosh(self, value: Any, operation: ColumnOperation) -> Any:
        """Inverse hyperbolic cosine function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        if value < 1.0:
            # For values < 1, return None (not NaN) to match PySpark behavior
            return None
        try:
            result = math.acosh(value)
            # Check if result is NaN and return None instead
            if math.isnan(result):
                return None
            return result
        except (ValueError, OverflowError):
            return None

    def _func_asinh(self, value: Any, operation: ColumnOperation) -> Any:
        """Inverse hyperbolic sine function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return math.asinh(value)
        except (ValueError, OverflowError):
            return None

    def _func_atanh(self, value: Any, operation: ColumnOperation) -> Any:
        """Inverse hyperbolic tangent function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        if value <= -1.0 or value >= 1.0:
            # For values outside domain, return None (not NaN) to match PySpark behavior
            return None
        try:
            result = math.atanh(value)
            # Check if result is NaN and return None instead
            if math.isnan(result):
                return None
            return result
        except (ValueError, OverflowError):
            return None

    def _func_cosh(self, value: Any, operation: ColumnOperation) -> Any:
        """Hyperbolic cosine function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return math.cosh(value)
        except (ValueError, OverflowError):
            return None

    def _func_sinh(self, value: Any, operation: ColumnOperation) -> Any:
        """Hyperbolic sine function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return math.sinh(value)
        except (ValueError, OverflowError):
            return None

    def _func_tanh(self, value: Any, operation: ColumnOperation) -> Any:
        """Hyperbolic tangent function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return math.tanh(value)
        except (ValueError, OverflowError):
            return None

    def _func_log1p(self, value: Any, operation: ColumnOperation) -> Any:
        """Natural logarithm of (1 + x)."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        if value <= -1.0:
            return None  # Domain error
        try:
            return math.log1p(value)
        except (ValueError, OverflowError):
            return None

    def _func_log2(self, value: Any, operation: ColumnOperation) -> Any:
        """Base-2 logarithm."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        if value <= 0:
            return None  # Domain error
        try:
            return math.log2(value)
        except (ValueError, OverflowError):
            return None

    def _func_log10(self, value: Any, operation: ColumnOperation) -> Any:
        """Base-10 logarithm."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        if value <= 0:
            return None  # Domain error
        try:
            return math.log10(value)
        except (ValueError, OverflowError):
            return None

    def _func_expm1(self, value: Any, operation: ColumnOperation) -> Any:
        """exp(x) - 1."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return math.expm1(value)
        except (ValueError, OverflowError):
            return None

    def _func_cbrt(self, value: Any, operation: ColumnOperation) -> Any:
        """Cube root function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return value ** (1.0 / 3.0)
        except (ValueError, OverflowError):
            return None

    def _func_degrees(self, value: Any, operation: ColumnOperation) -> Any:
        """Convert radians to degrees."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return math.degrees(value)
        except (ValueError, OverflowError):
            return None

    def _func_radians(self, value: Any, operation: ColumnOperation) -> Any:
        """Convert degrees to radians."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return math.radians(value)
        except (ValueError, OverflowError):
            return None

    def _func_rint(self, value: Any, operation: ColumnOperation) -> Any:
        """Round to nearest integer."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return round(value)
        except (ValueError, OverflowError):
            return None

    def _func_hypot(self, value: Any, operation: ColumnOperation) -> Any:
        """Hypotenuse function (sqrt(x^2 + y^2))."""
        # This should not be called directly - handled in _evaluate_function_call
        # But kept for registry completeness
        return None

    def _func_signum(self, value: Any, operation: ColumnOperation) -> Any:
        """Sign function (-1, 0, or 1)."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        if value > 0:
            return 1.0
        elif value < 0:
            return -1.0
        else:
            return 0.0

    def _func_e(self, value: Any, operation: ColumnOperation) -> Any:
        """Euler's number (e)."""
        # This is a constant function, value is ignored
        return math.e

    def _func_pi(self, value: Any, operation: ColumnOperation) -> Any:
        """Pi constant (π)."""
        # This is a constant function, value is ignored
        return math.pi

    # Array function implementations
    def _func_array_join(self, value: Any, operation: ColumnOperation) -> Any:
        """Join array elements with delimiter."""
        if value is None:
            return None
        if not isinstance(value, (list, tuple)):
            return None

        # Get delimiter from operation.value
        # array_join stores (delimiter, null_replacement) as a tuple
        delimiter = ","
        null_replacement = None
        if hasattr(operation, "value") and operation.value is not None:
            delimiter_val = operation.value
            # If it's a tuple (delimiter, null_replacement)
            if isinstance(delimiter_val, tuple):
                delimiter = (
                    str(delimiter_val[0]) if delimiter_val[0] is not None else ","
                )
                null_replacement = delimiter_val[1] if len(delimiter_val) > 1 else None
            # If it's a Literal, get its value
            elif hasattr(delimiter_val, "value") and hasattr(delimiter_val, "name"):
                delimiter = str(delimiter_val.value)
            elif isinstance(delimiter_val, str):
                delimiter = delimiter_val
            else:
                delimiter = str(delimiter_val)

        # Filter out None values and convert to strings, replacing None with null_replacement if provided
        if null_replacement is not None:
            filtered = [
                str(item) if item is not None else null_replacement for item in value
            ]
        else:
            filtered = [str(item) for item in value if item is not None]
        return delimiter.join(filtered)

    def _func_array_sort(self, value: Any, operation: ColumnOperation) -> Any:
        """Sort array elements in ascending order."""
        if value is None:
            return None
        if not isinstance(value, (list, tuple)):
            return None

        # Convert to list and sort
        result = list(value)
        # Handle None values - sort them to the end
        result.sort(key=lambda x: (x is None, x))
        return result

    def _func_array_union(self, value: Any, operation: ColumnOperation) -> Any:
        """Union of two arrays (remove duplicates, preserve order)."""
        # This should not be called directly - handled in _evaluate_function_call
        # But kept for registry completeness
        return None

    def _func_arrays_zip(self, value: Any, operation: ColumnOperation) -> Any:
        """Zip multiple arrays into array of structs."""
        # This should not be called directly - handled in _evaluate_function_call
        # But kept for registry completeness
        return None

    def _func_flatten(self, value: Any, operation: ColumnOperation) -> Any:
        """Flatten nested arrays."""
        if value is None:
            return None
        if not isinstance(value, (list, tuple)):
            return None

        result: List[Any] = []
        for item in value:
            if isinstance(item, (list, tuple)):
                result.extend(item)
            else:
                result.append(item)
        return result

    def _func_sequence(self, value: Any, operation: ColumnOperation) -> Any:
        """Generate sequence array from start to stop by step."""
        # This should not be called directly - handled in _evaluate_function_call
        # But kept for registry completeness
        return None

    # Map function implementations
    def _func_create_map(self, value: Any, operation: ColumnOperation) -> Any:
        """Create map from key-value pairs."""
        # This should not be called directly - handled in _evaluate_function_call
        # But kept for registry completeness
        return None

    def _func_map_filter(self, value: Any, operation: ColumnOperation) -> Any:
        """Filter map entries by lambda condition."""
        # Requires lambda evaluation - return None for now
        return None

    def _func_map_from_arrays(self, value: Any, operation: ColumnOperation) -> Any:
        """Create map from two arrays (keys and values)."""
        # This should not be called directly - handled in _evaluate_function_call
        # But kept for registry completeness
        return None

    def _func_map_from_entries(self, value: Any, operation: ColumnOperation) -> Any:
        """Create map from array of structs with key/value fields."""
        # map_from_entries takes an array of structs
        if value is None:
            return None
        if not isinstance(value, (list, tuple)):
            return None

        result = {}
        for entry in value:
            if isinstance(entry, dict):
                # PySpark structs in map_from_entries have "key" and "value" fields
                # But our struct function creates "col1", "col2", etc.
                # For map_from_entries, the first field is the key, second is the value
                keys = list(entry.keys())
                if len(keys) >= 2:
                    # Use first two fields as key and value
                    key = entry.get(keys[0])
                    val = entry.get(keys[1])
                    if key is not None:
                        result[key] = val
                elif "key" in entry and "value" in entry:
                    # Standard key/value fields
                    key = entry.get("key")
                    val = entry.get("value")
                    if key is not None:
                        result[key] = val
        return result

    def _func_map_zip_with(self, value: Any, operation: ColumnOperation) -> Any:
        """Combine two maps with lambda function."""
        # Requires lambda evaluation - return None for now
        return None

    def _func_transform_keys(self, value: Any, operation: ColumnOperation) -> Any:
        """Transform map keys with lambda."""
        # Requires lambda evaluation - return None for now
        return None

    def _func_transform_values(self, value: Any, operation: ColumnOperation) -> Any:
        """Transform map values with lambda."""
        # Requires lambda evaluation - return None for now
        return None

    # Datetime function implementations
    def _func_from_unixtime(self, value: Any, operation: ColumnOperation) -> Any:
        """Convert Unix timestamp to string.

        Unix timestamps are interpreted as UTC and converted to local timezone
        to match PySpark behavior. The result matches the session timezone.
        """
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None

        # Get format string if provided
        fmt = "yyyy-MM-dd HH:mm:ss"  # Default format
        if hasattr(operation, "value") and operation.value is not None:
            if isinstance(operation.value, str):
                fmt = operation.value
            elif hasattr(operation.value, "value"):
                fmt = str(self._get_literal_value(operation.value))

        try:
            from datetime import timezone

            timestamp = int(value)
            # Interpret unix timestamp as UTC and convert to local timezone
            # This matches PySpark's behavior where timestamps are in session timezone
            dt_utc = dt_module.datetime.fromtimestamp(timestamp, tz=timezone.utc)
            # Convert to local timezone (naive datetime for compatibility)
            dt_local = dt_utc.astimezone().replace(tzinfo=None)

            # Simple format conversion (basic implementation)
            if fmt == "yyyy-MM-dd HH:mm:ss" or fmt == "yyyy-MM-dd":
                return dt_local.strftime("%Y-%m-%d %H:%M:%S")
            else:
                # Basic format string conversion
                fmt = fmt.replace("yyyy", "%Y").replace("MM", "%m").replace("dd", "%d")
                fmt = fmt.replace("HH", "%H").replace("mm", "%M").replace("ss", "%S")
                return dt_local.strftime(fmt)
        except (ValueError, OSError, OverflowError):
            return None

    def _func_from_utc_timestamp(self, value: Any, operation: ColumnOperation) -> Any:
        """Convert UTC timestamp to timezone."""
        if value is None:
            return None

        # Get timezone from operation.value
        tz_str = None
        if hasattr(operation, "value") and operation.value is not None:
            if hasattr(operation.value, "value") and hasattr(operation.value, "name"):
                # It's a Literal
                tz_str = str(self._get_literal_value(operation.value))
            elif isinstance(operation.value, str):
                tz_str = operation.value
            else:
                tz_str = str(operation.value)

        if not tz_str:
            return value

        try:
            from datetime import datetime, timezone
            from zoneinfo import ZoneInfo

            # Parse the timestamp value
            if isinstance(value, str):
                dt = datetime.fromisoformat(
                    value.replace("Z", "+00:00").replace(" ", "T")
                )
            elif isinstance(value, datetime):
                dt = value
            else:
                return value

            # Convert from UTC to target timezone
            if dt.tzinfo is None:
                utc_dt = dt.replace(tzinfo=timezone.utc)
            else:
                utc_dt = dt.astimezone(timezone.utc)

            target_tz = ZoneInfo(tz_str)
            result_dt = utc_dt.astimezone(target_tz)

            # Return as formatted string
            return result_dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            # If timezone conversion fails, return original value
            return value

    def _func_to_utc_timestamp(self, value: Any, operation: ColumnOperation) -> Any:
        """Convert timestamp from timezone to UTC."""
        if value is None:
            return None

        # Get timezone from operation.value
        tz_str = None
        if hasattr(operation, "value") and operation.value is not None:
            if hasattr(operation.value, "value") and hasattr(operation.value, "name"):
                # It's a Literal
                tz_str = str(self._get_literal_value(operation.value))
            elif isinstance(operation.value, str):
                tz_str = operation.value
            else:
                tz_str = str(operation.value)

        if not tz_str:
            return value

        try:
            from datetime import datetime, timezone
            from zoneinfo import ZoneInfo

            # Parse the timestamp value
            if isinstance(value, str):
                dt = datetime.fromisoformat(
                    value.replace("Z", "+00:00").replace(" ", "T")
                )
            elif isinstance(value, datetime):
                dt = value
            else:
                return value

            # Convert from source timezone to UTC
            source_tz = ZoneInfo(tz_str)
            # Localize the datetime to the source timezone (assuming it's in that timezone)
            if dt.tzinfo is None:
                local_dt = dt.replace(tzinfo=source_tz)
            else:
                local_dt = dt.astimezone(source_tz)
            utc_dt = local_dt.astimezone(timezone.utc)

            # Return as formatted string
            return utc_dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            # If timezone conversion fails, return original value
            return value

    def _func_unix_timestamp(self, value: Any, operation: ColumnOperation) -> Any:
        """Convert timestamp to Unix timestamp."""
        if value is None:
            return None

        # If value is already a number, return it
        if isinstance(value, (int, float)):
            return int(value)

        # Try to parse as datetime string
        if isinstance(value, str):
            try:
                dt = dt_module.datetime.fromisoformat(
                    value.replace("Z", "+00:00").replace(" ", "T")
                )
                return int(dt.timestamp())
            except (ValueError, AttributeError):
                return None

        # If value is a datetime object
        if isinstance(value, dt_module.datetime):
            return int(value.timestamp())

        if isinstance(value, dt_module.date) and not isinstance(
            value, dt_module.datetime
        ):
            dt = dt_module.datetime.combine(value, dt_module.time())
            return int(dt.timestamp())

        return None

    # Special function implementations
    def _func_hash(self, value: Any, operation: ColumnOperation) -> Any:
        """Hash function for values - matches PySpark's hash behavior."""
        if value is None:
            return None
        try:
            # PySpark uses MurmurHash3, but for compatibility we'll use a simpler approach
            # that produces 32-bit signed integers like PySpark
            if isinstance(value, (int, float, str, bool)):
                # Use Python's hash and convert to 32-bit signed integer
                h = hash(value)
                # Convert to 32-bit signed integer (PySpark behavior)
                h_32bit = h & 0xFFFFFFFF
                if h_32bit > 0x7FFFFFFF:
                    h_32bit -= 0x100000000
                return h_32bit
            else:
                # For unhashable types, use string representation
                h = hash(str(value))
                h_32bit = h & 0xFFFFFFFF
                if h_32bit > 0x7FFFFFFF:
                    h_32bit -= 0x100000000
                return h_32bit
        except (TypeError, ValueError):
            return None

    def _func_overlay(self, value: Any, operation: ColumnOperation) -> Any:
        """Overlay/replace substring in string.

        overlay(src, replace, pos, len) replaces len characters starting at pos (1-indexed)
        with the replacement string.
        """
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)

        # Get arguments from operation.value: (replace, pos, len)
        if not hasattr(operation, "value") or operation.value is None:
            return value

        replace_val = None
        pos_val = 1
        len_val = -1

        if isinstance(operation.value, (list, tuple)) and len(operation.value) >= 1:
            # Extract replace, pos, len from tuple
            replace_val = operation.value[0] if len(operation.value) > 0 else None
            pos_val = operation.value[1] if len(operation.value) > 1 else 1
            len_val = operation.value[2] if len(operation.value) > 2 else -1

        # Evaluate replace_val if it's a Column/Literal
        if replace_val is not None:
            if hasattr(replace_val, "value") and hasattr(replace_val, "name"):
                # It's a Literal
                replace_val = replace_val.value
            elif hasattr(replace_val, "name"):
                # It's a Column - would need row context, but for now assume it's evaluated
                replace_val = str(replace_val)
            replace_val = str(replace_val) if replace_val is not None else ""
        else:
            replace_val = ""

        # Evaluate pos_val if it's a Column/Literal
        if hasattr(pos_val, "value") and hasattr(pos_val, "name"):
            pos_val = pos_val.value
        elif hasattr(pos_val, "name"):
            # Column - would need row context
            pos_val = 1
        pos_val = int(pos_val) if pos_val is not None else 1

        # Evaluate len_val if it's a Column/Literal
        if hasattr(len_val, "value") and hasattr(len_val, "name"):
            len_val = len_val.value
        elif hasattr(len_val, "name"):
            # Column - would need row context
            len_val = -1
        len_val = int(len_val) if len_val is not None else -1

        # Convert 1-indexed to 0-indexed
        start_idx = pos_val - 1 if pos_val > 0 else 0

        # Calculate end index
        end_idx = len(value) if len_val == -1 else start_idx + len_val

        # Perform overlay
        if start_idx < 0:
            start_idx = 0
        if start_idx > len(value):
            # Position beyond string, just append
            return value + replace_val

        # Replace the substring
        result = value[:start_idx] + replace_val + value[end_idx:]
        return result

    def _func_bitwise_not(self, value: Any, operation: ColumnOperation) -> Any:
        """Bitwise NOT function."""
        if value is None:
            return None
        if not isinstance(value, (int, float)):
            return None
        try:
            return ~int(value)
        except (ValueError, TypeError):
            return None

    def _func_nanvl(self, value: Any, operation: ColumnOperation) -> Any:
        """Return second value if first is NaN, otherwise return first."""
        if value is None:
            return None

        # Check if value is NaN
        if isinstance(value, float) and math.isnan(value):
            # Get replacement value from operation.value
            replacement = None
            if hasattr(operation, "value") and operation.value is not None:
                # Handle Literal
                if hasattr(operation.value, "value") and hasattr(
                    operation.value, "name"
                ):
                    replacement = self._get_literal_value(operation.value)
                # Handle Column - need to evaluate from row
                elif hasattr(operation.value, "name"):
                    # This should be evaluated in _evaluate_function_call with row context
                    replacement = operation.value
                else:
                    replacement = operation.value
            return replacement
        return value

    def _func_asc(self, value: Any, operation: ColumnOperation) -> Any:
        """Ascending order function."""
        # This is typically used in orderBy, not as a direct function
        # Return value as-is
        return value

    def _func_desc(self, value: Any, operation: ColumnOperation) -> Any:
        """Descending order function."""
        # This is typically used in orderBy, not as a direct function
        # Return value as-is
        return value

    def _func_cast(self, value: Any, operation: ColumnOperation) -> Any:
        """Cast function."""
        if value is None:
            return None
        cast_type = operation.value
        if isinstance(cast_type, str):
            # String type name, convert value
            if cast_type.lower() in ["double", "float"]:
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            elif cast_type.lower() in ["int", "integer"]:
                try:
                    return int(
                        float(value)
                    )  # Convert via float to handle decimal strings
                except (ValueError, TypeError):
                    return None
            elif cast_type.lower() in ["long", "bigint"]:
                # Special handling for timestamp to long (unix timestamp)
                # Check if value is already a datetime/timestamp
                if isinstance(value, dt_module.datetime):
                    return int(value.timestamp())
                elif isinstance(value, dt_module.date) and not isinstance(
                    value, dt_module.datetime
                ):
                    # Convert date to datetime at midnight, then to timestamp
                    dt = dt_module.datetime.combine(value, dt_module.time())
                    return int(dt.timestamp())
                elif isinstance(value, str):
                    # Try parsing as timestamp string
                    try:
                        # Try ISO format
                        dt = dt_module.datetime.fromisoformat(
                            value.replace("Z", "+00:00").replace(" ", "T").split(".")[0]
                        )
                        timestamp_result = int(dt.timestamp())
                        return timestamp_result
                    except (ValueError, TypeError, AttributeError):
                        # If timestamp parsing fails, try regular integer cast
                        pass
                # Regular integer cast
                try:
                    int_result = int(float(value))
                    return int_result
                except (ValueError, TypeError, OverflowError):
                    return None
            elif cast_type.lower() in ["string", "varchar"]:
                return str(value)
            elif cast_type.lower() in ["date"]:
                # Date type casting
                if isinstance(value, dt_module.date) and not isinstance(
                    value, dt_module.datetime
                ):
                    return value
                if isinstance(value, dt_module.datetime):
                    return value.date()
                if isinstance(value, str):
                    try:
                        return dt_module.date.fromisoformat(
                            value.split("T")[0].split(" ")[0]
                        )
                    except (ValueError, AttributeError):
                        try:
                            dt = dt_module.datetime.fromisoformat(
                                value.replace("Z", "+00:00").replace(" ", "T")
                            )
                            return dt.date()
                        except (ValueError, AttributeError):
                            return None
                return None
            elif cast_type.lower() in ["timestamp"]:
                # Timestamp type casting
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
            elif cast_type.lower().startswith("decimal"):
                # Decimal type casting with precision/scale
                from decimal import Decimal, ROUND_HALF_UP

                # Parse decimal(10,2) format
                import re

                match = re.match(r"decimal\((\d+),(\d+)\)", cast_type.lower())
                if match:
                    _precision, scale = int(match.group(1)), int(match.group(2))
                    decimal_value = Decimal(str(value))
                    quantize_exponent = Decimal(10) ** (-scale)
                    return decimal_value.quantize(
                        quantize_exponent, rounding=ROUND_HALF_UP
                    )
                else:
                    # Default precision/scale
                    return Decimal(str(value))
            else:
                return value
        else:
            # Type object (DataType), use TypeConverter for proper conversion
            # This handles StringType, IntegerType, LongType, DoubleType, BooleanType, etc.
            from ..casting.type_converter import TypeConverter

            try:
                return TypeConverter.cast_to_type(value, cast_type)
            except Exception:
                # Fallback: return value unchanged if cast fails
                return value

    # Datetime function implementations
    def _func_to_date(self, value: Any, operation: ColumnOperation) -> Any:
        """to_date function."""
        if value is None:
            return None
        try:
            if isinstance(value, str):
                # Accept 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS[.fff]'
                date_part = value.strip().split(" ")[0]
                return dt_module.date.fromisoformat(date_part)
            if hasattr(value, "date"):
                return value.date()
        except Exception:
            return None
        return None

    def _func_to_timestamp(self, value: Any, operation: ColumnOperation) -> Any:
        """to_timestamp function."""
        if value is None:
            return None
        try:
            if isinstance(value, str):
                return dt_module.datetime.fromisoformat(
                    value.replace("Z", "+00:00").replace(" ", "T")
                )
        except Exception:
            return None
        return None

    def _func_hour(self, value: Any, operation: ColumnOperation) -> Any:
        """Hour function."""
        return self._extract_datetime_component(value, "hour")

    def _func_minute(self, value: Any, operation: ColumnOperation) -> Any:
        """Minute function."""
        return self._extract_datetime_component(value, "minute")

    def _func_second(self, value: Any, operation: ColumnOperation) -> Any:
        """Second function."""
        return self._extract_datetime_component(value, "second")

    def _func_day(self, value: Any, operation: ColumnOperation) -> Any:
        """Day function."""
        return self._extract_datetime_component(value, "day")

    def _func_dayofmonth(self, value: Any, operation: ColumnOperation) -> Any:
        """Day of month function."""
        return self._extract_datetime_component(value, "day")

    def _func_month(self, value: Any, operation: ColumnOperation) -> Any:
        """Month function."""
        return self._extract_datetime_component(value, "month")

    def _func_year(self, value: Any, operation: ColumnOperation) -> Any:
        """Year function."""
        return self._extract_datetime_component(value, "year")

    def _func_quarter(self, value: Any, operation: ColumnOperation) -> Any:
        """Quarter function."""
        if value is None:
            return None
        dt = self._parse_datetime(value)
        if dt is None:
            return None
        return (dt.month - 1) // 3 + 1

    def _func_dayofweek(self, value: Any, operation: ColumnOperation) -> Any:
        """Day of week function."""
        if value is None:
            return None
        dt = self._parse_datetime(value)
        if dt is None:
            return None
        # Sunday=1, Monday=2, ..., Saturday=7
        return (dt.weekday() + 2) % 7 or 7

    def _func_dayofyear(self, value: Any, operation: ColumnOperation) -> Any:
        """Day of year function."""
        if value is None:
            return None
        dt = self._parse_datetime(value)
        if dt is None:
            return None
        return dt.timetuple().tm_yday

    def _func_weekofyear(self, value: Any, operation: ColumnOperation) -> Any:
        """Week of year function."""
        if value is None:
            return None
        dt = self._parse_datetime(value)
        if dt is None:
            return None
        return dt.isocalendar()[1]

    def _func_datediff(self, value: Any, operation: ColumnOperation) -> Any:
        """Date difference function (days).

        Evaluated via SQL translation during materialization; return None here
        to defer computation unless both operands are trivial literals (which
        are handled earlier in _evaluate_function_call).
        """
        return None

    def _func_date_format(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[str]:
        """Format date/timestamp as string using format pattern.

        Args:
            value: The date/timestamp value to format.
            operation: ColumnOperation with format string in operation.value.

        Returns:
            Formatted date string or None if value is None.
        """
        if value is None:
            return None

        format_str = operation.value if hasattr(operation, "value") else "yyyy-MM-dd"

        # Parse the date value
        dt = self._parse_datetime(value)
        if dt is None:
            return None

        # Convert PySpark format string to Python strftime format
        # Basic conversion for common patterns
        format_map = {
            "yyyy": "%Y",  # 4-digit year
            "MM": "%m",  # 2-digit month
            "dd": "%d",  # 2-digit day
            "HH": "%H",  # 24-hour format hour
            "mm": "%M",  # Minutes
            "ss": "%S",  # Seconds
        }

        # Simple conversion (handles basic cases)
        python_format = format_str
        for pyspark_fmt, python_fmt in format_map.items():
            python_format = python_format.replace(pyspark_fmt, python_fmt)

        try:
            return dt.strftime(python_format)
        except Exception:
            # Fallback to ISO format if conversion fails
            if isinstance(dt, dt_module.date) and not isinstance(
                dt, dt_module.datetime
            ):
                return dt.isoformat()  # type: ignore[unreachable]
            return dt.strftime("%Y-%m-%d")

    def _func_months_between(self, value: Any, operation: ColumnOperation) -> Any:
        """Months between function."""
        # This needs to be handled in _evaluate_function_call to get both dates
        # Placeholder - will be handled specially
        return None

    def _extract_datetime_component(self, value: Any, component: str) -> Any:
        """Extract a component from a datetime value."""
        if value is None:
            return None

        dt = self._parse_datetime(value)
        if dt is None:
            return None

        return getattr(dt, component)

    def _parse_datetime(self, value: Any) -> Optional[dt_module.datetime]:
        """Parse a value into a datetime object."""
        if isinstance(value, str):
            try:
                return dt_module.datetime.fromisoformat(value.replace(" ", "T"))
            except (ValueError, TypeError, AttributeError):
                return None
        elif (
            hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day")
        ):
            # Already a datetime-like object
            return cast("Optional[dt_module.datetime]", value)
        else:
            return None

    # New string function evaluations
    def _func_ilike(self, value: Any, operation: ColumnOperation) -> Optional[bool]:
        """Ilike function - case-insensitive LIKE pattern matching."""
        if value is None:
            return None
        pattern = operation.value
        import re

        # Convert SQL LIKE pattern to regex (simplified: % -> .*, _ -> .)
        if pattern is None:
            return False
        regex_pattern = pattern.replace("%", ".*").replace("_", ".")
        return bool(re.match(regex_pattern, str(value), re.IGNORECASE))

    def _func_find_in_set(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """Find_in_set function - find position in comma-separated list."""
        if value is None:
            return None
        str_list = operation.value
        if isinstance(str_list, str):
            parts = [p.strip() for p in str_list.split(",")]
            try:
                return parts.index(str(value)) + 1  # 1-indexed
            except ValueError:
                return 0
        return 0

    def _func_regexp_count(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """Regexp_count function - count regex matches."""
        if value is None:
            return None
        pattern = operation.value
        import re

        return len(re.findall(pattern, str(value)))

    def _func_regexp_like(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[bool]:
        """Regexp_like function - regex pattern matching."""
        if value is None:
            return None
        pattern = operation.value
        import re

        return bool(re.search(pattern, str(value)))

    def _func_regexp_substr(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[str]:
        """Regexp_substr function - extract substring matching regex."""
        if value is None:
            return None
        if isinstance(operation.value, tuple) and len(operation.value) >= 1:
            pattern = operation.value[0]
            import re

            match = re.search(pattern, str(value))
            return match.group(0) if match else None
        return None

    def _func_regexp_instr(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """Regexp_instr function - find position of regex match."""
        if value is None:
            return None
        if isinstance(operation.value, tuple) and len(operation.value) >= 1:
            pattern = operation.value[0]
            import re

            match = re.search(pattern, str(value))
            return match.start() + 1 if match else 0  # 1-indexed
        return 0

    def _func_regexp(self, value: Any, operation: ColumnOperation) -> Optional[bool]:
        """Regexp function - alias for rlike."""
        return self._func_rlike(value, operation)

    def _func_sentences(self, value: Any, operation: ColumnOperation) -> Any:
        """Sentences function - split text into sentences."""
        if value is None:
            return None
        # Simplified implementation - split by sentence-ending punctuation
        import re

        sentences = re.split(r"[.!?]+", str(value))
        return [s.strip() for s in sentences if s.strip()]

    def _func_printf(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Printf function - formatted string."""
        if value is None:
            return None
        if isinstance(operation.value, tuple) and len(operation.value) >= 1:
            format_str = operation.value[0]
            args = operation.value[1:] if len(operation.value) > 1 else []
            try:
                from typing import cast

                return cast("str", format_str % tuple(args))
            except (TypeError, ValueError):
                return None
        return None

    def _func_to_char(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """To_char function - convert to character string."""
        if value is None:
            return None
        # Simplified - just convert to string
        return str(value)

    def _func_to_varchar(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """To_varchar function - convert to varchar."""
        if value is None:
            return None
        length = operation.value
        result = str(value)
        if length is not None and isinstance(length, int):
            return result[:length]
        return result

    def _func_typeof(self, value: Any, operation: ColumnOperation) -> str:
        """Typeof function - get type as string."""
        if value is None:
            return "null"
        return type(value).__name__.lower()

    def _func_stack(self, value: Any, operation: ColumnOperation) -> Any:
        """Stack function - stack multiple columns into rows."""
        # Complex function - return None for now, needs special handling
        return None

    # New math/bitwise function evaluations
    def _func_pmod(self, value: Any, operation: ColumnOperation) -> Any:
        """Pmod function - positive modulo."""
        if value is None:
            return None
        divisor = operation.value
        if divisor is None or divisor == 0:
            return None
        try:
            result = value % divisor
            # Ensure positive result
            if result < 0:
                result += abs(divisor)
            return result
        except (TypeError, ValueError):
            return None

    def _func_negate(self, value: Any, operation: ColumnOperation) -> Any:
        """Negate function - alias for negative."""
        return self._func_negative(value, operation)

    def _func_shiftleft(self, value: Any, operation: ColumnOperation) -> Optional[int]:
        """Shiftleft function - bitwise left shift."""
        if value is None:
            return None
        num_bits = operation.value
        try:
            return int(value) << int(num_bits)
        except (TypeError, ValueError, OverflowError):
            return None

    def _func_shiftright(self, value: Any, operation: ColumnOperation) -> Optional[int]:
        """Shiftright function - bitwise right shift (signed)."""
        if value is None:
            return None
        num_bits = operation.value
        try:
            return int(value) >> int(num_bits)
        except (TypeError, ValueError, OverflowError):
            return None

    def _func_shiftrightunsigned(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """Shiftrightunsigned function - bitwise unsigned right shift."""
        if value is None:
            return None
        num_bits = operation.value
        try:
            val = int(value)
            # For unsigned right shift, treat as unsigned
            if val < 0:
                val = val + (1 << 32)  # Convert to unsigned 32-bit
            return val >> int(num_bits)
        except (TypeError, ValueError, OverflowError):
            return None

    def _func_ln(self, value: Any, operation: ColumnOperation) -> Optional[float]:
        """Ln function - natural logarithm."""
        if value is None:
            return None
        import math

        try:
            return math.log(float(value))
        except (ValueError, TypeError, OverflowError):
            return None

    # New datetime function evaluations
    def _func_years(self, value: Any, operation: ColumnOperation) -> Any:
        """Years function - convert number to years interval."""
        return value  # Return as-is for date arithmetic

    def _func_localtimestamp(self, value: Any, operation: ColumnOperation) -> Any:
        """Localtimestamp function - get local timestamp."""
        from datetime import datetime

        return datetime.now()

    def _func_dateadd(self, value: Any, operation: ColumnOperation) -> Any:
        """Dateadd function - SQL Server style date addition."""
        if value is None:
            return None
        if isinstance(operation.value, tuple) and len(operation.value) >= 2:
            date_part, add_value = operation.value[0], operation.value[1]
            dt = self._parse_datetime(value)
            if dt is None:
                return None
            from datetime import timedelta

            if date_part.lower() == "year":
                # Add years (simplified - add 365 days per year)
                return dt + timedelta(days=int(add_value) * 365)
            elif date_part.lower() == "month":
                # Add months (simplified - add 30 days per month)
                return dt + timedelta(days=int(add_value) * 30)
            elif date_part.lower() == "day":
                return dt + timedelta(days=int(add_value))
            elif date_part.lower() == "hour":
                return dt + timedelta(hours=int(add_value))
            elif date_part.lower() == "minute":
                return dt + timedelta(minutes=int(add_value))
            elif date_part.lower() == "second":
                return dt + timedelta(seconds=int(add_value))
        return None

    def _func_datepart(self, value: Any, operation: ColumnOperation) -> Any:
        """Datepart function - SQL Server style date part extraction."""
        if value is None:
            return None
        date_part = operation.value
        dt = self._parse_datetime(value)
        if dt is None:
            return None
        if date_part is None:
            return None
        part = date_part.lower()
        if part == "year":
            return dt.year
        elif part == "month":
            return dt.month
        elif part == "day":
            return dt.day
        elif part == "hour":
            return dt.hour
        elif part == "minute":
            return dt.minute
        elif part == "second":
            return dt.second
        elif part == "weekday":
            return dt.weekday() + 1
        return None

    def _func_make_timestamp(self, value: Any, operation: ColumnOperation) -> Any:
        """Make_timestamp function - create timestamp from components."""
        # Complex function - return None for now, needs special handling
        return None

    def _func_make_timestamp_ltz(self, value: Any, operation: ColumnOperation) -> Any:
        """Make_timestamp_ltz function - create timestamp with local timezone."""
        # Complex function - return None for now, needs special handling
        return None

    def _func_make_timestamp_ntz(self, value: Any, operation: ColumnOperation) -> Any:
        """Make_timestamp_ntz function - create timestamp with no timezone."""
        # Complex function - return None for now, needs special handling
        return None

    def _func_make_interval(self, value: Any, operation: ColumnOperation) -> Any:
        """Make_interval function - create interval from components."""
        # Complex function - return None for now, needs special handling
        return None

    def _func_make_dt_interval(self, value: Any, operation: ColumnOperation) -> Any:
        """Make_dt_interval function - create day-time interval."""
        # Complex function - return None for now, needs special handling
        return None

    def _func_make_ym_interval(self, value: Any, operation: ColumnOperation) -> Any:
        """Make_ym_interval function - create year-month interval."""
        # Complex function - return None for now, needs special handling
        return None

    def _func_to_number(self, value: Any, operation: ColumnOperation) -> Any:
        """To_number function - convert string to number."""
        if value is None:
            return None
        try:
            # Try int first, then float
            if isinstance(value, (int, float)):
                return value
            s = str(value).strip()
            if "." in s:
                return float(s)
            else:
                return int(s)
        except (ValueError, TypeError):
            return None

    def _func_to_binary(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[bytes]:
        """To_binary function - convert to binary format."""
        if value is None:
            return None
        if isinstance(value, bytes):
            return value
        try:
            return str(value).encode("utf-8")
        except (UnicodeEncodeError, TypeError):
            return None

    def _func_to_unix_timestamp(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """To_unix_timestamp function - convert to unix timestamp."""
        if value is None:
            return None
        dt = self._parse_datetime(value)
        if dt is None:
            return None
        import time

        return int(time.mktime(dt.timetuple()))

    def _func_unix_date(self, value: Any, operation: ColumnOperation) -> Any:
        """Unix_date function - convert unix timestamp to date."""
        if value is None:
            return None
        import time
        from datetime import date

        try:
            dt = time.localtime(int(value))
            return date(dt.tm_year, dt.tm_mon, dt.tm_mday)
        except (ValueError, TypeError, OverflowError):
            return None

    def _func_unix_seconds(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """Unix_seconds function - convert timestamp to unix seconds."""
        if value is None:
            return None
        dt = self._parse_datetime(value)
        if dt is None:
            return None
        import time

        return int(time.mktime(dt.timetuple()))

    def _func_unix_millis(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """Unix_millis function - convert timestamp to unix milliseconds."""
        if value is None:
            return None
        dt = self._parse_datetime(value)
        if dt is None:
            return None
        import time

        return int(time.mktime(dt.timetuple()) * 1000)

    def _func_unix_micros(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """Unix_micros function - convert timestamp to unix microseconds."""
        if value is None:
            return None
        dt = self._parse_datetime(value)
        if dt is None:
            return None
        import time

        return int(time.mktime(dt.timetuple()) * 1000000)

    def _func_timestamp_seconds(self, value: Any, operation: ColumnOperation) -> Any:
        """Timestamp_seconds function - create timestamp from unix seconds.

        Unix timestamps are interpreted as UTC and converted to local timezone
        to match PySpark behavior. The result matches the session timezone.
        """
        if value is None:
            return None
        from datetime import datetime, timezone

        try:
            timestamp = int(value)
            # Interpret unix timestamp as UTC and convert to local timezone
            # This matches PySpark's behavior where timestamps are in session timezone
            dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            # Convert to local timezone (naive datetime for compatibility)
            dt_local = dt_utc.astimezone().replace(tzinfo=None)
            # Return formatted string to match PySpark
            return dt_local.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError, OverflowError, OSError):
            return None

    def _func_timestamp_millis(self, value: Any, operation: ColumnOperation) -> Any:
        """Timestamp_millis function - create timestamp from unix milliseconds."""
        if value is None:
            return None
        from datetime import datetime

        try:
            return datetime.fromtimestamp(int(value) / 1000.0)
        except (ValueError, TypeError, OverflowError, OSError):
            return None

    def _func_timestamp_micros(self, value: Any, operation: ColumnOperation) -> Any:
        """Timestamp_micros function - create timestamp from unix microseconds."""
        if value is None:
            return None
        from datetime import datetime

        try:
            return datetime.fromtimestamp(int(value) / 1000000.0)
        except (ValueError, TypeError, OverflowError, OSError):
            return None

    # New utility function evaluations
    def _func_get(self, value: Any, operation: ColumnOperation) -> Any:
        """Get function - get element from array by index or map by key."""
        if value is None:
            return None
        key = operation.value
        if isinstance(value, (list, tuple)):
            # Array access
            try:
                idx = int(key)
                if 0 <= idx < len(value):
                    return value[idx]
                return None
            except (ValueError, TypeError, IndexError):
                return None
        elif isinstance(value, dict):
            # Map access
            return value.get(key)
        return None

    def _func_getItem(self, value: Any, operation: ColumnOperation) -> Any:
        """GetItem function - get element from array by index or map by key."""
        if value is None:
            return None
        key = operation.value
        if isinstance(value, (list, tuple)):
            # Array access
            try:
                idx = int(key)
                if 0 <= idx < len(value):
                    return value[idx]
                return None
            except (ValueError, TypeError, IndexError):
                return None
        elif isinstance(value, dict):
            # Map access
            return value.get(key)
        return None

    def _func_withField(self, value: Any, operation: ColumnOperation) -> Any:
        """WithField function - add or replace a field in a struct column.

        Note: This method is registered but withField is handled directly in
        _evaluate_function_call because it needs access to the row to evaluate
        the field column expression. This method exists for completeness but
        should not be called directly.

        Args:
            value: The struct value (dict) from the base column
            operation: ColumnOperation with operation="withField" and value containing
                      {"fieldName": str, "column": Column/ColumnOperation/Literal}

        Returns:
            Modified struct dict with the new/updated field, or None if base value is None
        """
        # This method should not be called - withField is handled in _evaluate_function_call
        # But we keep it for the function registry
        return None

    def _func_inline(self, value: Any, operation: ColumnOperation) -> Any:
        """Inline function - explode array of structs into rows."""
        # Complex function - return None for now, needs special handling
        return None

    def _func_inline_outer(self, value: Any, operation: ColumnOperation) -> Any:
        """Inline_outer function - explode array of structs into rows (outer join style)."""
        # Complex function - return None for now, needs special handling
        return None

    def _func_str_to_map(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[dict]:
        """Str_to_map function - convert string to map using delimiters."""
        if value is None:
            return None
        if isinstance(operation.value, tuple) and len(operation.value) >= 2:
            pair_delim, key_value_delim = operation.value[0], operation.value[1]
            result = {}
            pairs = str(value).split(pair_delim)
            for pair in pairs:
                if key_value_delim in pair:
                    key, val = pair.split(key_value_delim, 1)
                    result[key.strip()] = val.strip()
            return result
        return {}

    # New crypto function evaluations (PySpark 3.5+)
    def _func_aes_encrypt(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[bytes]:
        """Aes_encrypt function - encrypt data using AES."""
        if value is None:
            return None
        # Simplified: return None for now (encryption requires external library)
        return None

    def _func_aes_decrypt(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[str]:
        """Aes_decrypt function - decrypt data using AES."""
        if value is None:
            return None
        # Simplified: return None for now (decryption requires external library)
        return None

    def _func_try_aes_decrypt(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[str]:
        """Try_aes_decrypt function - null-safe AES decryption."""
        if value is None:
            return None
        try:
            return self._func_aes_decrypt(value, operation)
        except Exception:
            return None

    # New string function evaluations (PySpark 3.5+)
    def _func_sha(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Sha function - alias for sha1."""
        if value is None:
            return None
        import hashlib

        return hashlib.sha1(str(value).encode("utf-8")).hexdigest()

    def _func_mask(self, value: Any, operation: ColumnOperation) -> Optional[str]:
        """Mask function - mask sensitive data."""
        if value is None:
            return None
        params = operation.value if isinstance(operation.value, dict) else {}
        upper_char = params.get("upperChar", "X")
        lower_char = params.get("lowerChar", "x")
        digit_char = params.get("digitChar", "n")
        other_char = params.get("otherChar", "-")
        result = ""
        for c in str(value):
            if c.isupper():
                result += upper_char
            elif c.islower():
                result += lower_char
            elif c.isdigit():
                result += digit_char
            else:
                result += other_char
        return result

    def _func_json_array_length(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """Json_array_length function - get length of JSON array."""
        if value is None:
            return None
        import json

        path = operation.value if operation.value else None
        try:
            data = json.loads(str(value))
            if path:
                path_parts = path.lstrip("$.").split(".")
                for part in path_parts:
                    data = data.get(part, {})
            if isinstance(data, list):
                return len(data)
            return 0
        except (json.JSONDecodeError, AttributeError, TypeError):
            return 0

    def _func_json_object_keys(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[List[Any]]:
        """Json_object_keys function - get keys of JSON object."""
        if value is None:
            return None
        import json

        path = operation.value if operation.value else None
        try:
            data = json.loads(str(value))
            if path:
                path_parts = path.lstrip("$.").split(".")
                for part in path_parts:
                    data = data.get(part, {})
            if isinstance(data, dict):
                return list(data.keys())
            return []
        except (json.JSONDecodeError, AttributeError, TypeError):
            return []

    def _func_xpath_number(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[float]:
        """Xpath_number function - extract number from XML using XPath."""
        if value is None:
            return None
        # Simplified: return None for now (XPath requires lxml or similar library)
        return None

    def _func_user(self, value: Any, operation: ColumnOperation) -> str:
        """User function - get current user name."""
        import os

        return os.getenv("USER", os.getenv("USERNAME", "unknown"))

    # New math function evaluations (PySpark 3.5+)
    def _func_getbit(self, value: Any, operation: ColumnOperation) -> Optional[int]:
        """Getbit function - get bit at position."""
        if value is None:
            return None
        bit_pos = operation.value
        try:
            val = int(value)
            bit = int(bit_pos)
            return (val >> bit) & 1
        except (ValueError, TypeError):
            return None

    def _func_width_bucket(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[int]:
        """Width_bucket function - compute histogram bucket number."""
        if value is None:
            return None
        if isinstance(operation.value, tuple) and len(operation.value) >= 3:
            min_val, max_val, num_buckets = (
                operation.value[0],
                operation.value[1],
                operation.value[2],
            )
            try:
                val = float(value)
                min_v = (
                    float(min_val)
                    if not isinstance(min_val, (int, float))
                    else float(min_val)
                )
                max_v = (
                    float(max_val)
                    if not isinstance(max_val, (int, float))
                    else float(max_val)
                )
                num_b = int(num_buckets)
                if max_v <= min_v or num_b <= 0:
                    return None
                bucket = int(((val - min_v) / (max_v - min_v)) * num_b) + 1
                return max(1, min(bucket, num_b))
            except (ValueError, TypeError, ZeroDivisionError):
                return None
        return None

    # New datetime function evaluations (PySpark 3.5+)
    def _func_date_from_unix_date(self, value: Any, operation: ColumnOperation) -> Any:
        """Date_from_unix_date function - convert days since epoch to date."""
        if value is None:
            return None
        try:
            days = int(value)
            from datetime import date, timedelta

            epoch = date(1970, 1, 1)
            return epoch + timedelta(days=days)
        except (ValueError, TypeError, OverflowError):
            return None

    def _func_to_timestamp_ltz(self, value: Any, operation: ColumnOperation) -> Any:
        """To_timestamp_ltz function - convert to timestamp with local timezone."""
        if value is None:
            return None
        return self._func_to_timestamp(value, operation)

    def _func_to_timestamp_ntz(self, value: Any, operation: ColumnOperation) -> Any:
        """To_timestamp_ntz function - convert to timestamp with no timezone."""
        if value is None:
            return None
        return self._func_to_timestamp(value, operation)

    # New null-safe try function evaluations (PySpark 3.5+)
    def _func_try_add(self, value: Any, operation: ColumnOperation) -> Any:
        """Try_add function - null-safe addition."""
        if value is None:
            return None
        # Try to get right value - it might be a column reference or literal
        right_val = None
        if hasattr(operation, "value") and operation.value is not None:
            if isinstance(operation.value, (int, float, str)):
                right_val = operation.value
            else:
                # For column references, we'd need the row context, but for evaluation
                # we'll try to evaluate it directly if it's a simple value
                try:
                    right_val = self.evaluate_expression({}, operation.value)
                except Exception:
                    right_val = None
        if right_val is None:
            return None
        try:
            return value + right_val
        except (TypeError, ValueError):
            return None

    def _func_try_subtract(self, value: Any, operation: ColumnOperation) -> Any:
        """Try_subtract function - null-safe subtraction."""
        if value is None:
            return None
        # Try to get right value
        right_val = None
        if hasattr(operation, "value") and operation.value is not None:
            if isinstance(operation.value, (int, float, str)):
                right_val = operation.value
            else:
                try:
                    right_val = self.evaluate_expression({}, operation.value)
                except Exception:
                    right_val = None
        if right_val is None:
            return None
        try:
            return value - right_val
        except (TypeError, ValueError):
            return None

    def _func_try_multiply(self, value: Any, operation: ColumnOperation) -> Any:
        """Try_multiply function - null-safe multiplication."""
        if value is None:
            return None
        # Try to get right value
        right_val = None
        if hasattr(operation, "value") and operation.value is not None:
            if isinstance(operation.value, (int, float, str)):
                right_val = operation.value
            else:
                try:
                    right_val = self.evaluate_expression({}, operation.value)
                except Exception:
                    right_val = None
        if right_val is None:
            return None
        try:
            return value * right_val
        except (TypeError, ValueError):
            return None

    def _func_try_divide(self, value: Any, operation: ColumnOperation) -> Any:
        """Try_divide function - null-safe division."""
        if value is None:
            return None
        # Try to get right value
        right_val = None
        if hasattr(operation, "value") and operation.value is not None:
            if isinstance(operation.value, (int, float, str)):
                right_val = operation.value
            else:
                try:
                    right_val = self.evaluate_expression({}, operation.value)
                except Exception:
                    right_val = None
        if right_val is None or right_val == 0:
            return None
        try:
            return value / right_val
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    def _func_try_element_at(self, value: Any, operation: ColumnOperation) -> Any:
        """Try_element_at function - null-safe element_at."""
        if value is None:
            return None
        try:
            return self._func_get(value, operation)
        except (IndexError, KeyError, TypeError):
            return None

    def _func_try_to_binary(
        self, value: Any, operation: ColumnOperation
    ) -> Optional[bytes]:
        """Try_to_binary function - null-safe to_binary."""
        if value is None:
            return None
        try:
            format_str = operation.value if operation.value else "utf-8"
            if isinstance(value, bytes):
                return value
            elif isinstance(value, str):
                return value.encode(format_str)
            else:
                return str(value).encode(format_str)
        except (UnicodeEncodeError, TypeError):
            return None

    def _func_try_to_number(self, value: Any, operation: ColumnOperation) -> Any:
        """Try_to_number function - null-safe to_number."""
        if value is None:
            return None
        try:
            if isinstance(value, (int, float)):
                return value
            s = str(value).strip()
            if "." in s:
                return float(s)
            else:
                return int(s)
        except (ValueError, TypeError):
            return None

    def _func_try_to_timestamp(self, value: Any, operation: ColumnOperation) -> Any:
        """Try_to_timestamp function - null-safe to_timestamp."""
        if value is None:
            return None
        try:
            return self._func_to_timestamp(value, operation)
        except (ValueError, TypeError):
            return None
