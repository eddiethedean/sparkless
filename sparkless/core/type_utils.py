"""
Type utilities for Sparkless.

This module provides type guard functions and helper utilities for type checking
and narrowing, helping mypy understand types better and reducing false positive
unreachable code warnings.

Key Features:
    - Type guard functions using TypeGuard (PEP 647)
    - Helper functions for extracting names/values from expression types
    - Type narrowing utilities for Union types
    - Protocol type conversion helpers
"""

from typing import Any, TYPE_CHECKING, Union, cast

# Try to import TypeGuard from typing (Python 3.10+), fallback to typing_extensions
try:
    from typing import TypeGuard  # type: ignore[attr-defined,unused-ignore]
except (ImportError, AttributeError):
    from typing_extensions import TypeGuard

if TYPE_CHECKING:
    from ..functions import Column, ColumnOperation
    from ..functions.core.literals import Literal
    from ..spark_types import Row
    from .protocols import ColumnExpression
else:
    Column = Any
    ColumnOperation = Any
    Literal = Any
    Row = Any
    ColumnExpression = Any


def is_column(obj: Any) -> TypeGuard["Column"]:
    """Type guard: Check if object is a Column.

    Args:
        obj: Object to check

    Returns:
        True if object is a Column, False otherwise

    Example:
        >>> if is_column(expr):
        ...     print(expr.name)  # mypy knows expr is Column here
    """
    from ..functions import Column

    return isinstance(obj, Column)


def is_column_operation(obj: Any) -> TypeGuard["ColumnOperation"]:
    """Type guard: Check if object is a ColumnOperation.

    Args:
        obj: Object to check

    Returns:
        True if object is a ColumnOperation, False otherwise
    """
    from ..functions import ColumnOperation

    return isinstance(obj, ColumnOperation)


def is_literal(obj: Any) -> TypeGuard["Literal"]:
    """Type guard: Check if object is a Literal.

    Args:
        obj: Object to check

    Returns:
        True if object is a Literal, False otherwise
    """
    from ..functions.core.literals import Literal

    return isinstance(obj, Literal)


def is_row(obj: Any) -> TypeGuard["Row"]:
    """Type guard: Check if object is a Row.

    Args:
        obj: Object to check

    Returns:
        True if object is a Row, False otherwise
    """
    from ..spark_types import Row

    return isinstance(obj, Row)


def get_expression_name(expr: Any) -> str:
    """Extract name from any expression type.

    Handles Column, ColumnOperation, Literal, and other expression types,
    extracting the appropriate name attribute or value.

    Args:
        expr: Expression of any type (Column, ColumnOperation, Literal, str, etc.)

    Returns:
        String representation of the expression name

    Example:
        >>> get_expression_name(F.col("name"))  # Returns "name"
        >>> get_expression_name(F.lit(42))  # Returns "42"
    """
    if is_literal(expr):
        return str(expr.value)
    if is_column(expr):
        return str(expr.name)
    if is_column_operation(expr):
        return str(expr.name)
    if hasattr(expr, "name"):
        name_attr = getattr(expr, "name")
        return str(name_attr) if name_attr is not None else str(expr)
    return str(expr)


def get_expression_value(expr: Any) -> Any:
    """Extract value from any expression type.

    Handles Literal, ColumnOperation, and other expression types,
    extracting the appropriate value attribute.

    Args:
        expr: Expression of any type

    Returns:
        The value of the expression, or the expression itself if no value attribute

    Example:
        >>> get_expression_value(F.lit(42))  # Returns 42
        >>> get_expression_value(F.col("name"))  # Returns Column object
    """
    if is_literal(expr):
        return expr.value
    if hasattr(expr, "value"):
        return expr.value
    return expr


def normalize_to_column_expression(expr: Any) -> "ColumnExpression":
    """Normalize any expression to ColumnExpression protocol type.

    Converts various expression types (Column, ColumnOperation, Literal, str)
    to the ColumnExpression protocol type for use with protocol-typed functions.

    Args:
        expr: Expression of any type

    Returns:
        Expression cast to ColumnExpression protocol type

    Example:
        >>> expr = normalize_to_column_expression(F.col("name"))
        >>> df._evaluate_column_expression(row, expr)  # Type-safe
    """

    if isinstance(expr, str):
        return expr
    if is_column(expr) or is_column_operation(expr) or is_literal(expr):
        return cast("ColumnExpression", expr)
    return cast("ColumnExpression", str(expr))


def normalize_date_input(value: Any) -> Union["Column", "Literal"]:
    """Normalize date input to Column or Literal.

    Converts various input types (int, str, Column, Literal, ColumnOperation)
    to either a Column or Literal for date functions.

    Args:
        value: Input value of any type

    Returns:
        Column or Literal object

    Example:
        >>> year_col = normalize_date_input(2024)  # Returns Literal(2024)
        >>> month_col = normalize_date_input("month")  # Returns Column("month")
    """
    from ..functions import Column
    from ..functions.core.literals import Literal

    if isinstance(value, int):
        return Literal(value)
    if isinstance(value, str):
        return Column(value)
    if is_literal(value):
        return cast("Union[Column, Literal]", value)
    if is_column(value):
        return cast("Union[Column, Literal]", value)
    # For other types (ColumnOperation, etc.), create Column from name
    return cast("Union[Column, Literal]", Column(str(value)))
