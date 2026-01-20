"""
Column operations for Sparkless.

This module provides arithmetic, comparison, and logical operations
for Column and ColumnOperation classes.
"""

from typing import Any, TYPE_CHECKING
from .column import Column, ColumnOperation

__all__ = [
    "ColumnOperation",
    "ColumnOperations",
    "ComparisonOperations",
    "SortOperations",
    "TypeOperations",
    "ConditionalOperations",
    "WindowOperations",
]


class ColumnOperations:
    """Mixin class for column operations."""

    def __add__(self, other: Any) -> ColumnOperation:
        """Addition operation."""
        if isinstance(other, Column):
            return ColumnOperation(self, "+", other)
        return ColumnOperation(self, "+", other)

    def __sub__(self, other: Any) -> ColumnOperation:
        """Subtraction operation."""
        if isinstance(other, Column):
            return ColumnOperation(self, "-", other)
        return ColumnOperation(self, "-", other)

    def __mul__(self, other: Any) -> ColumnOperation:
        """Multiplication operation."""
        if isinstance(other, Column):
            return ColumnOperation(self, "*", other)
        return ColumnOperation(self, "*", other)

    def __truediv__(self, other: Any) -> ColumnOperation:
        """Division operation."""
        if isinstance(other, Column):
            return ColumnOperation(self, "/", other)
        return ColumnOperation(self, "/", other)

    def __mod__(self, other: Any) -> ColumnOperation:
        """Modulo operation."""
        if isinstance(other, Column):
            return ColumnOperation(self, "%", other)
        return ColumnOperation(self, "%", other)

    def __and__(self, other: Any) -> ColumnOperation:
        """Logical AND operation."""
        if isinstance(other, Column):
            return ColumnOperation(self, "&", other)
        return ColumnOperation(self, "&", other)

    def __or__(self, other: Any) -> ColumnOperation:
        """Logical OR operation."""
        if isinstance(other, Column):
            return ColumnOperation(self, "|", other)
        return ColumnOperation(self, "|", other)

    def __invert__(self) -> ColumnOperation:
        """Logical NOT operation."""
        return ColumnOperation(self, "!", None)

    def __neg__(self) -> ColumnOperation:
        """Unary minus operation (-column)."""
        return ColumnOperation(self, "-", None)

    def __eq__(self, other: Any) -> ColumnOperation:  # type: ignore[override]
        """Equality comparison."""
        if isinstance(other, Column):
            return ColumnOperation(self, "==", other)
        return ColumnOperation(self, "==", other)

    def __ne__(self, other: Any) -> ColumnOperation:  # type: ignore[override]
        """Inequality comparison."""
        if isinstance(other, Column):
            return ColumnOperation(self, "!=", other)
        return ColumnOperation(self, "!=", other)

    def __lt__(self, other: Any) -> ColumnOperation:
        """Less than comparison."""
        if isinstance(other, Column):
            return ColumnOperation(self, "<", other)
        return ColumnOperation(self, "<", other)

    def __le__(self, other: Any) -> ColumnOperation:
        """Less than or equal comparison."""
        if isinstance(other, Column):
            return ColumnOperation(self, "<=", other)
        return ColumnOperation(self, "<=", other)

    def __gt__(self, other: Any) -> ColumnOperation:
        """Greater than comparison."""
        if isinstance(other, Column):
            return ColumnOperation(self, ">", other)
        return ColumnOperation(self, ">", other)

    def __ge__(self, other: Any) -> ColumnOperation:
        """Greater than or equal comparison."""
        if isinstance(other, Column):
            return ColumnOperation(self, ">=", other)
        return ColumnOperation(self, ">=", other)


class ComparisonOperations:
    """Mixin class for comparison operations."""

    if TYPE_CHECKING:

        @property
        def name(self) -> str: ...

    def isnull(self) -> ColumnOperation:
        """Check if column value is null."""
        return ColumnOperation(self, "isnull", None)

    def isnotnull(self) -> ColumnOperation:
        """Check if column value is not null."""
        return ColumnOperation(self, "isnotnull", None)

    def isNull(self) -> ColumnOperation:
        """Check if column value is null (PySpark compatibility)."""
        return self.isnull()

    def isNotNull(self) -> ColumnOperation:
        """Check if column value is not null (PySpark compatibility)."""
        return self.isnotnull()

    def eqNullSafe(self, other: Any) -> ColumnOperation:
        """Null-safe equality comparison (PySpark eqNullSafe).

        This behaves like PySpark's eqNullSafe:
        - If both sides are null, the comparison is True.
        - If exactly one side is null, the comparison is False.
        - Otherwise, it behaves like standard equality, including any
          backend-specific type coercion rules.
        """
        return ColumnOperation(self, "eqNullSafe", other)

    def isin(self, *values: Any) -> ColumnOperation:
        """Check if column value is in list of values."""
        # Normalize: if single list argument provided, use it directly
        if len(values) == 1 and isinstance(values[0], (list, tuple)):
            normalized_values = list(values[0])
        else:
            normalized_values = list(values)
        return ColumnOperation(self, "isin", normalized_values)

    def between(self, lower: Any, upper: Any) -> ColumnOperation:
        """Check if column value is between lower and upper bounds."""
        return ColumnOperation(self, "between", (lower, upper))

    def like(self, pattern: str) -> ColumnOperation:
        """SQL LIKE pattern matching."""
        return ColumnOperation(self, "like", pattern)

    def rlike(self, pattern: str) -> ColumnOperation:
        """Regular expression pattern matching.

        Args:
            pattern: Regular expression pattern to match.

        Returns:
            ColumnOperation representing the rlike function.

        Example:
            >>> df.select(F.col("name").rlike("^A.*"))
        """
        if isinstance(pattern, str):
            return ColumnOperation(
                self, "rlike", pattern, name=f"rlike({self.name}, {pattern!r})"
            )
        else:
            # If pattern is a Column, create a binary operation
            from ...functions.base import Column  # type: ignore[unreachable]

            if isinstance(pattern, Column):
                return ColumnOperation(
                    self, "rlike", pattern, name=f"rlike({self.name}, {pattern.name})"
                )
            else:
                return ColumnOperation(
                    self, "rlike", pattern, name=f"rlike({self.name}, {pattern!r})"
                )


class SortOperations:
    """Mixin class for sort operations."""

    def asc(self) -> ColumnOperation:
        """Ascending sort order."""
        return ColumnOperation(self, "asc", None)

    def desc(self) -> ColumnOperation:
        """Descending sort order."""
        return ColumnOperation(self, "desc", None)


class TypeOperations:
    """Mixin class for type operations."""

    def cast(self, data_type: Any) -> ColumnOperation:
        """Cast column to different data type."""
        return ColumnOperation(self, "cast", data_type)


class ConditionalOperations:
    """Mixin class for conditional operations."""

    def when(self, condition: ColumnOperation, value: Any) -> Any:
        """Start a CASE WHEN expression."""
        from ..conditional import CaseWhen

        return CaseWhen(self, condition, value)

    def otherwise(self, value: Any) -> Any:
        """End a CASE WHEN expression with default value."""
        from ..conditional import CaseWhen

        return CaseWhen(self, None, value)


class WindowOperations:
    """Mixin class for window operations."""

    def over(self, window_spec: Any) -> Any:
        """Apply window function over window specification."""
        from ..window_execution import WindowFunction

        return WindowFunction(self, window_spec)
