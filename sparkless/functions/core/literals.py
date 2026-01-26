"""
Literal values for Sparkless.

This module provides Literal class for representing literal values
in column expressions and transformations.
"""

from typing import Any, Callable, Optional, TYPE_CHECKING, Union, cast
import math
from ...spark_types import DataType
from ...core.interfaces.functions import IColumn

if TYPE_CHECKING:
    from .operations import ColumnOperation


class Literal(IColumn):
    """Literal value for DataFrame operations.

    Represents a literal value that can be used in column expressions
    and transformations, maintaining compatibility with PySpark's lit function.
    """

    def __init__(
        self,
        value: Any,
        data_type: Optional[DataType] = None,
        resolver: Optional[Callable[[], Any]] = None,
    ):
        """Initialize Literal.

        Args:
            value: The literal value.
            data_type: Optional data type. Inferred from value if not specified.
            resolver: Optional callable that returns the resolved value at evaluation time.
                     The resolver should handle session resolution internally.
        """
        self.value = value
        self.data_type = data_type or self._infer_type(value)
        self.column_type = self.data_type  # Add column_type attribute for compatibility

        # Support for lazy evaluation of session-aware literals
        self._resolver = resolver
        self._is_lazy = resolver is not None

        # Use the actual value as column name for PySpark compatibility
        # Handle boolean values to match PySpark's lowercase representation
        if isinstance(value, bool):
            self._name = str(value).lower()
        elif isinstance(value, float) and math.isnan(value):
            # PySpark uses 'NaN' (capital) for NaN literals
            self._name = "NaN"
        else:
            self._name = str(value)

    def _resolve_lazy_value(self) -> Any:
        """Resolve lazy literal value using resolver function.

        The resolver function should resolve the session at evaluation time,
        not use a stored session reference.

        Returns:
            Resolved value from the session.
        """
        if not self._is_lazy:
            return self.value

        if self._resolver is None:
            return self.value

        try:
            # Resolver should handle session resolution internally
            return self._resolver()
        except Exception:
            # Fallback to stored value if resolution fails
            return self.value

    @property
    def name(self) -> str:
        """Get literal name."""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """Set literal name."""
        self._name = value

    def _infer_type(self, value: Any) -> DataType:
        """Infer data type from value.

        Delegates to SchemaInferenceEngine for consistency.

        Args:
            value: The value to infer type for.

        Returns:
            Inferred DataType.
        """
        from ...core.schema_inference import SchemaInferenceEngine

        return cast("DataType", SchemaInferenceEngine._infer_type(value))

    def __eq__(self, other: Any) -> "ColumnOperation":  # type: ignore[override]
        """Equality comparison.

        Note: Returns ColumnOperation instead of bool for PySpark compatibility.
        """
        from .column import ColumnOperation

        return ColumnOperation(self, "==", other)

    def __ne__(self, other: Any) -> "ColumnOperation":  # type: ignore[override]
        """Inequality comparison.

        Note: Returns ColumnOperation instead of bool for PySpark compatibility.
        """
        from .column import ColumnOperation

        return ColumnOperation(self, "!=", other)

    def __lt__(self, other: Any) -> "IColumn":
        """Less than comparison."""
        from .column import ColumnOperation

        return ColumnOperation(self, "<", other)

    def __le__(self, other: Any) -> "IColumn":
        """Less than or equal comparison."""
        from .column import ColumnOperation

        return ColumnOperation(self, "<=", other)

    def __gt__(self, other: Any) -> "IColumn":
        """Greater than comparison."""
        from .column import ColumnOperation

        return ColumnOperation(self, ">", other)

    def __ge__(self, other: Any) -> "IColumn":
        """Greater than or equal comparison."""
        from .column import ColumnOperation

        return ColumnOperation(self, ">=", other)

    def __add__(self, other: Any) -> "IColumn":
        """Addition operation."""
        from .column import ColumnOperation

        return ColumnOperation(self, "+", other)

    def __sub__(self, other: Any) -> "IColumn":
        """Subtraction operation."""
        from .column import ColumnOperation

        return ColumnOperation(self, "-", other)

    def __mul__(self, other: Any) -> "IColumn":
        """Multiplication operation."""
        from .column import ColumnOperation

        return ColumnOperation(self, "*", other)

    def __truediv__(self, other: Any) -> "IColumn":
        """Division operation."""
        from .column import ColumnOperation

        return ColumnOperation(self, "/", other)

    def __mod__(self, other: Any) -> "IColumn":
        """Modulo operation."""
        from .column import ColumnOperation

        return ColumnOperation(self, "%", other)

    def __and__(self, other: Any) -> "IColumn":
        """Logical AND operation."""
        from .column import ColumnOperation

        return ColumnOperation(self, "&", other)

    def __or__(self, other: Any) -> "IColumn":
        """Logical OR operation."""
        from .column import ColumnOperation

        return ColumnOperation(self, "|", other)

    def __invert__(self) -> "IColumn":
        """Logical NOT operation."""
        from .column import ColumnOperation

        return ColumnOperation(self, "!", None)

    def __neg__(self) -> "ColumnOperation":
        """Unary minus operation (-literal)."""
        from .column import ColumnOperation

        return ColumnOperation(self, "-", None)

    def isnull(self) -> "ColumnOperation":
        """Check if literal value is null."""
        from .column import ColumnOperation

        return ColumnOperation(self, "isnull", None)

    def isnotnull(self) -> "ColumnOperation":
        """Check if literal value is not null."""
        from .column import ColumnOperation

        return ColumnOperation(self, "isnotnull", None)

    def isNull(self) -> "ColumnOperation":
        """Check if literal value is null (PySpark compatibility)."""
        return self.isnull()

    def isNotNull(self) -> "ColumnOperation":
        """Check if literal value is not null (PySpark compatibility)."""
        return self.isnotnull()

    def eqNullSafe(self, other: Any) -> "ColumnOperation":
        """Null-safe equality comparison (PySpark eqNullSafe).

        This behaves like PySpark's eqNullSafe:
        - If both sides are null, the comparison is True.
        - If exactly one side is null, the comparison is False.
        - Otherwise, it behaves like standard equality, including any backend-specific type coercion rules.
        """
        from .column import ColumnOperation

        return ColumnOperation(self, "eqNullSafe", other)

    def isin(self, *values: Any) -> "ColumnOperation":
        """Check if literal value is in list of values."""
        from .column import ColumnOperation

        # Normalize: if single list argument provided, use it directly
        if len(values) == 1 and isinstance(values[0], (list, tuple)):
            normalized_values = list(values[0])
        else:
            normalized_values = list(values)
        return ColumnOperation(self, "isin", normalized_values)

    def between(self, lower: Any, upper: Any) -> "ColumnOperation":
        """Check if literal value is between lower and upper bounds."""
        from .column import ColumnOperation

        return ColumnOperation(self, "between", (lower, upper))

    def like(self, pattern: str) -> "ColumnOperation":
        """SQL LIKE pattern matching."""
        from .column import ColumnOperation

        return ColumnOperation(self, "like", pattern)

    def rlike(self, pattern: str) -> "ColumnOperation":
        """Regular expression pattern matching."""
        from .column import ColumnOperation

        return ColumnOperation(self, "rlike", pattern)

    def alias(self, name: str) -> "Literal":
        """Create an alias for the literal."""
        aliased_literal = Literal(
            self.value,
            self.data_type,
            resolver=self._resolver,
        )
        aliased_literal._name = name
        return aliased_literal

    def asc(self) -> "ColumnOperation":
        """Ascending sort order."""
        from .column import ColumnOperation

        return ColumnOperation(self, "asc", None)

    def desc(self) -> "ColumnOperation":
        """Descending sort order."""
        from .column import ColumnOperation

        return ColumnOperation(self, "desc", None)

    def cast(self, data_type: Union[DataType, str]) -> "ColumnOperation":
        """Cast literal to different data type."""
        from .column import ColumnOperation

        return ColumnOperation(self, "cast", data_type)

    def astype(self, data_type: Union[DataType, str]) -> "ColumnOperation":
        """Cast literal to different data type (alias for cast).

        This method is an alias for cast() and matches PySpark's API.

        Args:
            data_type: The target data type (DataType object or string name).

        Returns:
            ColumnOperation representing the cast operation.

        Example:
            >>> F.lit(1).astype("string")
        """
        return self.cast(data_type)

    def when(self, condition: "ColumnOperation", value: Any) -> Any:
        """Start a CASE WHEN expression."""
        from ..conditional import CaseWhen

        return CaseWhen(self, condition, value)

    def otherwise(self, value: Any) -> Any:
        """End a CASE WHEN expression with default value."""
        from ..conditional import CaseWhen

        return CaseWhen(self, None, value)

    def over(self, window_spec: Any) -> Any:
        """Apply window function over window specification."""
        from ..window_execution import WindowFunction

        return WindowFunction(self, window_spec)
