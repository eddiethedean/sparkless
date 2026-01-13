"""
Function interface definitions.

This module defines the abstract interfaces for function operations,
ensuring consistent behavior across all function implementations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

RowDict = Dict[str, Any]
ArgT = TypeVar("ArgT")
RetT = TypeVar("RetT")


class IFunction(Generic[ArgT, RetT], ABC):
    """Abstract interface for all functions."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get function name."""
        pass

    @abstractmethod
    def alias(self, name: str) -> "IFunction[ArgT, RetT]":
        """Create function alias."""
        pass

    @abstractmethod
    def evaluate(self, arg: ArgT) -> RetT:
        """Evaluate function with arguments."""
        pass


class IColumnFunction(IFunction[RowDict, Any]):
    """Abstract interface for column functions."""

    @abstractmethod
    def apply_to_column(self, column_name: str) -> "IColumnFunction":
        """Apply function to column."""
        pass

    @abstractmethod
    def apply_to_value(self, value: Any) -> Any:
        """Apply function to value."""
        pass


class IAggregateFunction(IFunction[List[Any], Any]):
    """Abstract interface for aggregate functions."""

    @abstractmethod
    def apply_to_group(self, values: List[Any]) -> Any:
        """Apply function to group of values."""
        pass

    @abstractmethod
    def get_column_name(self) -> str:
        """Get column name for aggregation."""
        pass


class IWindowFunction(IFunction[List[Dict[str, Any]], List[Any]]):
    """Abstract interface for window functions."""

    @abstractmethod
    def over(self, window_spec: "IWindowSpec") -> "IWindowFunction":
        """Apply window specification."""
        pass

    @abstractmethod
    def apply_to_partition(self, partition: List[Any]) -> List[Any]:
        """Apply function to partition."""
        pass


class IWindowSpec(ABC):
    """Abstract interface for window specifications."""

    @abstractmethod
    def partitionBy(self, *columns: Union[str, Any]) -> "IWindowSpec":
        """Set partition columns."""
        pass

    @abstractmethod
    def orderBy(self, *columns: Union[str, Any]) -> "IWindowSpec":
        """Set order columns."""
        pass

    @abstractmethod
    def rowsBetween(self, start: int, end: int) -> "IWindowSpec":
        """Set row-based window frame."""
        pass

    @abstractmethod
    def rangeBetween(self, start: int, end: int) -> "IWindowSpec":
        """Set range-based window frame."""
        pass


class IStringFunction(IColumnFunction):
    """Abstract interface for string functions."""

    @abstractmethod
    def upper(self, column: Union[str, Any]) -> "IStringFunction":
        """Convert to uppercase."""
        pass

    @abstractmethod
    def lower(self, column: Union[str, Any]) -> "IStringFunction":
        """Convert to lowercase."""
        pass

    @abstractmethod
    def length(self, column: Union[str, Any]) -> "IStringFunction":
        """Get string length."""
        pass

    @abstractmethod
    def trim(self, column: Union[str, Any]) -> "IStringFunction":
        """Trim whitespace."""
        pass

    @abstractmethod
    def substring(
        self, column: Union[str, Any], start: int, length: int
    ) -> "IStringFunction":
        """Extract substring."""
        pass

    @abstractmethod
    def concat(self, *columns: Union[str, Any]) -> "IStringFunction":
        """Concatenate strings."""
        pass


class IMathFunction(IColumnFunction):
    """Abstract interface for mathematical functions."""

    @abstractmethod
    def abs(self, column: Union[str, Any]) -> "IMathFunction":
        """Absolute value."""
        pass

    @abstractmethod
    def round(self, column: Union[str, Any], scale: int = 0) -> "IMathFunction":
        """Round to scale."""
        pass

    @abstractmethod
    def ceil(self, column: Union[str, Any]) -> "IMathFunction":
        """Ceiling function."""
        pass

    @abstractmethod
    def floor(self, column: Union[str, Any]) -> "IMathFunction":
        """Floor function."""
        pass

    @abstractmethod
    def sqrt(self, column: Union[str, Any]) -> "IMathFunction":
        """Square root."""
        pass


class IDateTimeFunction(IColumnFunction):
    """Abstract interface for datetime functions."""

    @abstractmethod
    def current_timestamp(self) -> "IDateTimeFunction":
        """Current timestamp."""
        pass

    @abstractmethod
    def current_date(self) -> "IDateTimeFunction":
        """Current date."""
        pass

    @abstractmethod
    def to_date(
        self, column: Union[str, Any], format: Optional[str] = None
    ) -> "IDateTimeFunction":
        """Convert to date."""
        pass

    @abstractmethod
    def to_timestamp(
        self, column: Union[str, Any], format: Optional[str] = None
    ) -> "IDateTimeFunction":
        """Convert to timestamp."""
        pass

    @abstractmethod
    def year(self, column: Union[str, Any]) -> "IDateTimeFunction":
        """Extract year."""
        pass

    @abstractmethod
    def month(self, column: Union[str, Any]) -> "IDateTimeFunction":
        """Extract month."""
        pass

    @abstractmethod
    def day(self, column: Union[str, Any]) -> "IDateTimeFunction":
        """Extract day."""
        pass

    @abstractmethod
    def hour(self, column: Union[str, Any]) -> "IDateTimeFunction":
        """Extract hour."""
        pass


class IConditionalFunction(IColumnFunction):
    """Abstract interface for conditional functions."""

    @abstractmethod
    def when(self, condition: Any, value: Any) -> "ICaseWhen":
        """CASE WHEN condition."""
        pass

    @abstractmethod
    def otherwise(self, value: Any) -> "ICaseWhen":
        """CASE WHEN ELSE."""
        pass

    @abstractmethod
    def coalesce(self, *columns: Union[str, Any]) -> "IConditionalFunction":
        """Coalesce function."""
        pass

    @abstractmethod
    def isnull(self, column: Union[str, Any]) -> "IConditionalFunction":
        """Check if null."""
        pass

    @abstractmethod
    def isnan(self, column: Union[str, Any]) -> "IConditionalFunction":
        """Check if NaN."""
        pass


class ICaseWhen(IFunction[RowDict, Any]):
    """Abstract interface for CASE WHEN expressions."""

    @abstractmethod
    def when(self, condition: Any, value: Any) -> "ICaseWhen":
        """Add WHEN condition."""
        pass

    @abstractmethod
    def otherwise(self, value: Any) -> "ICaseWhen":
        """Add ELSE clause."""
        pass

    @abstractmethod
    def evaluate(self, row: RowDict) -> Any:
        """Evaluate CASE WHEN for row.

        Note: More specific signature than parent IFunction for CASE WHEN evaluation.
        """
        pass


class IColumn(ABC):
    """Abstract interface for column operations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get column name."""
        pass

    @abstractmethod
    def alias(self, name: str) -> "IColumn":
        """Create column alias."""
        pass

    @abstractmethod
    def __add__(self, other: Any) -> "IColumn":
        """Addition operation."""
        pass

    @abstractmethod
    def __sub__(self, other: Any) -> "IColumn":
        """Subtraction operation."""
        pass

    @abstractmethod
    def __mul__(self, other: Any) -> "IColumn":
        """Multiplication operation."""
        pass

    @abstractmethod
    def __truediv__(self, other: Any) -> "IColumn":
        """Division operation."""
        pass

    @abstractmethod
    def __mod__(self, other: Any) -> "IColumn":
        """Modulo operation."""
        pass

    @abstractmethod
    def __eq__(self, other: Any) -> "IColumn":  # type: ignore[override]
        """Equality operation."""
        pass

    @abstractmethod
    def __ne__(self, other: Any) -> "IColumn":  # type: ignore[override]
        """Inequality operation."""
        pass

    @abstractmethod
    def __lt__(self, other: Any) -> "IColumn":
        """Less than operation."""
        pass

    @abstractmethod
    def __le__(self, other: Any) -> "IColumn":
        """Less than or equal operation."""
        pass

    @abstractmethod
    def __gt__(self, other: Any) -> "IColumn":
        """Greater than operation."""
        pass

    @abstractmethod
    def __ge__(self, other: Any) -> "IColumn":
        """Greater than or equal operation."""
        pass

    @abstractmethod
    def __and__(self, other: Any) -> "IColumn":
        """Logical AND operation."""
        pass

    @abstractmethod
    def __or__(self, other: Any) -> "IColumn":
        """Logical OR operation."""
        pass

    @abstractmethod
    def __invert__(self) -> "IColumn":
        """Logical NOT operation."""
        pass
