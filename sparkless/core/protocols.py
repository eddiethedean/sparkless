"""
Type protocols for Sparkless.

This module defines structural typing protocols (PEP 544) for better
type safety and clearer contracts without tight coupling.
"""

from typing import Any, Protocol, Union, runtime_checkable


@runtime_checkable
class ColumnLike(Protocol):
    """Protocol for column-like objects."""

    @property
    def name(self) -> str:
        """Column name."""
        ...


@runtime_checkable
class OperationLike(Protocol):
    """Protocol for column operation objects."""

    @property
    def column(self) -> Any:
        """The column being operated on."""
        ...

    @property
    def operation(self) -> str:
        """The operation type."""
        ...

    @property
    def value(self) -> Any:
        """The operation value/operand."""
        ...

    @property
    def name(self) -> str:
        """Operation name."""
        ...


@runtime_checkable
class LiteralLike(Protocol):
    """Protocol for literal value objects."""

    @property
    def value(self) -> Any:
        """The literal value."""
        ...

    @property
    def name(self) -> str:
        """Literal name."""
        ...


@runtime_checkable
class CaseWhenLike(Protocol):
    """Protocol for CASE WHEN expression objects."""

    @property
    def conditions(self) -> Any:
        """List of (condition, value) tuples."""
        ...

    @property
    def default_value(self) -> Any:
        """Default value for ELSE clause."""
        ...


@runtime_checkable
class DataFrameLike(Protocol):
    """Protocol for DataFrame-like objects."""

    @property
    def data(self) -> Any:
        """DataFrame data."""
        ...

    @property
    def schema(self) -> Any:
        """DataFrame schema."""
        ...

    def collect(self) -> Any:
        """Collect DataFrame rows."""
        ...


@runtime_checkable
class SchemaLike(Protocol):
    """Protocol for schema-like objects."""

    @property
    def fields(self) -> Any:
        """Schema fields."""
        ...

    def fieldNames(self) -> Any:
        """Get field names."""
        ...


# Type aliases for common unions (improved type safety)
# Use string literals for forward references to avoid import cycles
ColumnExpression = Union[
    ColumnLike,
    OperationLike,
    LiteralLike,
    str,
]  # Can also include Column, ColumnOperation, Literal at runtime
AggregateExpression = Union[
    str, OperationLike, ColumnLike
]  # Can be string name or column operation
WindowExpression = Any  # WindowFunction is complex - keep as Any for now
