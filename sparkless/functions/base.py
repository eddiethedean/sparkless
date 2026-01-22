"""
Base function classes for Sparkless.

This module provides base classes for all function types.
Most classes are imported from core/ modules to avoid duplication.
"""

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union
from sparkless.spark_types import DataType, StringType

# Import core classes from their canonical locations
from .core.column import Column, ColumnOperation
from .core.literals import Literal
from .core.lambda_parser import (
    MockLambdaExpression,
    LambdaParser,
    LambdaTranslationError,
)

if TYPE_CHECKING:
    from .window_execution import WindowFunction

# Re-export for backward compatibility
__all__ = [
    "Column",
    "ColumnOperation",
    "Literal",
    "AggregateFunction",
    "MockLambdaExpression",
    "LambdaParser",
    "LambdaTranslationError",
]


class AggregateFunction:
    """Base class for aggregate functions.

    This class provides the base functionality for all aggregate functions
    including count, sum, avg, max, min, etc.
    """

    def __init__(
        self,
        column: Union[Column, ColumnOperation, str, None],
        function_name: str,
        data_type: Optional[DataType] = None,
        ignorenulls: Optional[bool] = None,
    ):
        """Initialize AggregateFunction.

        Args:
            column: The column to aggregate (None for count(*)).
            function_name: Name of the aggregate function.
            data_type: Optional return data type.
            ignorenulls: Optional flag to ignore nulls (for first/last functions).
        """
        self.column = column
        self.function_name = function_name
        self.data_type = self._configure_data_type(data_type)
        self.name = self._generate_name()
        # Optional attributes for specific functions
        self.ord_column: Optional[Union[Column, str]] = None  # For max_by, min_by
        self.ignorenulls: Optional[bool] = ignorenulls  # For first/last functions
        self.rsd: Optional[float] = (
            None  # For approx_count_distinct (relative standard deviation)
        )
        self.percentage: Optional[float] = None  # For percentile function

    def _configure_data_type(self, data_type: Optional[DataType]) -> DataType:
        """Configure data type with appropriate nullability based on function type."""
        if not data_type:
            return StringType()

        # Functions that always return non-nullable results in PySpark
        non_nullable_functions = {
            "count",
            "countDistinct",
            "row_number",
            "rank",
            "dense_rank",
            "isNull",
            "isnan",
            "coalesce",
        }

        if self.function_name in non_nullable_functions:
            data_type.nullable = False

        return data_type

    @property
    def column_name(self) -> str:
        """Get the column name for compatibility."""
        if self.column is None:
            return "*"
        elif isinstance(self.column, str):
            return self.column
        else:
            return str(self.column.name)

    def _generate_name(self) -> str:
        """Generate a name for this aggregate function."""
        # PySpark uses 'avg' as column name even when using mean()
        # PySpark uses 'stddev_samp' as column name for stddev()
        # PySpark uses 'var_samp' as column name for variance()
        if self.function_name == "mean":
            display_name = "avg"
        elif self.function_name == "stddev":
            display_name = "stddev_samp"
        elif self.function_name == "variance":
            display_name = "var_samp"
        else:
            display_name = self.function_name

        if self.column is None:
            # For count(*), PySpark generates just "count", not "count(*)"
            if self.function_name == "count":
                return "count"
            else:
                return f"{display_name}(*)"
        elif isinstance(self.column, str):
            # For count(\"*\"), our PySpark parity fixtures expect the
            # column name \"count\" (not \"count(1)\"), so we normalise
            # to the same name we use for COUNT(*).
            if self.function_name == "count" and self.column == "*":
                return "count"
            elif self.function_name == "countDistinct":
                # PySpark uses "count(column)" not "count(DISTINCT column)" for column names
                return f"count({self.column})"
            else:
                return f"{display_name}({self.column})"
        else:
            if self.function_name == "countDistinct":
                # PySpark uses "count(column)" not "count(DISTINCT column)" for column names
                return f"count({self.column.name})"
            elif self.function_name == "approx_count_distinct":
                # PySpark doesn't include rsd in column name, just use the base name
                return f"{display_name}({self.column.name})"
            else:
                return f"{display_name}({self.column.name})"

    def evaluate(self, data: List[Dict[str, Any]]) -> Any:
        """Evaluate the aggregate function on the given data.

        Args:
            data: List of data rows to aggregate.

        Returns:
            The aggregated result.
        """
        if self.function_name == "count":
            return self._evaluate_count(data)
        elif self.function_name == "sum":
            return self._evaluate_sum(data)
        elif self.function_name == "avg":
            return self._evaluate_avg(data)
        elif self.function_name == "max":
            return self._evaluate_max(data)
        elif self.function_name == "min":
            return self._evaluate_min(data)
        else:
            return None

    def _evaluate_count(self, data: List[Dict[str, Any]]) -> int:
        """Evaluate count function."""
        if self.column is None:
            return len(data)
        else:
            column_name = (
                self.column if isinstance(self.column, str) else self.column.name
            )
            return sum(1 for row in data if row.get(column_name) is not None)

    def _evaluate_sum(self, data: List[Dict[str, Any]]) -> Any:
        """Evaluate sum function."""
        if self.column is None:
            return 0

        column_name = self.column if isinstance(self.column, str) else self.column.name
        total = 0
        for row in data:
            value = row.get(column_name)
            if value is not None:
                total += value
        return total

    def _evaluate_avg(self, data: List[Dict[str, Any]]) -> Any:
        """Evaluate average function."""
        if self.column is None:
            return 0.0

        column_name = self.column if isinstance(self.column, str) else self.column.name
        values = [
            row.get(column_name) for row in data if row.get(column_name) is not None
        ]
        numeric_values = [v for v in values if isinstance(v, (int, float))]
        if numeric_values:
            return sum(numeric_values) / len(numeric_values)
        else:
            return None

    def _evaluate_max(self, data: List[Dict[str, Any]]) -> Any:
        """Evaluate max function."""
        if self.column is None:
            return None

        column_name = self.column if isinstance(self.column, str) else self.column.name
        values = [
            row.get(column_name) for row in data if row.get(column_name) is not None
        ]
        if values:
            return max(values)  # type: ignore[type-var]
        else:
            return None

    def _evaluate_min(self, data: List[Dict[str, Any]]) -> Any:
        """Evaluate min function."""
        if self.column is None:
            return None

        column_name = self.column if isinstance(self.column, str) else self.column.name
        values = [
            row.get(column_name) for row in data if row.get(column_name) is not None
        ]
        if values:
            return min(values)  # type: ignore[type-var]
        else:
            return None

    def over(self, window_spec: Any) -> "WindowFunction":
        """Apply window function over window specification."""
        from .window_execution import WindowFunction

        return WindowFunction(self, window_spec)

    def alias(self, name: str) -> "AggregateFunction":
        """Create an alias for this aggregate function.

        Args:
            name: The alias name.

        Returns:
            Self for method chaining.
        """
        self.name = name
        return self

    def cast(self, data_type: Union[DataType, str]) -> "ColumnOperation":
        """Cast the aggregate function result to a different data type.

        Args:
            data_type: The target data type (DataType instance or string type name).

        Returns:
            ColumnOperation representing the cast operation.

        Example:
            >>> F.mean(F.col("value")).cast("string")
        """
        return ColumnOperation(self, "cast", data_type)

    def _create_operation(self, operation: str, other: Any) -> "ColumnOperation":
        """Create a ColumnOperation with the given operation and other operand.

        Args:
            operation: The operation to perform (e.g., "+", "-", etc.)
            other: The other operand

        Returns:
            ColumnOperation instance
        """
        return ColumnOperation(self, operation, other)

    def __add__(self, other: Any) -> "ColumnOperation":
        """Addition operation (PySpark-compatible)."""
        return self._create_operation("+", other)

    def __sub__(self, other: Any) -> "ColumnOperation":
        """Subtraction operation (PySpark-compatible)."""
        return self._create_operation("-", other)

    def __mul__(self, other: Any) -> "ColumnOperation":
        """Multiplication operation (PySpark-compatible)."""
        return self._create_operation("*", other)

    def __truediv__(self, other: Any) -> "ColumnOperation":
        """Division operation (PySpark-compatible)."""
        return self._create_operation("/", other)

    def __mod__(self, other: Any) -> "ColumnOperation":
        """Modulo operation (PySpark-compatible)."""
        return self._create_operation("%", other)

    def __radd__(self, other: Any) -> "ColumnOperation":
        """Reverse addition operation (for `2 + agg_func`)."""
        # For commutative operations, we can just swap operands
        return self._create_operation("+", other)

    def __rsub__(self, other: Any) -> "ColumnOperation":
        """Reverse subtraction operation (for `2 - agg_func`)."""
        # For non-commutative operations, create ColumnOperation with literal as left operand
        return ColumnOperation(other, "-", self)

    def __rmul__(self, other: Any) -> "ColumnOperation":
        """Reverse multiplication operation (for `2 * agg_func`)."""
        # For commutative operations, we can just swap operands
        return self._create_operation("*", other)

    def __rtruediv__(self, other: Any) -> "ColumnOperation":
        """Reverse division operation (for `2 / agg_func`)."""
        # For non-commutative operations, create ColumnOperation with literal as left operand
        return ColumnOperation(other, "/", self)

    def __rmod__(self, other: Any) -> "ColumnOperation":
        """Reverse modulo operation (for `2 % agg_func`)."""
        # For non-commutative operations, create ColumnOperation with literal as left operand
        return ColumnOperation(other, "%", self)
