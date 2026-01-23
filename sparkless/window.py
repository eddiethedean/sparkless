"""
Mock Window functions implementation for PySpark compatibility.

This module provides comprehensive mock implementations of PySpark window
functions that behave identically to the real PySpark window functions.
Includes window specifications, partitioning, ordering, and boundary
definitions for advanced analytics operations.

Key Features:
    - Complete PySpark Window API compatibility
    - Window specification with partitionBy and orderBy
    - Row-based and range-based window boundaries
    - Window functions (row_number, rank, etc.)
    - Proper partitioning and ordering logic

Example:
    >>> from sparkless.sql import SparkSession, functions as F, Window
    >>> spark = SparkSession("test")
    >>> data = [{"department": "IT", "salary": 50000}, {"department": "IT", "salary": 60000}]
    >>> df = spark.createDataFrame(data)
    >>> window = Window.partitionBy("department").orderBy("salary")
    >>> result = df.select(F.row_number().over(window).alias("rank"))
    >>> result.show()
    DataFrame[2 rows, 1 columns]

    rank
    1
    2
"""

import sys
from typing import List, Optional, TYPE_CHECKING, Tuple, Union

if TYPE_CHECKING:
    from .functions import Column


class WindowSpec:
    """Mock WindowSpec for window function specifications.

    Provides a PySpark-compatible interface for defining window specifications
    including partitioning, ordering, and boundary conditions for window functions.

    Attributes:
        _partition_by: List of columns to partition by.
        _order_by: List of columns to order by.
        _rows_between: Row-based window boundaries.
        _range_between: Range-based window boundaries.

    Example:
        >>> window = WindowSpec()
        >>> window.partitionBy("department").orderBy("salary")
        >>> window.rowsBetween(-1, 1)
    """

    def __init__(self) -> None:
        self._partition_by: List[Union[str, Column]] = []
        self._order_by: List[Union[str, Column]] = []
        self._rows_between: Optional[Tuple[int, int]] = None
        self._range_between: Optional[Tuple[int, int]] = None

    def partitionBy(
        self, *cols: Union[str, "Column", List[Union[str, "Column"]]]
    ) -> "WindowSpec":
        """Add partition by columns.

        Args:
            *cols: Column names, "Column" objects, or a list of columns to partition by.
                   If a single list is provided, it will be unpacked.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If no columns provided or invalid column types.
        """
        if not cols:
            raise ValueError("At least one column must be specified for partitionBy")

        # Handle case where a single list is passed: partitionBy(["col1", "col2"])
        if len(cols) == 1 and isinstance(cols[0], list):
            # Unpack the list
            cols_list: List[Union[str, Column]] = cols[0]
        else:
            # Convert tuple to list, filtering out any nested lists (shouldn't happen)
            cols_list = [col for col in cols if not isinstance(col, list)]

        for col in cols_list:
            # Check if it's a string or has the name attribute (Column-like)
            if not isinstance(col, str) and not hasattr(col, "name"):
                raise ValueError(
                    f"Invalid column type: {type(col)}. Must be str or Column"
                )

        self._partition_by = cols_list
        return self

    def orderBy(
        self, *cols: Union[str, "Column", List[Union[str, "Column"]]]
    ) -> "WindowSpec":
        """Add order by columns.

        Args:
            *cols: Column names, "Column" objects, or a list of columns to order by.
                   If a single list is provided, it will be unpacked.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If no columns provided or invalid column types.
        """
        if not cols:
            raise ValueError("At least one column must be specified for orderBy")

        # Handle case where a single list is passed: orderBy(["col1", "col2"])
        if len(cols) == 1 and isinstance(cols[0], list):
            # Unpack the list
            cols_list: List[Union[str, Column]] = cols[0]
        else:
            # Convert tuple to list, filtering out any nested lists (shouldn't happen)
            cols_list = [col for col in cols if not isinstance(col, list)]

        for col in cols_list:
            # Check if it's a string or has the name attribute (Column-like)
            if not isinstance(col, str) and not hasattr(col, "name"):
                raise ValueError(
                    f"Invalid column type: {type(col)}. Must be str or Column"
                )

        self._order_by = cols_list
        return self

    def rowsBetween(self, start: int, end: int) -> "WindowSpec":
        """Set rows between boundaries.

        Args:
            start: Starting row offset (negative for preceding rows).
            end: Ending row offset (positive for following rows).

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If start > end or invalid values.
        """
        if start > end:
            raise ValueError(f"start ({start}) cannot be greater than end ({end})")

        self._rows_between = (start, end)
        return self

    def rangeBetween(self, start: int, end: int) -> "WindowSpec":
        """Set range between boundaries.

        Args:
            start: Starting range offset (negative for preceding range).
            end: Ending range offset (positive for following range).

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If start > end or invalid values.
        """
        if start > end:
            raise ValueError(f"start ({start}) cannot be greater than end ({end})")

        self._range_between = (start, end)
        return self

    def __repr__(self) -> str:
        """String representation."""
        parts = []
        if self._partition_by:
            parts.append(
                f"partitionBy({', '.join(str(col) for col in self._partition_by)})"
            )
        if self._order_by:
            parts.append(f"orderBy({', '.join(str(col) for col in self._order_by)})")
        if self._rows_between:
            parts.append(
                f"rowsBetween({self._rows_between[0]}, {self._rows_between[1]})"
            )
        if self._range_between:
            parts.append(
                f"rangeBetween({self._range_between[0]}, {self._range_between[1]})"
            )
        return f"WindowSpec({', '.join(parts)})"


class Window:
    """Mock Window class for creating window specifications.

    Provides static methods for creating window specifications with partitioning,
    ordering, and boundary conditions. Equivalent to PySpark's Window class.

    Example:
        >>> Window.partitionBy("department")
        >>> Window.orderBy("salary")
        >>> Window.partitionBy("department").orderBy("salary")
    """

    # Window boundary constants
    currentRow = 0
    unboundedPreceding = -sys.maxsize - 1
    unboundedFollowing = sys.maxsize

    @staticmethod
    def partitionBy(
        *cols: Union[str, "Column", List[Union[str, "Column"]]],
    ) -> WindowSpec:
        """Create a window spec with partition by columns.

        Args:
            *cols: Column names, "Column" objects, or a list of columns to partition by.
                   If a single list is provided, it will be unpacked.
        """
        return WindowSpec().partitionBy(*cols)

    @staticmethod
    def orderBy(*cols: Union[str, "Column", List[Union[str, "Column"]]]) -> WindowSpec:
        """Create a window spec with order by columns.

        Args:
            *cols: Column names, "Column" objects, or a list of columns to order by.
                   If a single list is provided, it will be unpacked.
        """
        return WindowSpec().orderBy(*cols)

    @staticmethod
    def rowsBetween(start: int, end: int) -> WindowSpec:
        """Create a window spec with rows between boundaries."""
        return WindowSpec().rowsBetween(start, end)

    @staticmethod
    def rangeBetween(start: int, end: int) -> WindowSpec:
        """Create a window spec with range between boundaries."""
        return WindowSpec().rangeBetween(start, end)
