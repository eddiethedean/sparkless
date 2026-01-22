"""
Collection handler for DataFrame.

This module handles data collection and materialization operations
following the Single Responsibility Principle.
"""

from __future__ import annotations
from typing import Iterator
from typing import Any, Dict, List, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ..spark_types import Row, StructType


class CollectionHandler:
    """Handles data collection and materialization operations."""

    def collect(self, data: List[Dict[str, Any]], schema: StructType) -> List[Row]:
        """Convert data to Row objects."""
        from ..spark_types import Row

        return [Row(row, schema) for row in data]

    def take(self, data: List[Dict[str, Any]], schema: StructType, n: int) -> List[Row]:
        """Take first n rows."""
        from ..spark_types import Row

        return [Row(row, schema) for row in data[:n]]

    def head(
        self, data: List[Dict[str, Any]], schema: StructType, n: int = 1
    ) -> Union[Row, List[Row], None]:
        """Get first row(s)."""
        if not data:
            return None
        # PySpark always returns a list, even when n=1
        from ..spark_types import Row

        rows = [Row(data[i], schema) for i in range(min(n, len(data)))]
        return rows

    def tail(
        self, data: List[Dict[str, Any]], schema: StructType, n: int = 1
    ) -> Union[Row, List[Row], None]:
        """Get last n rows."""
        if not data:
            return None
        # PySpark always returns a list, even when n=1
        from ..spark_types import Row

        rows = [Row(data[i], schema) for i in range(max(0, len(data) - n), len(data))]
        return rows

    def first(self, data: List[Dict[str, Any]], schema: StructType) -> Union[Row, None]:
        """Get first row.

        Returns the first row of the DataFrame, or None if empty.
        This matches PySpark's DataFrame.first() behavior.

        Args:
            data: List of dictionaries representing DataFrame rows.
            schema: StructType defining the DataFrame schema.

        Returns:
            First Row, or None if DataFrame is empty.
        """
        if not data:
            return None
        from ..spark_types import Row

        return Row(data[0], schema)

    def to_local_iterator(
        self,
        data: List[Dict[str, Any]],
        schema: StructType,
        prefetch: bool = False,
    ) -> Iterator[Row]:
        """Return iterator over rows."""
        return iter(self.collect(data, schema))
