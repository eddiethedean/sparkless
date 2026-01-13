"""
Metadata and utility functions for Sparkless.

This module provides metadata functions like input_file_name, partition IDs,
and special utilities like broadcast hints.
"""

from typing import Any, Union
from sparkless.functions.base import Column, ColumnOperation
from sparkless.functions.core.literals import Literal


class MetadataFunctions:
    """Collection of metadata and utility functions."""

    @staticmethod
    def input_file_name() -> ColumnOperation:
        """Returns the name of the file being read (returns empty string in mock).

        Returns:
            ColumnOperation representing input_file_name
        """
        return ColumnOperation(Literal(""), "input_file_name", name="input_file_name()")

    @staticmethod
    def monotonically_increasing_id() -> ColumnOperation:
        """Generate monotonically increasing 64-bit integers.

        Returns:
            ColumnOperation representing monotonically_increasing_id
        """
        return ColumnOperation(
            Literal(0),
            "monotonically_increasing_id",
            name="monotonically_increasing_id()",
        )

    @staticmethod
    def spark_partition_id() -> ColumnOperation:
        """Returns the partition ID (returns 0 in mock).

        Returns:
            ColumnOperation representing spark_partition_id
        """
        return ColumnOperation(
            Literal(0), "spark_partition_id", name="spark_partition_id()"
        )

    @staticmethod
    def broadcast(df: Any) -> Any:
        """Mark DataFrame for broadcast join (pass-through in mock).

        Args:
            df: DataFrame to broadcast

        Returns:
            The same DataFrame (broadcast is a hint, no-op in mock)
        """
        return df

    @staticmethod
    def column(col_name: str) -> Column:
        """Create a column reference (alias for col).

        Args:
            col_name: Column name

        Returns:
            Column reference
        """
        return Column(col_name)


class GroupingFunctions:
    """Grouping indicator functions."""

    @staticmethod
    def grouping(column: Union[Column, str]) -> ColumnOperation:
        """Indicates whether a column is aggregated (for CUBE/ROLLUP).

        Args:
            column: Column name

        Returns:
            ColumnOperation representing grouping
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "grouping", name=f"grouping({column.name})")

    @staticmethod
    def grouping_id(*cols: Union[Column, str]) -> ColumnOperation:
        """Computes grouping ID for CUBE/ROLLUP.

        Args:
            *cols: Columns to compute grouping ID

        Returns:
            ColumnOperation representing grouping_id
        """
        columns = []
        for col in cols:
            if isinstance(col, str):
                columns.append(Column(col))
            else:
                columns.append(col)

        return ColumnOperation(
            columns[0] if columns else Column(""),
            "grouping_id",
            value=columns[1:] if len(columns) > 1 else [],
            name="grouping_id(...)",
        )
