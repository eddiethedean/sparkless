"""
JSON and CSV functions for Sparkless.

This module provides JSON and CSV processing functions that match PySpark's API.
Includes parsing, generation, and schema inference for JSON and CSV data.
"""

from typing import Any, Dict, Optional, Union
from sparkless.functions.base import Column, ColumnOperation


class JSONCSVFunctions:
    """Collection of JSON and CSV manipulation functions."""

    @staticmethod
    def from_json(
        column: Union[Column, str],
        schema: Any,
        options: Optional[Dict[str, Any]] = None,
    ) -> ColumnOperation:
        """Parse JSON string column into struct/array column.

        Args:
            column: JSON string column
            schema: Target schema
            options: Optional parsing options

        Returns:
            ColumnOperation representing from_json
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "from_json",
            value=(schema, options),
            name=f"from_json({column.name})",
        )

    @staticmethod
    def to_json(column: Union[Column, str]) -> ColumnOperation:
        """Convert struct/array column to JSON string.

        Args:
            column: Struct or array column

        Returns:
            ColumnOperation representing to_json
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "to_json", name=f"to_json({column.name})")

    @staticmethod
    def get_json_object(column: Union[Column, str], path: str) -> ColumnOperation:
        """Extract JSON object at specified path.

        Args:
            column: JSON string column
            path: JSON path (e.g., '$.field')

        Returns:
            ColumnOperation representing get_json_object
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "get_json_object",
            value=path,
            name=f"get_json_object({column.name}, {path})",
        )

    @staticmethod
    def json_tuple(column: Union[Column, str], *fields: str) -> ColumnOperation:
        """Extract multiple fields from JSON string.

        Args:
            column: JSON string column
            *fields: Field names to extract

        Returns:
            ColumnOperation representing json_tuple
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "json_tuple", value=fields, name=f"json_tuple({column.name}, ...)"
        )

    @staticmethod
    def schema_of_json(json_string: str) -> ColumnOperation:
        """Infer schema from JSON string.

        Args:
            json_string: Sample JSON string

        Returns:
            ColumnOperation representing schema_of_json
        """
        from sparkless.functions.core.literals import Literal

        return ColumnOperation(
            Literal(json_string), "schema_of_json", name="schema_of_json(...)"
        )

    @staticmethod
    def from_csv(
        column: Union[Column, str],
        schema: Any,
        options: Optional[Dict[str, Any]] = None,
    ) -> ColumnOperation:
        """Parse CSV string column into struct column.

        Args:
            column: CSV string column
            schema: Target schema
            options: Optional parsing options

        Returns:
            ColumnOperation representing from_csv
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "from_csv", value=(schema, options), name=f"from_csv({column.name})"
        )

    @staticmethod
    def to_csv(column: Union[Column, str]) -> ColumnOperation:
        """Convert struct column to CSV string.

        Args:
            column: Struct column

        Returns:
            ColumnOperation representing to_csv
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "to_csv", name=f"to_csv({column.name})")

    @staticmethod
    def schema_of_csv(csv_string: str) -> ColumnOperation:
        """Infer schema from CSV string.

        Args:
            csv_string: Sample CSV string

        Returns:
            ColumnOperation representing schema_of_csv
        """
        from sparkless.functions.core.literals import Literal

        return ColumnOperation(
            Literal(csv_string), "schema_of_csv", name="schema_of_csv(...)"
        )
