"""DataFrame formatting utilities for display operations."""

from typing import Any, List, Optional

from ...spark_types import Row


class DataFrameFormatter:
    """Handles DataFrame formatting for display operations."""

    @staticmethod
    def format_row(row: Row, columns: List[str]) -> str:
        """Format a single row for display."""
        formatted_values = []
        for col in columns:
            value = getattr(row, col, None)
            if value is None:
                formatted_values.append("null")
            elif isinstance(value, str):
                formatted_values.append(f'"{value}"')
            else:
                formatted_values.append(str(value))
        return f"[{', '.join(formatted_values)}]"

    @staticmethod
    def format_rows(
        rows: List[Row], columns: List[str], limit: Optional[int] = None
    ) -> List[str]:
        """Format multiple rows for display."""
        if limit is not None:
            rows = rows[:limit]
        return [DataFrameFormatter.format_row(row, columns) for row in rows]

    @staticmethod
    def format_schema(columns: List[str], types: List[str]) -> str:
        """Format the schema information."""
        schema_parts = []
        for col, col_type in zip(columns, types):
            schema_parts.append(f" {col}: {col_type}")
        return "root\n" + "\n".join(schema_parts)

    @staticmethod
    def truncate_string(value: str, max_length: int = 50) -> str:
        """Truncate a string to the specified maximum length."""
        if len(value) <= max_length:
            return value
        return value[: max_length - 3] + "..."

    @staticmethod
    def format_value_for_display(value: Any, max_length: int = 50) -> str:
        """Format a value for display with truncation."""
        if value is None:
            return "null"
        elif isinstance(value, str):
            return DataFrameFormatter.truncate_string(value, max_length)
        else:
            return str(value)
