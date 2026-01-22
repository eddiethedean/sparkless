"""
Display service for DataFrame operations.

This service provides display and collection operations using composition instead of mixin inheritance.
"""

from typing import Any, List, TYPE_CHECKING, Union, cast

from ...spark_types import Row, StringType, StructField, StructType

if TYPE_CHECKING:
    from ..dataframe import DataFrame
    from ..protocols import SupportsDataFrameOps


class DisplayService:
    """Service providing display and collection operations for DataFrame."""

    def __init__(self, df: "DataFrame"):
        """Initialize display service with DataFrame instance."""
        self._df = df

    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Display DataFrame content in a clean table format.

        Args:
            n: Number of rows to display (default: 20).
            truncate: Whether to truncate long values (default: True).

        Example:
            >>> df.show(5)
            DataFrame[3 rows, 3 columns]
            name    age  salary
            Alice   25   50000
            Bob     30   60000
            Charlie 35   70000
        """
        # Materialize lazy operations if needed
        if self._df._operations_queue:
            materialized = self._df._materialize_if_lazy()
            # Call show on the materialized DataFrame - need to access via DataFrame
            if hasattr(materialized, "_display"):
                materialized._display.show(n, truncate)
            else:
                # Fallback: create new DisplayService
                from ..services.display_service import DisplayService

                display_service = DisplayService(materialized)  # type: ignore[arg-type]
                return display_service.show(n, truncate)
            return

        print(
            f"DataFrame[{len(self._df.data)} rows, {len(self._df.schema.fields)} columns]"
        )
        if not self._df.data:
            print("(empty)")
            return

        # Show first n rows
        display_data = self._df.data[:n]

        # Get column names
        columns = (
            list(display_data[0].keys())
            if display_data
            else self._df.schema.fieldNames()
        )

        # Calculate column widths
        col_widths = {}
        for col in columns:
            # Start with column name width
            col_widths[col] = len(col)
            # Check data widths
            for row in display_data:
                value = str(row.get(col, "null"))
                if truncate and len(value) > 20:
                    value = value[:17] + "..."
                col_widths[col] = max(col_widths[col], len(value))

        # Print header (no extra padding) - add blank line for separation
        print()  # Add blank line between metadata and headers
        header_parts = []
        for col in columns:
            header_parts.append(col.ljust(col_widths[col]))
        print(" ".join(header_parts))

        # Print data rows (with padding for alignment)
        for row in display_data:
            row_parts = []
            for col in columns:
                value = str(row.get(col, "null"))
                if truncate and len(value) > 20:
                    value = value[:17] + "..."
                # Add padding to data but not headers
                padded_width = col_widths[col] + 2
                row_parts.append(value.ljust(padded_width))
            print(" ".join(row_parts))

        if len(self._df.data) > n:
            print(f"\n... ({len(self._df.data) - n} more rows)")

    def to_markdown(
        self,
        n: int = 20,
        truncate: bool = True,
        underline_headers: bool = True,
    ) -> str:
        """
        Return DataFrame as a markdown table string.

        Args:
            n: Number of rows to show
            truncate: Whether to truncate long strings
            underline_headers: Whether to underline headers with = symbols

        Returns:
            String representation of DataFrame as markdown table
        """
        if not self._df.data:
            return f"DataFrame[{len(self._df.data)} rows, {len(self._df.schema.fields)} columns]\n\n(empty)"

        # Show first n rows
        display_data = self._df.data[:n]

        # Get column names
        columns = (
            list(display_data[0].keys())
            if display_data
            else self._df.schema.fieldNames()
        )

        # Build markdown table
        lines = []
        lines.append(
            f"DataFrame[{len(self._df.data)} rows, {len(self._df.schema.fields)} columns]"
        )
        lines.append("")  # Blank line

        # Header row
        header_row = "| " + " | ".join(columns) + " |"
        lines.append(header_row)

        # Separator row - use underlines for better visual distinction
        if underline_headers:
            separator_row = (
                "| " + " | ".join(["=" * len(col) for col in columns]) + " |"
            )
        else:
            separator_row = "| " + " | ".join(["---" for _ in columns]) + " |"
        lines.append(separator_row)

        # Data rows
        for row in display_data:
            row_values = []
            for col in columns:
                value = str(row.get(col, "null"))
                if truncate and len(value) > 20:
                    value = value[:17] + "..."
                row_values.append(value)
            data_row = "| " + " | ".join(row_values) + " |"
            lines.append(data_row)

        if len(self._df.data) > n:
            lines.append(f"\n... ({len(self._df.data) - n} more rows)")

        return "\n".join(lines)

    def printSchema(self) -> None:
        """Print DataFrame schema."""
        print("DataFrame Schema:")
        for field in self._df.schema.fields:
            nullable = "nullable" if field.nullable else "not nullable"
            print(
                f" |-- {field.name}: {field.dataType.__class__.__name__} ({nullable})"
            )

    def collect(self) -> List[Row]:
        """Collect all data as list of Row objects."""

        if self._df._operations_queue:
            materialized = self._df._materialize_if_lazy()
            result = self._df._get_collection_handler().collect(
                materialized.data, materialized.schema
            )
            return result
        result = self._df._get_collection_handler().collect(
            self._df.data, self._df.schema
        )
        return result

    def take(self, n: int) -> List[Row]:
        """Take first n rows as list of Row objects."""

        if self._df._operations_queue:
            materialized = self._df._materialize_if_lazy()
            result = self._df._get_collection_handler().take(
                materialized.data, materialized.schema, n
            )
            return result
        result = self._df._get_collection_handler().take(
            self._df.data, self._df.schema, n
        )
        return result

    def head(self, n: int = 1) -> List[Row]:
        """Return first n rows. Always returns a list, matching PySpark behavior."""
        if self._df._operations_queue:
            materialized = self._df._materialize_if_lazy()
            result = self._df._get_collection_handler().head(
                materialized.data, materialized.schema, n
            )
        else:
            result = self._df._get_collection_handler().head(
                self._df.data, self._df.schema, n
            )

        # Ensure we always return a list (PySpark behavior)
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return [result]

    def tail(self, n: int = 1) -> Union[Row, List[Row], None]:
        """Return last n rows."""
        if self._df._operations_queue:
            materialized = self._df._materialize_if_lazy()
            return self._df._get_collection_handler().tail(
                materialized.data, materialized.schema, n
            )
        return self._df._get_collection_handler().tail(
            self._df.data, self._df.schema, n
        )

    def toPandas(self) -> Any:
        """Convert to pandas DataFrame (requires pandas as optional dependency)."""
        from ..export import DataFrameExporter

        return DataFrameExporter.to_pandas(cast("SupportsDataFrameOps", self._df))

    def toJSON(self) -> "SupportsDataFrameOps":
        """Return a single-column DataFrame of JSON strings."""
        import json

        json_rows = [{"value": json.dumps(row)} for row in self._df.data]

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(
                json_rows,
                StructType([StructField("value", StringType())]),
                self._df.storage,
            ),
        )

    def count(self) -> int:
        """Count number of rows."""

        # Materialize lazy operations if needed
        if self._df._operations_queue:
            materialized = self._df._materialize_if_lazy()

            # Don't call count() recursively - just return the length of materialized data
            return len(materialized.data)

        if self._df._cached_count is None:
            self._df._cached_count = len(self._df.data)

        return self._df._cached_count

    def isEmpty(self) -> bool:
        """Check if DataFrame is empty (PySpark 3.3+).

        Returns:
            True if DataFrame has no rows
        """
        return len(self._df.data) == 0

    def first(self) -> Union[Row, None]:
        """Return the first row, or None if empty.

        This matches PySpark's DataFrame.first() behavior exactly:
        - Returns a single Row object (not a list)
        - Returns None if the DataFrame is empty

        Returns:
            First Row, or None if DataFrame is empty.
        """
        if self._df._operations_queue:
            materialized = self._df._materialize_if_lazy()
            return self._df._get_collection_handler().first(
                materialized.data, materialized.schema
            )
        return self._df._get_collection_handler().first(self._df.data, self._df.schema)
