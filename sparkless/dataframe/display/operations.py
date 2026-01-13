"""
Display operations mixin for DataFrame.

This mixin provides display and collection operations that can be mixed into
the DataFrame class to add display capabilities.
"""

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, Union, cast

from ...spark_types import Row, StringType, StructField, StructType
from ..protocols import SupportsDataFrameOps

if TYPE_CHECKING:
    from ..protocols import CollectionHandler


class DisplayOperations:
    """Mixin providing display and collection operations for DataFrame."""

    if TYPE_CHECKING:
        data: List[Dict[str, Any]]
        schema: StructType
        _operations_queue: List[Tuple[str, Any]]
        _cached_count: Optional[int]

        def _materialize_if_lazy(self) -> SupportsDataFrameOps: ...

        def _get_collection_handler(self) -> "CollectionHandler": ...

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
        if self._operations_queue:
            materialized = cast("DisplayOperations", self._materialize_if_lazy())
            return materialized.show(n, truncate)

        print(f"DataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]")
        if not self.data:
            print("(empty)")
            return

        # Show first n rows
        display_data = self.data[:n]

        # Get column names
        columns = (
            list(display_data[0].keys()) if display_data else self.schema.fieldNames()
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

        if len(self.data) > n:
            print(f"\n... ({len(self.data) - n} more rows)")

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
        if not self.data:
            return f"DataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]\n\n(empty)"

        # Show first n rows
        display_data = self.data[:n]

        # Get column names
        columns = (
            list(display_data[0].keys()) if display_data else self.schema.fieldNames()
        )

        # Build markdown table
        lines = []
        lines.append(
            f"DataFrame[{len(self.data)} rows, {len(self.schema.fields)} columns]"
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

        if len(self.data) > n:
            lines.append(f"\n... ({len(self.data) - n} more rows)")

        return "\n".join(lines)

    def printSchema(self) -> None:
        """Print DataFrame schema."""
        print("DataFrame Schema:")
        for field in self.schema.fields:
            nullable = "nullable" if field.nullable else "not nullable"
            print(
                f" |-- {field.name}: {field.dataType.__class__.__name__} ({nullable})"
            )

    def collect(self) -> List[Row]:
        """Collect all data as list of Row objects."""

        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            result = self._get_collection_handler().collect(
                materialized.data, materialized.schema
            )
            return result
        result = self._get_collection_handler().collect(self.data, self.schema)
        return result

    def take(self, n: int) -> List[Row]:
        """Take first n rows as list of Row objects."""

        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            result = self._get_collection_handler().take(
                materialized.data, materialized.schema, n
            )
            return result
        result = self._get_collection_handler().take(self.data, self.schema, n)
        return result

    def head(self, n: int = 1) -> List[Row]:
        """Return first n rows. Always returns a list, matching PySpark behavior."""

        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            result = self._get_collection_handler().head(
                materialized.data, materialized.schema, n
            )
        else:
            result = self._get_collection_handler().head(self.data, self.schema, n)

        # Ensure we always return a list (PySpark behavior)
        # Defensive check: collection_handler.head() may return None
        # Note: mypy sees this as unreachable because collection_handler.head()
        # always returns a list in practice, but type annotation allows None
        if result is None:
            return []  # type: ignore[unreachable]
        # result from collection_handler.head() may be Row or List[Row]
        if isinstance(result, list):
            return result
        # Type narrowing: if we reach here, result is not None and not a list
        # Wrap single Row in list (defensive code, unlikely to execute)
        return [result]  # type: ignore[unreachable]

    def tail(self, n: int = 1) -> Union[Row, List[Row], None]:
        """Return last n rows."""
        from typing import cast

        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            result = self._get_collection_handler().tail(
                materialized.data, materialized.schema, n
            )
            # Protocol method returns Union but mypy sees it as Any
            return cast("Union[Row, List[Row], None]", result)
        result = self._get_collection_handler().tail(self.data, self.schema, n)
        # Protocol method returns Union but mypy sees it as Any
        return cast("Union[Row, List[Row], None]", result)

    def toPandas(self) -> Any:
        """Convert to pandas DataFrame (requires pandas as optional dependency)."""
        from ..export import DataFrameExporter

        return DataFrameExporter.to_pandas(cast("SupportsDataFrameOps", self))

    def toJSON(self: SupportsDataFrameOps) -> SupportsDataFrameOps:
        """Return a single-column DataFrame of JSON strings."""
        import json

        json_rows = [{"value": json.dumps(row)} for row in self.data]

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(
                json_rows,
                StructType([StructField("value", StringType())]),
                self.storage,
            ),
        )

    def count(self) -> int:
        """Count number of rows."""
        # Materialize lazy operations if needed
        if self._operations_queue:
            materialized = self._materialize_if_lazy()
            # Don't call count() recursively - just return the length of materialized data
            return len(materialized.data)

        if self._cached_count is None:
            self._cached_count = len(self.data)
        return self._cached_count

    def isEmpty(self) -> bool:
        """Check if DataFrame is empty (PySpark 3.3+).

        Returns:
            True if DataFrame has no rows
        """
        return len(self.data) == 0
