"""
Schema operations mixin for DataFrame.

This mixin provides schema-related properties and operations that can be
        mixed into the DataFrame class to add schema capabilities.
"""

from typing import Generic, List, Tuple, TypeVar

from ..protocols import SupportsDataFrameOps
from ...spark_types import StructType

DataFrameT = TypeVar("DataFrameT", bound=SupportsDataFrameOps)


class SchemaOperations(Generic[DataFrameT]):
    """Mixin providing schema-related operations for DataFrame."""

    @property
    def columns(self: DataFrameT) -> List[str]:
        """Get column names."""
        # Get schema (handles lazy evaluation)
        current_schema = self.schema

        # Ensure schema has fields attribute
        if not hasattr(current_schema, "fields"):
            return []

        # Return column names from schema fields
        # This works even for empty DataFrames with explicit schemas
        return [field.name for field in current_schema.fields]

    @property
    def schema(self: DataFrameT) -> StructType:
        """Get DataFrame schema.

        If lazy with queued operations, project the resulting schema without materializing data.
        """
        if self._operations_queue:
            return self._project_schema_with_operations()
        return self._schema

    @schema.setter
    def schema(self: DataFrameT, value: StructType) -> None:
        """Set DataFrame schema."""
        self._schema = value

    @property
    def dtypes(self: DataFrameT) -> List[Tuple[str, str]]:
        """Get column names and their data types."""
        return [(field.name, field.dataType.typeName()) for field in self.schema.fields]
