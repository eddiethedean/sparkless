"""
Metadata type definitions.

This module defines the abstract interfaces for metadata operations,
including table metadata, field metadata, and general metadata management.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


class IMetadata(ABC):
    """Abstract interface for metadata operations."""

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """Get metadata value."""
        pass

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Set metadata value."""
        pass

    @abstractmethod
    def has(self, key: str) -> bool:
        """Check if key exists."""
        pass

    @abstractmethod
    def remove(self, key: str) -> None:
        """Remove metadata key."""
        pass

    @abstractmethod
    def keys(self) -> List[str]:
        """Get all keys."""
        pass

    @abstractmethod
    def values(self) -> List[Any]:
        """Get all values."""
        pass

    @abstractmethod
    def items(self) -> List[Tuple[Any, Any]]:
        """Get all items."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all metadata."""
        pass

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        pass

    @abstractmethod
    def from_dict(self, data: Dict[str, Any]) -> None:
        """Load from dictionary."""
        pass


class ITableMetadata(IMetadata):
    """Abstract interface for table metadata operations."""

    @property
    @abstractmethod
    def schema_name(self) -> str:
        """Get schema name."""
        pass

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Get table name."""
        pass

    @property
    @abstractmethod
    def created_at(self) -> float:
        """Get creation timestamp."""
        pass

    @property
    @abstractmethod
    def updated_at(self) -> float:
        """Get update timestamp."""
        pass

    @property
    @abstractmethod
    def row_count(self) -> int:
        """Get row count."""
        pass

    @property
    @abstractmethod
    def size_bytes(self) -> int:
        """Get size in bytes."""
        pass

    @abstractmethod
    def update_row_count(self, count: int) -> None:
        """Update row count."""
        pass

    @abstractmethod
    def update_size(self, size_bytes: int) -> None:
        """Update size."""
        pass

    @abstractmethod
    def update_timestamp(self) -> None:
        """Update timestamp."""
        pass


class IFieldMetadata(IMetadata):
    """Abstract interface for field metadata operations."""

    @property
    @abstractmethod
    def field_name(self) -> str:
        """Get field name."""
        pass

    @property
    @abstractmethod
    def data_type(self) -> str:
        """Get data type."""
        pass

    @property
    @abstractmethod
    def nullable(self) -> bool:
        """Check if nullable."""
        pass

    @abstractmethod
    def set_data_type(self, data_type: str) -> None:
        """Set data type."""
        pass

    @abstractmethod
    def set_nullable(self, nullable: bool) -> None:
        """Set nullable flag."""
        pass


class ISchemaMetadata(IMetadata):
    """Abstract interface for schema metadata operations."""

    @property
    @abstractmethod
    def schema_name(self) -> str:
        """Get schema name."""
        pass

    @property
    @abstractmethod
    def created_at(self) -> float:
        """Get creation timestamp."""
        pass

    @property
    @abstractmethod
    def table_count(self) -> int:
        """Get table count."""
        pass

    @abstractmethod
    def add_table(self, table_name: str) -> None:
        """Add table to schema."""
        pass

    @abstractmethod
    def remove_table(self, table_name: str) -> None:
        """Remove table from schema."""
        pass

    @abstractmethod
    def list_tables(self) -> List[str]:
        """List tables in schema."""
        pass


class IColumnMetadata(IMetadata):
    """Abstract interface for column metadata operations."""

    @property
    @abstractmethod
    def column_name(self) -> str:
        """Get column name."""
        pass

    @property
    @abstractmethod
    def data_type(self) -> str:
        """Get data type."""
        pass

    @property
    @abstractmethod
    def nullable(self) -> bool:
        """Check if nullable."""
        pass

    @property
    @abstractmethod
    def default_value(self) -> Any:
        """Get default value."""
        pass

    @abstractmethod
    def set_data_type(self, data_type: str) -> None:
        """Set data type."""
        pass

    @abstractmethod
    def set_nullable(self, nullable: bool) -> None:
        """Set nullable flag."""
        pass

    @abstractmethod
    def set_default_value(self, value: Any) -> None:
        """Set default value."""
        pass


class IStatisticsMetadata(IMetadata):
    """Abstract interface for statistics metadata operations."""

    @abstractmethod
    def get_statistics(self, column_name: str) -> Dict[str, Any]:
        """Get column statistics."""
        pass

    @abstractmethod
    def set_statistics(self, column_name: str, stats: Dict[str, Any]) -> None:
        """Set column statistics."""
        pass

    @abstractmethod
    def update_statistics(self, column_name: str, **stats: Any) -> None:
        """Update column statistics."""
        pass

    @abstractmethod
    def clear_statistics(self, column_name: str) -> None:
        """Clear column statistics."""
        pass

    @abstractmethod
    def list_columns_with_stats(self) -> List[str]:
        """List columns with statistics."""
        pass


class IMetadataRegistry(ABC):
    """Abstract interface for metadata registry operations."""

    @abstractmethod
    def register_metadata(self, key: str, metadata: IMetadata) -> None:
        """Register metadata."""
        pass

    @abstractmethod
    def get_metadata(self, key: str) -> Optional[IMetadata]:
        """Get metadata by key."""
        pass

    @abstractmethod
    def remove_metadata(self, key: str) -> None:
        """Remove metadata."""
        pass

    @abstractmethod
    def list_metadata_keys(self) -> List[str]:
        """List all metadata keys."""
        pass

    @abstractmethod
    def clear_all(self) -> None:
        """Clear all metadata."""
        pass
