"""
Schema type definitions.

This module defines the abstract interfaces for schema operations,
including struct types, field definitions, and schema management.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .data_types import IDataType


class IStructField(ABC):
    """Abstract interface for struct field operations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get field name."""
        pass

    @property
    @abstractmethod
    def data_type(self) -> "IDataType":
        """Get field data type."""
        pass

    @property
    @abstractmethod
    def nullable(self) -> bool:
        """Check if field is nullable."""
        pass

    @property
    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """Get field metadata."""
        pass

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        """Check equality with another field."""
        pass

    @abstractmethod
    def __hash__(self) -> int:
        """Get hash for field."""
        pass

    @abstractmethod
    def __str__(self) -> str:
        """Get string representation."""
        pass

    @abstractmethod
    def __repr__(self) -> str:
        """Get representation."""
        pass


class IStructType(ABC):
    """Abstract interface for struct type operations."""

    @property
    @abstractmethod
    def fields(self) -> List[IStructField]:
        """Get struct fields."""
        pass

    @abstractmethod
    def add_field(self, field: IStructField) -> None:
        """Add field to struct."""
        pass

    @abstractmethod
    def remove_field(self, field_name: str) -> None:
        """Remove field from struct."""
        pass

    @abstractmethod
    def get_field(self, field_name: str) -> Optional[IStructField]:
        """Get field by name."""
        pass

    @abstractmethod
    def field_names(self) -> List[str]:
        """Get field names."""
        pass

    @abstractmethod
    def field_types(self) -> Dict[str, "IDataType"]:
        """Get field types."""
        pass

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        """Check equality with another struct."""
        pass

    @abstractmethod
    def __hash__(self) -> int:
        """Get hash for struct."""
        pass

    @abstractmethod
    def __str__(self) -> str:
        """Get string representation."""
        pass

    @abstractmethod
    def __repr__(self) -> str:
        """Get representation."""
        pass


class ISchema(ABC):
    """Abstract interface for schema operations."""

    @property
    @abstractmethod
    def fields(self) -> List[IStructField]:
        """Get schema fields."""
        pass

    @abstractmethod
    def add_field(self, field: IStructField) -> None:
        """Add field to schema."""
        pass

    @abstractmethod
    def remove_field(self, field_name: str) -> None:
        """Remove field from schema."""
        pass

    @abstractmethod
    def get_field(self, field_name: str) -> Optional[IStructField]:
        """Get field by name."""
        pass

    @abstractmethod
    def field_names(self) -> List[str]:
        """Get field names."""
        pass

    @abstractmethod
    def field_types(self) -> Dict[str, "IDataType"]:
        """Get field types."""
        pass

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        """Check equality with another schema."""
        pass

    @abstractmethod
    def __hash__(self) -> int:
        """Get hash for schema."""
        pass

    @abstractmethod
    def __str__(self) -> str:
        """Get string representation."""
        pass

    @abstractmethod
    def __repr__(self) -> str:
        """Get representation."""
        pass


class ISchemaBuilder(ABC):
    """Abstract interface for schema building."""

    @abstractmethod
    def add_field(
        self,
        name: str,
        data_type: "IDataType",
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "ISchemaBuilder":
        """Add field to schema."""
        pass

    @abstractmethod
    def add_fields(self, fields: List[IStructField]) -> "ISchemaBuilder":
        """Add multiple fields to schema."""
        pass

    @abstractmethod
    def build(self) -> IStructType:
        """Build the schema."""
        pass

    @abstractmethod
    def clear(self) -> "ISchemaBuilder":
        """Clear all fields."""
        pass


class ISchemaValidator(ABC):
    """Abstract interface for schema validation."""

    @abstractmethod
    def validate_schema(self, schema: IStructType) -> bool:
        """Validate schema structure."""
        pass

    @abstractmethod
    def validate_field(self, field: IStructField) -> bool:
        """Validate field structure."""
        pass

    @abstractmethod
    def validate_data_against_schema(
        self, data: List[Dict[str, Any]], schema: IStructType
    ) -> bool:
        """Validate data against schema."""
        pass

    @abstractmethod
    def get_validation_errors(self, schema: IStructType) -> List[str]:
        """Get validation errors."""
        pass
