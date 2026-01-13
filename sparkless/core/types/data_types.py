"""
Data type definitions.

This module defines the abstract interfaces for data types,
ensuring type safety and consistency across the system.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .schema import IStructField


class IDataType(ABC):
    """Abstract interface for all data types."""

    @property
    @abstractmethod
    def type_name(self) -> str:
        """Get type name."""
        pass

    @abstractmethod
    def is_nullable(self) -> bool:
        """Check if type is nullable."""
        pass

    @abstractmethod
    def validate_value(self, value: Any) -> bool:
        """Validate value against type."""
        pass

    @abstractmethod
    def convert_value(self, value: Any) -> Any:
        """Convert value to type."""
        pass

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        """Check equality with another type."""
        pass

    @abstractmethod
    def __hash__(self) -> int:
        """Get hash for type."""
        pass

    @abstractmethod
    def __str__(self) -> str:
        """Get string representation."""
        pass

    @abstractmethod
    def __repr__(self) -> str:
        """Get representation."""
        pass


class IPrimitiveType(IDataType):
    """Abstract interface for primitive data types."""

    @abstractmethod
    def get_default_value(self) -> Any:
        """Get default value for type."""
        pass


class IStringType(IPrimitiveType):
    """Abstract interface for string data type."""

    @property
    @abstractmethod
    def max_length(self) -> Optional[int]:
        """Get maximum length."""
        pass

    @abstractmethod
    def validate_length(self, value: str) -> bool:
        """Validate string length."""
        pass


class IIntegerType(IPrimitiveType):
    """Abstract interface for integer data type."""

    @property
    @abstractmethod
    def min_value(self) -> int:
        """Get minimum value."""
        pass

    @property
    @abstractmethod
    def max_value(self) -> int:
        """Get maximum value."""
        pass

    @abstractmethod
    def validate_range(self, value: int) -> bool:
        """Validate integer range."""
        pass


class IBooleanType(IPrimitiveType):
    """Abstract interface for boolean data type."""

    @abstractmethod
    def validate_boolean(self, value: Any) -> bool:
        """Validate boolean value."""
        pass


class IFloatType(IPrimitiveType):
    """Abstract interface for float data type."""

    @property
    @abstractmethod
    def precision(self) -> int:
        """Get precision."""
        pass

    @property
    @abstractmethod
    def scale(self) -> int:
        """Get scale."""
        pass

    @abstractmethod
    def validate_precision(self, value: float) -> bool:
        """Validate precision."""
        pass


class IDateType(IPrimitiveType):
    """Abstract interface for date data type."""

    @abstractmethod
    def validate_date(self, value: Any) -> bool:
        """Validate date value."""
        pass

    @abstractmethod
    def parse_date(self, value: str) -> Any:
        """Parse date from string."""
        pass


class ITimestampType(IPrimitiveType):
    """Abstract interface for timestamp data type."""

    @abstractmethod
    def validate_timestamp(self, value: Any) -> bool:
        """Validate timestamp value."""
        pass

    @abstractmethod
    def parse_timestamp(self, value: str) -> Any:
        """Parse timestamp from string."""
        pass


class IArrayType(IDataType):
    """Abstract interface for array data type."""

    @property
    @abstractmethod
    def element_type(self) -> IDataType:
        """Get element type."""
        pass

    @abstractmethod
    def validate_array(self, value: List[Any]) -> bool:
        """Validate array value."""
        pass

    @abstractmethod
    def validate_element(self, value: Any) -> bool:
        """Validate array element."""
        pass


class IMapType(IDataType):
    """Abstract interface for map data type."""

    @property
    @abstractmethod
    def key_type(self) -> IDataType:
        """Get key type."""
        pass

    @property
    @abstractmethod
    def value_type(self) -> IDataType:
        """Get value type."""
        pass

    @abstractmethod
    def validate_map(self, value: Dict[Any, Any]) -> bool:
        """Validate map value."""
        pass

    @abstractmethod
    def validate_key(self, key: Any) -> bool:
        """Validate map key."""
        pass

    @abstractmethod
    def validate_value(self, value: Any) -> bool:
        """Validate map value."""
        pass


class IStructTypeLegacy(IDataType):
    """Abstract interface for struct data type (legacy - use IStructType from schema instead)."""

    @property
    @abstractmethod
    def fields(self) -> List["IStructField"]:
        """Get struct fields."""
        pass

    @abstractmethod
    def get_field(self, name: str) -> Optional["IStructField"]:
        """Get field by name."""
        pass

    @abstractmethod
    def validate_struct(self, value: Dict[str, Any]) -> bool:
        """Validate struct value."""
        pass


class IBinaryType(IPrimitiveType):
    """Abstract interface for binary data type."""

    @abstractmethod
    def validate_binary(self, value: Any) -> bool:
        """Validate binary value."""
        pass


class IDataTypeRegistry(ABC):
    """Abstract interface for data type registry."""

    @abstractmethod
    def register_type(self, type_name: str, type_class: type) -> None:
        """Register data type."""
        pass

    @abstractmethod
    def get_type(self, type_name: str) -> Optional[type]:
        """Get type by name."""
        pass

    @abstractmethod
    def list_types(self) -> List[str]:
        """List all registered types."""
        pass

    @abstractmethod
    def create_type(self, type_name: str, **kwargs: Any) -> IDataType:
        """Create type instance."""
        pass
