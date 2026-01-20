"""
Mock data types and schema system for Sparkless.

This module provides comprehensive mock implementations of PySpark data types
and schema structures that behave identically to the real PySpark types.
Includes primitive types, complex types, schema definitions, and Row objects
for complete type system compatibility.

Key Features:
    - Complete PySpark data type hierarchy
    - Primitive types (String, Integer, Long, Double, Boolean)
    - Complex types (Array, Map, Struct)
    - Schema definition with StructType and StructField
    - Row objects with PySpark-compatible interface
    - Type inference and conversion utilities

Example:
    >>> from sparkless.spark_types import StringType, IntegerType, StructType, StructField
    >>> schema = StructType([
    ...     StructField("name", StringType()),
    ...     StructField("age", IntegerType())
    ... ])
    >>> df = spark.createDataFrame(data, schema)
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)
from dataclasses import dataclass

if TYPE_CHECKING:
    from collections.abc import ItemsView, ValuesView

# Try to import PySpark types for compatibility
try:
    from pyspark.sql.types import (
        DataType as PySparkDataType,
        StructType as PySparkStructType,
        StructField as PySparkStructField,
        StringType as PySparkStringType,
        IntegerType as PySparkIntegerType,
        LongType as PySparkLongType,
        DoubleType as PySparkDoubleType,
        BooleanType as PySparkBooleanType,
        DateType as PySparkDateType,
        TimestampType as PySparkTimestampType,
    )

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    # Create dummy base classes for type hints
    # These are only used for type checking when PySpark is not available
    # Use type: ignore[no-redef] to suppress redefinition errors
    PySparkDataType: Type[object] = object  # type: ignore[no-redef]
    PySparkStructType: Type[object] = object  # type: ignore[no-redef]
    PySparkStructField: Type[object] = object  # type: ignore[no-redef]
    PySparkStringType: Type[object] = object  # type: ignore[no-redef]
    PySparkIntegerType: Type[object] = object  # type: ignore[no-redef]
    PySparkLongType: Type[object] = object  # type: ignore[no-redef]
    PySparkDoubleType: Type[object] = object  # type: ignore[no-redef]
    PySparkBooleanType: Type[object] = object  # type: ignore[no-redef]
    PySparkDateType: Type[object] = object  # type: ignore[no-redef]
    PySparkTimestampType: Type[object] = object  # type: ignore[no-redef]


_DataTypeBase = PySparkDataType if PYSPARK_AVAILABLE else object


class DataType(_DataTypeBase):  # type: ignore[misc,valid-type]
    """Base class for mock data types.

    Provides the foundation for all data types in the Sparkless type system.
    Supports nullable/non-nullable semantics and PySpark-compatible type names.
    Inherits from PySpark DataType when available for compatibility.

    Attributes:
        nullable: Whether the data type allows null values.

    Example:
        >>> StringType()
        StringType(nullable=True)
        >>> IntegerType(nullable=False)
        IntegerType(nullable=False)
    """

    def __init__(self, nullable: bool = True):
        if PYSPARK_AVAILABLE:
            # Call PySpark parent constructor if available
            import contextlib

            with contextlib.suppress(Exception):
                super().__init__()
        self.nullable = nullable

    def __eq__(self, other: Any) -> bool:
        # For PySpark compatibility, compare only the type class
        # nullable is a field-level property, not a type-level property
        if hasattr(other, "__class__"):
            return isinstance(other, self.__class__)
        return False

    def __hash__(self) -> int:
        """Hash method to make DataType hashable."""
        return hash((self.__class__.__name__, self.nullable))

    def __repr__(self) -> str:
        # Always include nullable in representation for consistency
        if hasattr(self, "nullable"):
            return f"{self.__class__.__name__}(nullable={self.nullable})"
        else:
            # Fallback if nullable not set (shouldn't happen)
            return f"{self.__class__.__name__}()"

    def typeName(self) -> str:
        """Get PySpark-compatible type name."""
        type_mapping = {
            "StringType": "string",
            "IntegerType": "int",  # Fixed: was "integer", should be "int"
            "LongType": "long",  # PySpark uses "long", not "bigint"
            "DoubleType": "double",
            "BooleanType": "boolean",
            "DateType": "date",
            "TimestampType": "timestamp",
            "TimestampNTZType": "timestamp_ntz",
            "FloatType": "float",
            "ShortType": "smallint",
            "ByteType": "tinyint",
            "DecimalType": "decimal",
            "BinaryType": "binary",
            "NullType": "null",
            "ArrayType": "array",
            "MapType": "map",
            "StructType": "struct",
            "CharType": "char",
            "VarcharType": "varchar",
            "IntervalType": "interval",
            "YearMonthIntervalType": "interval_year_month",
            "DayTimeIntervalType": "interval_day_time",
        }
        return type_mapping.get(
            self.__class__.__name__, self.__class__.__name__.lower()
        )

    def simpleString(self) -> str:
        """Get PySpark-compatible simple string representation of the data type.

        Returns:
            Simple string representation (e.g., "string", "int", "array<string>").

        Note:
            Fixed in version 3.23.0 (Issue #231): All DataType classes now implement
            simpleString() with PySpark-compatible string representations.
        """
        return self.typeName()


class StringType(DataType):
    """Mock StringType.

    Inherits from DataType which inherits from PySpark DataType when available.
    This avoids the singleton issue while maintaining compatibility.
    """

    def __init__(self, nullable: bool = True):
        """Initialize StringType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class IntegerType(DataType):
    """Mock IntegerType.

    Inherits from DataType which inherits from PySpark DataType when available.
    """

    def __init__(self, nullable: bool = True):
        """Initialize IntegerType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class LongType(DataType):
    """Mock LongType.

    Inherits from DataType which inherits from PySpark DataType when available.
    """

    def __init__(self, nullable: bool = True):
        """Initialize LongType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class DoubleType(DataType):
    """Mock DoubleType.

    Inherits from DataType which inherits from PySpark DataType when available.
    """

    def __init__(self, nullable: bool = True):
        """Initialize DoubleType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class BooleanType(DataType):
    """Mock BooleanType.

    Inherits from DataType which inherits from PySpark DataType when available.
    """

    def __init__(self, nullable: bool = True):
        """Initialize BooleanType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class DateType(DataType):
    """Mock DateType.

    Inherits from DataType which inherits from PySpark DataType when available.
    """

    def __init__(self, nullable: bool = True):
        """Initialize DateType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class TimestampType(DataType):
    """Mock TimestampType.

    Inherits from DataType which inherits from PySpark DataType when available.
    """

    def __init__(self, nullable: bool = True):
        """Initialize TimestampType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class DecimalType(DataType):
    """Mock decimal type."""

    def __init__(self, precision: int = 10, scale: int = 0, nullable: bool = True):
        """Initialize DecimalType."""
        super().__init__(nullable)
        self.precision = precision
        self.scale = scale

    def __repr__(self) -> str:
        """String representation."""
        return f"DecimalType({self.precision}, {self.scale})"


class ArrayType(DataType):
    """Mock array type.

    Represents an array data type with PySpark-compatible initialization.
    Supports both PySpark's camelCase keyword convention and backward-compatible
    snake_case naming.

    Example:
        >>> # PySpark convention (camelCase)
        >>> ArrayType(elementType=StringType())
        >>> # Backward-compatible (snake_case)
        >>> ArrayType(element_type=StringType())
        >>> # Positional argument
        >>> ArrayType(StringType())
    """

    def __init__(
        self,
        element_type: Optional[DataType] = None,
        elementType: Optional[DataType] = None,  # PySpark keyword name
        nullable: bool = True,
    ):
        """Initialize ArrayType.

        Args:
            element_type: Element data type (positional or keyword with snake_case)
            elementType: Element data type (keyword, PySpark convention - Issue #247)
            nullable: Whether the array can contain null values

        Either element_type (positional/keyword) or elementType (keyword) must be provided.

        Raises:
            TypeError: If both elementType and element_type are provided, or if neither is provided.

        Note:
            This matches PySpark's ArrayType API. Using `elementType` keyword argument
            provides full PySpark compatibility (Issue #247).
        """
        # Handle both camelCase (PySpark) and snake_case (backward compat)
        # Issue #262: Check if elementType is actually a DataType (not a bool from positional arg)
        # If elementType is a bool, it was incorrectly matched from a positional argument
        # In that case, it should be treated as None and the bool should be nullable
        if isinstance(elementType, bool):
            # elementType was incorrectly matched from a positional argument
            # The bool value is actually the nullable parameter
            nullable = elementType
            elementType = None

        if elementType is not None and element_type is not None:
            raise TypeError("Cannot specify both 'elementType' and 'element_type'")

        # Prefer elementType (PySpark convention), fallback to element_type
        final_element_type = elementType if elementType is not None else element_type

        if final_element_type is None:
            raise TypeError("elementType or element_type is required")

        super().__init__(nullable)
        self.element_type = final_element_type

    def __repr__(self) -> str:
        """String representation."""
        return f"ArrayType({self.element_type})"

    def simpleString(self) -> str:
        """Get PySpark-compatible simple string representation."""
        return f"array<{self.element_type.simpleString()}>"


class MapType(DataType):
    """Mock map type."""

    def __init__(self, key_type: DataType, value_type: DataType, nullable: bool = True):
        """Initialize MapType."""
        super().__init__(nullable)
        self.key_type = key_type
        self.value_type = value_type

    def __repr__(self) -> str:
        """String representation."""
        return f"MapType({self.key_type}, {self.value_type})"

    def simpleString(self) -> str:
        """Get PySpark-compatible simple string representation."""
        return f"map<{self.key_type.simpleString()},{self.value_type.simpleString()}>"


class BinaryType(DataType):
    """Mock BinaryType for binary data."""

    def __init__(self, nullable: bool = True):
        """Initialize BinaryType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class NullType(DataType):
    """Mock NullType for null values."""

    def __init__(self, nullable: bool = True):
        """Initialize NullType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class FloatType(DataType):
    """Mock FloatType for single precision floating point numbers."""

    def __init__(self, nullable: bool = True):
        """Initialize FloatType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class ShortType(DataType):
    """Mock ShortType for short integers."""

    def __init__(self, nullable: bool = True):
        """Initialize ShortType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class ByteType(DataType):
    """Mock ByteType for byte values."""

    def __init__(self, nullable: bool = True):
        """Initialize ByteType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class CharType(DataType):
    """Mock CharType for fixed-length character strings."""

    def __init__(self, length: int = 1, nullable: bool = True):
        super().__init__(nullable)
        self.length = length

    def __repr__(self) -> str:
        return f"CharType({self.length})"


class VarcharType(DataType):
    """Mock VarcharType for variable-length character strings."""

    def __init__(self, length: int = 255, nullable: bool = True):
        super().__init__(nullable)
        self.length = length

    def __repr__(self) -> str:
        return f"VarcharType({self.length})"


class TimestampNTZType(DataType):
    """Mock TimestampNTZType for timestamp without timezone."""

    def __init__(self, nullable: bool = True):
        """Initialize TimestampNTZType.

        Args:
            nullable: Whether the type allows null values.
        """
        super().__init__(nullable)


class IntervalType(DataType):
    """Mock IntervalType for time intervals."""

    def __init__(
        self, start_field: str = "YEAR", end_field: str = "MONTH", nullable: bool = True
    ):
        super().__init__(nullable)
        self.start_field = start_field
        self.end_field = end_field

    def __repr__(self) -> str:
        return f"IntervalType({self.start_field}, {self.end_field})"


class YearMonthIntervalType(DataType):
    """Mock YearMonthIntervalType for year-month intervals."""

    def __init__(
        self, start_field: str = "YEAR", end_field: str = "MONTH", nullable: bool = True
    ):
        super().__init__(nullable)
        self.start_field = start_field
        self.end_field = end_field

    def __repr__(self) -> str:
        return f"YearMonthIntervalType({self.start_field}, {self.end_field})"


class DayTimeIntervalType(DataType):
    """Mock DayTimeIntervalType for day-time intervals."""

    def __init__(
        self, start_field: str = "DAY", end_field: str = "SECOND", nullable: bool = True
    ):
        super().__init__(nullable)
        self.start_field = start_field
        self.end_field = end_field

    def __repr__(self) -> str:
        return f"DayTimeIntervalType({self.start_field}, {self.end_field})"


@dataclass
class StructField(PySparkStructField if PYSPARK_AVAILABLE else object):  # type: ignore[misc]
    """Mock StructField for schema definition.

    Inherits from PySpark StructField when available for compatibility.
    """

    name: str
    dataType: DataType
    nullable: bool = True
    metadata: Optional[Dict[str, Any]] = None
    default_value: Optional[Any] = None  # PySpark 3.2+ feature

    def __post_init__(self) -> None:
        if self.metadata is None:
            self.metadata = {}
        # Add field_type attribute for compatibility
        self.field_type = self.dataType

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, StructField)
            and self.name == other.name
            and self.dataType == other.dataType
            and self.nullable == other.nullable
        )

    def __repr__(self) -> str:
        default_str = (
            f", default_value={self.default_value!r}"
            if self.default_value is not None
            else ""
        )
        return f"StructField(name='{self.name}', dataType={self.dataType}, nullable={self.nullable}{default_str})"


class StructType(
    PySparkStructType if PYSPARK_AVAILABLE else DataType  # type: ignore[misc]
):
    """Mock StructType for schema definition.

    Inherits from PySpark StructType when available for compatibility.
    """

    def __init__(
        self, fields: Optional[List[StructField]] = None, nullable: bool = True
    ):
        if PYSPARK_AVAILABLE:
            # PySpark StructType expects fields as first argument
            # Convert sparkless StructFields to PySpark StructFields if needed
            if fields:
                # PySpark StructType will handle the fields
                try:
                    super().__init__(fields)
                    # Ensure fields attribute exists (PySpark might set it differently)
                    if (
                        not hasattr(self, "fields")
                        or getattr(self, "fields", None) != fields
                    ):
                        object.__setattr__(self, "fields", (fields or []))
                except Exception:
                    # If PySpark init fails, fall back to our implementation
                    if not hasattr(self, "fields"):
                        object.__setattr__(self, "fields", (fields or []))
                    DataType.__init__(self, nullable)
            else:
                super().__init__([])
                self.fields: List[StructField] = []
            # Always initialize _field_map after fields are set
            if hasattr(self, "fields") and self.fields:
                self._field_map = {field.name: field for field in self.fields}
            else:
                self._field_map = {}
        else:
            DataType.__init__(self, nullable)
            self.fields = fields or []
            if fields:
                self._field_map = {field.name: field for field in self.fields}
            else:
                self._field_map = {}

    def __getitem__(self, index: int) -> StructField:
        return self.fields[index]

    def __len__(self) -> int:
        return len(self.fields)

    def __iter__(self) -> Iterator[StructField]:
        return iter(self.fields)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, StructType) and self.fields == other.fields

    def __repr__(self) -> str:
        fields_str = ", ".join(repr(field) for field in self.fields)
        return f"StructType([{fields_str}])"

    def simpleString(self) -> str:
        """Get PySpark-compatible simple string representation."""
        fields_str = ",".join(
            f"{field.name}:{field.dataType.simpleString()}" for field in self.fields
        )
        return f"struct<{fields_str}>"

    def merge_with(self, other: StructType) -> StructType:
        """Merge this schema with another, adding new fields from other.

        Args:
            other: Schema to merge with

        Returns:
            New schema with fields from both schemas
        """
        # Create dict of existing fields by name
        existing_fields = {f.name: f for f in self.fields}

        # Add fields from other that don't exist
        merged_fields = list(self.fields)  # Start with current fields
        for field in other.fields:
            if field.name not in existing_fields:
                merged_fields.append(field)

        return StructType(merged_fields)

    def has_same_columns(self, other: StructType) -> bool:
        """Check if two schemas have the same column names.

        Args:
            other: Schema to compare with

        Returns:
            True if column names match, False otherwise
        """
        self_cols = {f.name for f in self.fields}
        other_cols = {f.name for f in other.fields}
        return self_cols == other_cols

    def fieldNames(self) -> List[str]:
        """Get list of field names."""
        return [field.name for field in self.fields]

    def getFieldIndex(self, name: str) -> int:
        """Get index of field by name."""
        if name not in self._field_map:
            raise ValueError(f"Field '{name}' not found in schema")
        return self.fields.index(self._field_map[name])

    def contains(self, name: str) -> bool:
        """Check if field exists in schema."""
        return name in self._field_map

    def add_field(self, field: StructField) -> None:
        """Add a field to the struct type."""
        self.fields.append(field)
        self._field_map[field.name] = field

    def get_field_by_name(self, name: str) -> Optional[StructField]:
        """Get field by name."""
        return self._field_map.get(name)

    def has_field(self, name: str) -> bool:
        """Check if field exists in schema."""
        return name in self._field_map


@dataclass
class MockDatabase:
    """Mock database representation."""

    name: str
    description: Optional[str] = None
    locationUri: Optional[str] = None

    def __repr__(self) -> str:
        return f"MockDatabase(name='{self.name}')"


@dataclass
class MockTable:
    """Mock table representation."""

    name: str
    database: str
    tableType: str = "MANAGED"
    isTemporary: bool = False

    def __repr__(self) -> str:
        return f"MockTable(name='{self.name}', database='{self.database}')"


# Type conversion utilities
def convert_python_type_to_mock_type(python_type: type) -> DataType:
    """Convert Python type to DataType."""
    type_mapping = {
        str: StringType(),
        int: LongType(),  # Use LongType for integers to match PySpark
        float: DoubleType(),
        bool: BooleanType(),
        bytes: BinaryType(),
        type(None): NullType(),
    }

    return type_mapping.get(python_type, StringType())


def infer_schema_from_data(data: List[Dict[str, Any]]) -> StructType:
    """Infer schema from data."""
    if not data:
        return StructType([])

    # Get field names and types from first row
    first_row = data[0]
    fields = []

    for name, value in first_row.items():
        if value is None:
            data_type: DataType = StringType()
        else:
            data_type = convert_python_type_to_mock_type(type(value))

        fields.append(StructField(name=name, dataType=data_type))

    return StructType(fields)


def create_schema_from_columns(columns: List[str]) -> StructType:
    """Create schema from column names (all StringType)."""
    fields = [StructField(name=col, dataType=StringType()) for col in columns]
    return StructType(fields)


class Row:
    """Mock Row object providing PySpark-compatible row interface.

    Represents a single row in a DataFrame with PySpark-compatible methods
    for accessing data by index, key, or attribute.

    Attributes:
        data: Dictionary containing row data.

    Example:
        >>> row = Row({"name": "Alice", "age": 25})
        >>> row.name
        'Alice'
        >>> row[0]
        'Alice'
        >>> row.asDict()
        {'name': 'Alice', 'age': 25}
    """

    def __init__(
        self, data: Any = None, schema: Optional[StructType] = None, **kwargs: Any
    ):
        """Initialize Row.

        Args:
            data: Row data. Accepts dict, list of tuples, or sequence-like.
                  If None and kwargs are provided, kwargs are used as data (PySpark-compatible).
            schema: Optional schema providing ordered field names for index access.
            **kwargs: Optional keyword arguments for kwargs-style initialization (PySpark-compatible).
                     Example: Row(Column1="Value1", Column2=2)

        Example:
            >>> row = Row({"name": "Alice", "age": 25})
            >>> row = Row(name="Alice", age=25)  # kwargs-style
            >>> row.name
            'Alice'
        """
        # PySpark compatibility: if data is None and kwargs are provided, use kwargs as data
        if data is None and kwargs:
            data = kwargs

        self._schema = schema

        # Handle list of tuples - preserves duplicate column names
        if (
            isinstance(data, (list, tuple))
            and len(data) > 0
            and isinstance(data[0], (list, tuple))
        ):
            # List of (name, value) tuples - preserve duplicates
            self.data: Union[List[Tuple[str, Any]], Dict[str, Any]] = list(
                data
            )  # Keep as list
            self._data_dict = dict(data)  # For backward compatibility
        elif isinstance(data, dict):
            if schema is not None and getattr(schema, "fields", None):
                # Reorder dict according to schema field order
                ordered_items = [(f.name, data.get(f.name)) for f in schema.fields]
                self.data = list(ordered_items)  # Store as list of tuples
                self._data_dict = dict(ordered_items)  # For backward compatibility
            else:
                self.data = list(data.items())
                self._data_dict = dict(data)
        else:
            # sequence-like data paired with schema
            if schema is None or not getattr(schema, "fields", None):
                raise ValueError("Sequence row data requires a schema with fields")
            values = list(data)
            names = [f.name for f in schema.fields]
            # If values shorter/longer, pad/truncate to schema length
            if len(values) < len(names):
                values = values + [None] * (len(names) - len(values))
            if len(values) > len(names):
                values = values[: len(names)]
            self.data = list(zip(names, values))  # Store as list of tuples
            self._data_dict = {name: values[idx] for idx, name in enumerate(names)}

    def __getitem__(self, key: Any) -> Any:
        """Get item by column name or index (PySpark-compatible)."""
        if isinstance(key, str):
            # Use dict for backward compatibility
            if hasattr(self, "_data_dict"):
                if key not in self._data_dict:
                    raise KeyError(f"Key '{key}' not found in row")
                return self._data_dict[key]
            # Fallback for old format - check if data is dict or list
            data_dict = self.data if isinstance(self.data, dict) else dict(self.data)
            if key not in data_dict:
                raise KeyError(f"Key '{key}' not found in row")
            return data_dict[key]
        # Support integer index access using schema order
        if isinstance(key, int):
            # If data is list of tuples, access directly
            if isinstance(self.data, list) and len(self.data) > 0:
                if key >= len(self.data):
                    raise IndexError("Row index out of range")
                return self.data[key][1]  # Return value (second element)
            # Fallback for dict format
            field_names = self._get_field_names_ordered()
            try:
                name = field_names[key]
            except IndexError:
                raise IndexError("Row index out of range")
            if hasattr(self, "_data_dict"):
                return self._data_dict.get(name)
            data_dict = self.data if isinstance(self.data, dict) else dict(self.data)
            return data_dict.get(name)
        raise TypeError("Row indices must be integers or strings")

    def __contains__(self, key: str) -> bool:
        """Check if key exists."""
        if hasattr(self, "_data_dict"):
            return key in self._data_dict
        if isinstance(self.data, list):
            return any(k == key for k, v in self.data)
        return key in self.data

    def values(self) -> ValuesView[Any]:
        """Get values."""
        if hasattr(self, "_data_dict"):
            return self._data_dict.values()
        if isinstance(self.data, list):
            from collections import OrderedDict

            return OrderedDict(self.data).values()
        data_dict = self.data if isinstance(self.data, dict) else dict(self.data)
        return data_dict.values()

    def items(self) -> ItemsView[str, Any]:
        """Get items."""
        if hasattr(self, "_data_dict"):
            return self._data_dict.items()
        if isinstance(self.data, list):
            from collections import OrderedDict

            return OrderedDict(self.data).items()
        data_dict = self.data if isinstance(self.data, dict) else dict(self.data)
        return data_dict.items()

    def __len__(self) -> int:
        """Get length."""
        return len(self.data)

    def __eq__(self, other: Any) -> bool:
        """Compare with another row object."""
        if hasattr(other, "data"):
            # Compare with another Row
            result: bool = self.data == other.data
            return result
        elif hasattr(other, "__dict__"):
            # Compare with PySpark Row object
            # PySpark Row objects have attributes for each column
            try:
                from collections import OrderedDict

                data_dict: Union[Dict[str, Any], OrderedDict[str, Any]]
                if isinstance(self.data, list):
                    data_dict = OrderedDict(self.data)
                elif isinstance(self.data, dict):
                    data_dict = dict(self.data)
                else:
                    # self.data is dict-like (has items() method)
                    data_dict = dict(self.data)  # type: ignore[unreachable]
                for key, value in data_dict.items():
                    if not hasattr(other, key) or getattr(other, key) != value:
                        return False
                return True
            except Exception:
                return False
        else:
            return False

    def asDict(self) -> Dict[str, Any]:
        """Convert to dictionary (PySpark compatibility)."""
        # If we have _data_dict, use it (last value for duplicates)
        if hasattr(self, "_data_dict"):
            if self._schema is not None:
                # Return in schema order with last value for duplicates
                return {
                    name: self._data_dict.get(name)
                    for name in self._get_field_names_ordered()
                }
            return self._data_dict
        # Handle list of tuples format
        if (
            isinstance(self.data, list)
            and len(self.data) > 0
            and isinstance(self.data[0], (list, tuple))
        ):
            # Convert list of tuples to dict (last value for duplicates)
            result = dict(self.data)
            if self._schema is not None:
                # Return in schema order
                return {
                    name: result.get(name) for name in self._get_field_names_ordered()
                }
            return result
        # Fallback for dict format
        data_dict = self.data if isinstance(self.data, dict) else dict(self.data)
        if self._schema is not None:
            return {
                name: data_dict.get(name) for name in self._get_field_names_ordered()
            }
        if isinstance(self.data, dict):
            return self.data.copy()
        return dict(self.data)

    def __getattr__(self, name: str) -> Any:
        """Get value by attribute name (PySpark compatibility)."""
        if isinstance(self.data, dict):
            if name in self.data:
                return self.data[name]
        elif isinstance(self.data, list):
            data_dict = dict(self.data)
            if name in data_dict:
                return data_dict[name]
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def __iter__(self) -> Iterator[Any]:
        """Iterate values in schema order if available, else dict order."""
        data_dict = self.data if isinstance(self.data, dict) else dict(self.data)
        for name in self._get_field_names_ordered():
            yield data_dict.get(name)

    def __repr__(self) -> str:
        """String representation matching PySpark format."""
        data_dict = self.data if isinstance(self.data, dict) else dict(self.data)
        values_str = ", ".join(
            f"{k}={data_dict.get(k)}" for k in self._get_field_names_ordered()
        )
        return f"Row({values_str})"

    def get(self, key: str, default: Any = None) -> Any:
        """Get value by key with default."""
        data_dict = self.data if isinstance(self.data, dict) else dict(self.data)
        return data_dict.get(key, default)

    def _get_field_names_ordered(self) -> List[str]:
        if self._schema is not None and getattr(self._schema, "fields", None):
            return [f.name for f in self._schema.fields]
        # fallback to dict insertion order
        data_dict = self.data if isinstance(self.data, dict) else dict(self.data)
        return list(data_dict.keys())
