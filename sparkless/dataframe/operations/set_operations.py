"""Set operations for DataFrame."""

from typing import Any, Dict, List, TYPE_CHECKING, Tuple, Type

from ...spark_types import (
    Row,
    DataType,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    ShortType,
    ByteType,
    StructField,
)

if TYPE_CHECKING:
    from ...spark_types import StructType


class SetOperations:
    """Handles set operations for DataFrame."""

    @staticmethod
    def distinct_rows(rows: List[Row]) -> List[Row]:
        """Remove duplicate rows from a list."""
        seen = set()
        unique_rows = []
        for row in rows:
            # Create a hashable representation of the row
            row_values = []
            # Handle Row objects - they may have data attribute or __dict__
            row_dict = {}
            if hasattr(row, "data"):
                if isinstance(row.data, dict):
                    row_dict = row.data
                elif isinstance(row.data, list):
                    # List of (key, value) tuples
                    row_dict = dict(row.data)
                else:
                    # Check for _data_dict attribute for other row-like objects
                    if hasattr(row.data, "_data_dict"):  # type: ignore[unreachable]
                        row_dict = row.data._data_dict
                    else:
                        row_dict = {}
            elif hasattr(row, "__dict__"):
                row_dict = row.__dict__

            # Get all column names from the row
            if row_dict:
                for col in sorted(row_dict.keys()):  # Sort for consistent ordering
                    value = row_dict[col]
                    # Convert unhashable types to hashable representations
                    try:
                        if isinstance(value, dict):
                            # Recursively convert dict - use string for unhashable values
                            value = tuple(
                                sorted(
                                    (
                                        k,
                                        v
                                        if not isinstance(v, (dict, list))
                                        else str(v),
                                    )
                                    for k, v in value.items()
                                )
                            )
                        elif isinstance(value, list):
                            # Convert list - use string for unhashable elements
                            value = tuple(
                                v if not isinstance(v, (dict, list)) else str(v)
                                for v in value
                            )
                        row_values.append((col, value))
                    except (TypeError, ValueError):
                        # If value can't be made hashable, use string representation
                        row_values.append((col, str(value)))
            else:
                # Fallback: try to get attributes directly
                for attr in dir(row):
                    if not attr.startswith("_"):
                        try:
                            value = getattr(row, attr)
                            if not callable(value):
                                row_values.append((attr, str(value)))
                        except (AttributeError, TypeError):
                            pass

            row_tuple = tuple(row_values)
            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_rows.append(row)
        return unique_rows

    @staticmethod
    def union_rows(rows1: List[Row], rows2: List[Row]) -> List[Row]:
        """Union two lists of rows."""
        return rows1 + rows2

    @staticmethod
    def _are_types_compatible(type1: DataType, type2: DataType) -> bool:
        """Check if two types are compatible for union operations.

        PySpark allows some type promotions (e.g., IntegerType -> LongType),
        and also allows numeric/string combinations (normalizes to string).

        Args:
            type1: First data type
            type2: Second data type

        Returns:
            True if types are compatible, False otherwise
        """
        # Exact match
        if type1 == type2:
            return True

        # Numeric type compatibility
        # PySpark allows numeric promotions: Byte -> Short -> Integer -> Long -> Float -> Double
        numeric_types: Tuple[Type[DataType], ...] = (
            ByteType,
            ShortType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
        )
        is_numeric1 = isinstance(type1, numeric_types)
        is_numeric2 = isinstance(type2, numeric_types)
        is_string1 = isinstance(type1, StringType)
        is_string2 = isinstance(type2, StringType)

        if is_numeric1 and is_numeric2:
            # All numeric types are compatible for union (PySpark promotes to wider type)
            return True

        # String types are generally compatible
        if is_string1 and is_string2:
            return True

        # PySpark allows numeric/string combinations (normalizes to string)
        # Issue #242: LongType + StringType -> StringType
        # For other types, require exact match
        return (is_numeric1 and is_string2) or (is_string1 and is_numeric2)

    @staticmethod
    def union(
        data1: List[Dict[str, Any]],
        schema1: "StructType",
        data2: List[Dict[str, Any]],
        schema2: "StructType",
        storage: Any,
    ) -> Tuple[List[Dict[str, Any]], "StructType"]:
        """Union two DataFrames with their data and schemas.

        Raises:
            AnalysisException: If DataFrames have incompatible schemas
        """
        from ...core.exceptions.analysis import AnalysisException

        # Validate schema compatibility
        # Check column count
        if len(schema1.fields) != len(schema2.fields):
            raise AnalysisException(
                f"Union can only be performed on tables with the same number of columns, "
                f"but the first table has {len(schema1.fields)} columns and "
                f"the second table has {len(schema2.fields)} columns"
            )

        # Check column names and types, and determine coercion targets
        from ...dataframe.casting.type_converter import TypeConverter

        coerced_data1 = data1.copy()
        coerced_data2 = data2.copy()
        result_fields = []

        for i, (field1, field2) in enumerate(zip(schema1.fields, schema2.fields)):
            if field1.name != field2.name:
                raise AnalysisException(
                    f"Union can only be performed on tables with compatible column names. "
                    f"Column {i} name mismatch: '{field1.name}' vs '{field2.name}'"
                )

            # Type compatibility check
            if not SetOperations._are_types_compatible(
                field1.dataType, field2.dataType
            ):
                raise AnalysisException(
                    f"Union can only be performed on tables with compatible column types. "
                    f"Column '{field1.name}' type mismatch: "
                    f"{field1.dataType} vs {field2.dataType}"
                )

            # Determine target type for coercion (PySpark behavior)
            target_type = field1.dataType
            if field1.dataType != field2.dataType:
                # PySpark normalizes numeric+string to string (issue #242)
                if isinstance(field1.dataType, StringType) or isinstance(
                    field2.dataType, StringType
                ):
                    target_type = StringType()
                # Numeric type promotion (already compatible, but ensure wider type)
                numeric_type_tuple: Tuple[Type[DataType], ...] = (
                    ByteType,
                    ShortType,
                    IntegerType,
                    LongType,
                    FloatType,
                    DoubleType,
                )
                if isinstance(field1.dataType, numeric_type_tuple) and isinstance(
                    field2.dataType, numeric_type_tuple
                ):
                    # Prefer Float over Int, Long over Int
                    if isinstance(
                        field1.dataType, (FloatType, DoubleType)
                    ) or isinstance(field2.dataType, (FloatType, DoubleType)):
                        target_type = DoubleType()
                    elif isinstance(field1.dataType, LongType) or isinstance(
                        field2.dataType, LongType
                    ):
                        target_type = LongType()
                    else:
                        target_type = field1.dataType  # Keep first type if compatible

            result_fields.append(StructField(field1.name, target_type, nullable=True))

            # Coerce data if types differ
            if target_type != field1.dataType or target_type != field2.dataType:
                col_name = field1.name
                # Coerce data1 values if needed
                if target_type != field1.dataType:
                    for row in coerced_data1:
                        if col_name in row:
                            row[col_name] = TypeConverter.cast_to_type(
                                row[col_name], target_type
                            )
                # Coerce data2 values if needed
                if target_type != field2.dataType:
                    for row in coerced_data2:
                        if col_name in row:
                            row[col_name] = TypeConverter.cast_to_type(
                                row[col_name], target_type
                            )

        # Convert data to Row objects for union
        rows1 = [Row(row) for row in coerced_data1]
        rows2 = [Row(row) for row in coerced_data2]

        # Perform union
        unioned_rows = SetOperations.union_rows(rows1, rows2)

        # Convert back to dict format
        result_data: List[Dict[str, Any]] = []
        for row in unioned_rows:  # type: ignore[assignment]
            if isinstance(row, Row):
                # Use Row.asDict() method for proper conversion
                row_dict: Dict[str, Any] = row.asDict()
                result_data.append(row_dict)
            elif hasattr(row, "data"):
                # Row object - convert data to dict
                if isinstance(row.data, dict):
                    result_data.append(row.data)
                elif isinstance(row.data, list):
                    # List of (key, value) tuples
                    result_data.append(dict(row.data))
                elif hasattr(row.data, "items"):
                    result_data.append(dict(row.data))
                else:
                    result_data.append({})
            elif hasattr(row, "__dict__"):
                # row.__dict__ is already a dict
                result_data.append(row.__dict__)
            else:
                result_data.append(dict(row) if hasattr(row, "items") else {})

        # Create result schema with coerced types
        from ...spark_types import StructType

        result_schema = StructType(result_fields)

        return result_data, result_schema

    @staticmethod
    def intersect_rows(rows1: List[Row], rows2: List[Row]) -> List[Row]:
        """Find intersection of two lists of rows."""

        def make_hashable(row: Row) -> Tuple[Any, ...]:
            row_values = []
            for col in row.__dict__:
                value = getattr(row, col)
                if isinstance(value, dict):
                    value = tuple(sorted(value.items()))
                elif isinstance(value, list):
                    value = tuple(value)
                row_values.append(value)
            return tuple(row_values)

        set1 = {make_hashable(row) for row in rows1}
        set2 = {make_hashable(row) for row in rows2}

        intersection = set1.intersection(set2)
        result = []
        for row in rows1:
            row_tuple = make_hashable(row)
            if row_tuple in intersection:
                result.append(row)
        return result

    @staticmethod
    def except_rows(rows1: List[Row], rows2: List[Row]) -> List[Row]:
        """Find rows in rows1 that are not in rows2."""

        def make_hashable(row: Row) -> Tuple[Any, ...]:
            row_values = []
            for col in row.__dict__:
                value = getattr(row, col)
                if isinstance(value, dict):
                    value = tuple(sorted(value.items()))
                elif isinstance(value, list):
                    value = tuple(value)
                row_values.append(value)
            return tuple(row_values)

        set2 = {make_hashable(row) for row in rows2}
        result = []
        for row in rows1:
            row_tuple = make_hashable(row)
            if row_tuple not in set2:
                result.append(row)
        return result

    @staticmethod
    def rows_equal(row1: Row, row2: Row) -> bool:
        """Check if two rows are equal."""
        if row1.__dict__.keys() != row2.__dict__.keys():
            return False
        return all(getattr(row1, col) == getattr(row2, col) for col in row1.__dict__)
