"""
Join service for DataFrame operations.

This service provides join and set operations using composition instead of mixin inheritance.
"""

from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Tuple, Union, cast

from ...spark_types import (
    DataType,
    StringType,
    StructField,
    StructType,
    IntegerType,
    LongType,
    DoubleType,
)
from ..protocols import SupportsDataFrameOps

if TYPE_CHECKING:
    from ...functions import ColumnOperation
    from ..dataframe import DataFrame


class JoinService:
    """Service providing join and set operations for DataFrame."""

    def __init__(self, df: "DataFrame"):
        """Initialize join service with DataFrame instance."""
        self._df = df

    def join(
        self,
        other: SupportsDataFrameOps,
        on: Union[str, List[str], "ColumnOperation"],
        how: str = "inner",
    ) -> "SupportsDataFrameOps":
        """Join with another DataFrame."""
        if isinstance(on, str):
            on = [on]

        return self._df._queue_op("join", (other, on, how))

    def crossJoin(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Cross join (Cartesian product) with another DataFrame.

        Args:
            other: Another DataFrame to cross join with.

        Returns:
            New DataFrame with Cartesian product of rows.
        """
        # Create new schema combining both DataFrames

        # Combine field names, handling duplicates
        new_fields = []
        field_names = set()

        # Add fields from self DataFrame
        for field in self._df.schema.fields:
            new_fields.append(field)
            field_names.add(field.name)

        # Add fields from other DataFrame - keep duplicate names as in PySpark
        for field in other.schema.fields:
            new_fields.append(field)  # Keep original name even if duplicate
            field_names.add(field.name)

        new_schema = StructType(new_fields)

        # Create Cartesian product
        result_data = []

        for left_row in self._df.data:
            for right_row in other.data:
                new_row = {}

                # Add fields from left DataFrame
                for field in self._df.schema.fields:
                    new_row[field.name] = left_row.get(field.name)

                # Add fields from right DataFrame - allow duplicates
                for field in other.schema.fields:
                    # When accessing by key, duplicate columns get overwritten
                    # Use a dict which naturally handles this (last value wins)
                    new_row[field.name] = right_row.get(field.name)

                result_data.append(new_row)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps", DataFrame(result_data, new_schema, self._df.storage)
        )

    def union(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Union with another DataFrame.

        Raises:
            AnalysisException: If DataFrames have incompatible schemas
        """
        from ...core.exceptions.analysis import AnalysisException
        from ...dataframe.operations.set_operations import SetOperations

        # Validate schema compatibility before queuing (PySpark compatibility)
        # PySpark raises exceptions immediately, not lazily
        self_schema = self._df.schema
        other_schema = other.schema

        # Check column count
        if len(self_schema.fields) != len(other_schema.fields):
            raise AnalysisException(
                f"Union can only be performed on tables with the same number of columns, "
                f"but the first table has {len(self_schema.fields)} columns and "
                f"the second table has {len(other_schema.fields)} columns"
            )

        # Check column names and types
        for i, (field1, field2) in enumerate(
            zip(self_schema.fields, other_schema.fields)
        ):
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

        return self._df._queue_op("union", other)

    def unionByName(
        self,
        other: SupportsDataFrameOps,
        allowMissingColumns: bool = False,
    ) -> "SupportsDataFrameOps":
        """Union with another DataFrame by column names.

        Args:
            other: Another DataFrame to union with.
            allowMissingColumns: If True, allows missing columns (fills with null).

        Returns:
            New DataFrame with combined data.

        Raises:
            AnalysisException: If DataFrames have incompatible column types
        """
        from ...core.exceptions.analysis import AnalysisException
        from ...dataframe.operations.set_operations import SetOperations

        # Get column names from both DataFrames (case-insensitive matching)

        self_cols: Set[str] = {field.name for field in self._df.schema.fields}
        other_cols: Set[str] = {field.name for field in other.schema.fields}

        # Build case-insensitive mappings
        self_cols_lower = {col.lower(): col for col in self_cols}
        other_cols_lower = {col.lower(): col for col in other_cols}

        # Check for missing columns (case-insensitive)
        missing_in_other: Set[str] = set()
        for self_col in self_cols:
            if self_col.lower() not in other_cols_lower:
                missing_in_other.add(self_col)

        missing_in_self: Set[str] = set()
        for other_col in other_cols:
            if other_col.lower() not in self_cols_lower:
                missing_in_self.add(other_col)

        if not allowMissingColumns and (missing_in_other or missing_in_self):
            raise AnalysisException(
                f"Union by name failed: missing columns in one of the DataFrames. "
                f"Missing in other: {missing_in_other}, Missing in self: {missing_in_self}"
            )

        # Check type compatibility for columns that exist in both schemas (case-insensitive)
        common_cols: Set[str] = set()
        for self_col in self_cols:
            if self_col.lower() in other_cols_lower:
                common_cols.add(self_col)

        for col_name in common_cols:
            # Find the field in both schemas
            self_field: StructField = next(
                f for f in self._df.schema.fields if f.name == col_name
            )
            # Find corresponding field in other schema (case-insensitive)
            other_col_name = other_cols_lower.get(col_name.lower())
            if other_col_name is None:
                # Skip if no matching column found (shouldn't happen, but handle gracefully)
                continue
            other_field: Optional[StructField] = next(
                (f for f in other.schema.fields if f.name == other_col_name), None
            )
            if other_field is None:
                # Skip if field not found (shouldn't happen, but handle gracefully)
                continue

            # Check type compatibility
            if not SetOperations._are_types_compatible(
                self_field.dataType, other_field.dataType
            ):
                raise AnalysisException(
                    f"Union can only be performed on tables with compatible column types. "
                    f"Column '{col_name}' type mismatch: "
                    f"{self_field.dataType} vs {other_field.dataType}"
                )

        # Get all unique column names in order
        all_cols: List[str] = list(self_cols.union(other_cols))

        # Create combined data with all columns
        combined_data: List[Dict[str, Any]] = []

        # Add rows from self DataFrame
        for row in self._df.data:
            new_row: Dict[str, Any] = {}
            for col in all_cols:
                if col in row:
                    new_row[col] = row[col]
                else:
                    new_row[col] = None  # Missing column filled with null
            combined_data.append(new_row)

        # Add rows from other DataFrame
        # Build a mapping from case-insensitive column names to actual column names in other DataFrame
        other_row_keys_lower = {
            key.lower(): key for key in (other.data[0].keys() if other.data else [])
        }
        for row in other.data:
            other_new_row: Dict[str, Any] = {}
            for col in all_cols:
                # Find actual column name case-insensitively in row
                actual_col_in_row = other_row_keys_lower.get(col.lower())
                if actual_col_in_row and actual_col_in_row in row:
                    other_new_row[col] = row[actual_col_in_row]
                else:
                    other_new_row[col] = None  # Missing column filled with null
            combined_data.append(other_new_row)

        # Create new schema with all columns
        # For common columns, apply type coercion (PySpark behavior: numeric+string -> string)
        # For nullable flags, result is nullable if either input is nullable
        from ...dataframe.casting.type_converter import TypeConverter
        from ...spark_types import (
            ByteType,
            ShortType,
            FloatType,
        )

        numeric_types = (
            ByteType,
            ShortType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
        )

        new_fields: List[StructField] = []
        coerced_combined_data = combined_data.copy()

        for col in all_cols:
            # Determine target type for coercion (PySpark behavior)
            field_type: DataType = StringType()
            nullable: bool = True

            # Get field types from both schemas
            self_field_coerce: Optional[StructField] = None
            other_field_coerce: Optional[StructField] = None

            for field in self._df.schema.fields:
                if field.name == col:
                    self_field_coerce = field
                    field_type = field.dataType
                    nullable = field.nullable
                    break

            # Find other field case-insensitively
            other_col_name = other_cols_lower.get(col.lower())
            if other_col_name:
                for field in other.schema.fields:
                    if field.name == other_col_name:
                        other_field_coerce = field
                        break

            # If not found in self schema, use other schema
            if self_field_coerce is None and other_field_coerce:
                field_type = other_field_coerce.dataType
                nullable = other_field_coerce.nullable

            # For common columns, apply type coercion
            if col in common_cols and self_field_coerce and other_field_coerce:
                # Determine coerced type (PySpark behavior: numeric+string -> string)
                is_numeric1 = isinstance(self_field_coerce.dataType, numeric_types)
                is_numeric2 = isinstance(other_field_coerce.dataType, numeric_types)
                is_string1 = isinstance(self_field_coerce.dataType, StringType)
                is_string2 = isinstance(other_field_coerce.dataType, StringType)

                if (is_numeric1 and is_string2) or (is_string1 and is_numeric2):
                    # Numeric + String -> String (PySpark behavior, issue #242)
                    target_type = StringType()
                    # Coerce data values
                    for row in coerced_combined_data:
                        if col in row:
                            row[col] = TypeConverter.cast_to_type(row[col], target_type)
                    field_type = target_type
                elif is_numeric1 and is_numeric2:
                    # Both numeric - promote to wider type
                    if isinstance(
                        self_field_coerce.dataType, (FloatType, DoubleType)
                    ) or isinstance(
                        other_field_coerce.dataType, (FloatType, DoubleType)
                    ):
                        target_type = DoubleType()
                    elif isinstance(self_field_coerce.dataType, LongType) or isinstance(
                        other_field_coerce.dataType, LongType
                    ):
                        target_type = LongType()
                    else:
                        target_type = self_field_coerce.dataType
                    # Coerce data values if needed
                    if target_type != field_type:
                        for row in coerced_combined_data:
                            if col in row:
                                row[col] = TypeConverter.cast_to_type(
                                    row[col], target_type
                                )
                    field_type = target_type

                # Nullable is True if either is nullable
                nullable = bool(
                    self_field_coerce.nullable or other_field_coerce.nullable
                )

            new_fields.append(StructField(col, field_type, nullable))

        new_schema = StructType(new_fields)
        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(coerced_combined_data, new_schema, self._df.storage),
        )

    def unionAll(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Deprecated alias for union() - Use union() instead (all PySpark versions).

        Args:
            other: DataFrame to union with

        Returns:
            Union of both DataFrames

        Note:
            Deprecated in PySpark 2.0+, use union() instead
        """
        import warnings

        warnings.warn(
            "unionAll is deprecated. Use union instead.", FutureWarning, stacklevel=2
        )
        return self.union(other)

    def intersect(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Intersect with another DataFrame.

        Args:
            other: Another DataFrame to intersect with.

        Returns:
            New DataFrame with common rows.
        """
        # Convert rows to tuples for comparison
        self_rows = [
            tuple(row.get(field.name) for field in self._df.schema.fields)
            for row in self._df.data
        ]
        other_rows = [
            tuple(row.get(field.name) for field in other.schema.fields)
            for row in other.data
        ]

        # Find common rows
        self_row_set = set(self_rows)
        other_row_set = set(other_rows)
        common_rows = self_row_set.intersection(other_row_set)

        # Convert back to dictionaries
        result_data = []
        for row_tuple in common_rows:
            row_dict = {}
            for i, field in enumerate(self._df.schema.fields):
                row_dict[field.name] = row_tuple[i]
            result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, self._df.schema, self._df.storage),
        )

    def intersectAll(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Return intersection with duplicates (PySpark 3.0+).

        Args:
            other: DataFrame to intersect with

        Returns:
            DataFrame with common rows (preserving duplicates)
        """
        from collections import Counter

        def row_to_tuple(row: Dict[str, Any]) -> Tuple[Any, ...]:
            return tuple(row.get(field.name) for field in self._df.schema.fields)

        # Count occurrences in each DataFrame
        self_counter = Counter(row_to_tuple(row) for row in self._df.data)
        other_counter = Counter(row_to_tuple(row) for row in other.data)

        # Intersection preserves minimum count
        result_data = []
        for row_tuple, count in self_counter.items():
            min_count = min(count, other_counter.get(row_tuple, 0))
            for _ in range(min_count):
                row_dict = {
                    field.name: value
                    for field, value in zip(self._df.schema.fields, row_tuple)
                }
                result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, self._df.schema, self._df.storage),
        )

    def exceptAll(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Except all with another DataFrame (set difference with duplicates).

        Args:
            other: Another DataFrame to except from this one.

        Returns:
            New DataFrame with rows from self not in other, preserving duplicates.
        """
        # Convert rows to tuples for comparison
        self_rows = [
            tuple(row.get(field.name) for field in self._df.schema.fields)
            for row in self._df.data
        ]
        other_rows = [
            tuple(row.get(field.name) for field in other.schema.fields)
            for row in other.data
        ]

        # Count occurrences in other DataFrame

        other_row_counts: Dict[Tuple[Any, ...], int] = {}
        for row_tuple in other_rows:
            other_row_counts[row_tuple] = other_row_counts.get(row_tuple, 0) + 1

        # Count occurrences in self DataFrame
        self_row_counts: Dict[Tuple[Any, ...], int] = {}
        for row_tuple in self_rows:
            self_row_counts[row_tuple] = self_row_counts.get(row_tuple, 0) + 1

        # Calculate the difference preserving duplicates
        result_rows: List[Tuple[Any, ...]] = []
        for row_tuple in self_rows:
            # Count how many times this row appears in other
            other_count = other_row_counts.get(row_tuple, 0)
            # Count how many times this row appears in self so far
            self_count_so_far = result_rows.count(row_tuple)
            # If we haven't exceeded the difference, include this row
            if self_count_so_far < (self_row_counts[row_tuple] - other_count):
                result_rows.append(row_tuple)

        # Convert back to dictionaries
        result_data = []
        for row_tuple in result_rows:
            row_dict = {}
            for i, field in enumerate(self._df.schema.fields):
                row_dict[field.name] = row_tuple[i]
            result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, self._df.schema, self._df.storage),
        )

    def subtract(self, other: SupportsDataFrameOps) -> "SupportsDataFrameOps":
        """Return rows in this DataFrame but not in another (all PySpark versions).

        Args:
            other: DataFrame to subtract

        Returns:
            DataFrame with rows from this DataFrame that are not in other
        """

        # Convert rows to tuples for comparison
        def row_to_tuple(row: Dict[str, Any]) -> Tuple[Any, ...]:
            return tuple(row.get(field.name) for field in self._df.schema.fields)

        self_rows = {row_to_tuple(row) for row in self._df.data}
        other_rows = {row_to_tuple(row) for row in other.data}

        # Find rows in self but not in other
        result_tuples = self_rows - other_rows

        # Convert back to dicts
        result_data = []
        for row_tuple in result_tuples:
            row_dict = {
                field.name: value
                for field, value in zip(self._df.schema.fields, row_tuple)
            }
            result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(result_data, self._df.schema, self._df.storage),
        )
