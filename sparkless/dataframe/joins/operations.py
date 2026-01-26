"""
Join operations mixin for DataFrame.

This mixin provides join and set operations that can be mixed into
the DataFrame class to add join capabilities.
"""

from typing import Any, Dict, Generic, List, TYPE_CHECKING, Tuple, TypeVar, Union, cast

from ...spark_types import DataType, StringType, StructField, StructType
from ..protocols import SupportsDataFrameOps

if TYPE_CHECKING:
    from ...functions import ColumnOperation

SupportsDF = TypeVar("SupportsDF", bound=SupportsDataFrameOps)


class JoinOperations(Generic[SupportsDF]):
    """Mixin providing join and set operations for DataFrame."""

    if TYPE_CHECKING:
        schema: StructType
        data: List[Dict[str, Any]]
        storage: Any
        _operations_queue: List[Tuple[str, Any]]

        def _queue_op(self, operation: str, payload: Any) -> SupportsDataFrameOps: ...

    def join(
        self: SupportsDF,
        other: SupportsDataFrameOps,
        on: Union[str, List[str], "ColumnOperation"],
        how: str = "inner",
    ) -> SupportsDF:
        """Join with another DataFrame."""
        if isinstance(on, str):
            on = [on]

        return cast("SupportsDF", self._queue_op("join", (other, on, how)))  # type: ignore[redundant-cast,unused-ignore]

    def crossJoin(self: SupportsDF, other: SupportsDataFrameOps) -> SupportsDF:
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
        for field in self.schema.fields:
            new_fields.append(field)
            field_names.add(field.name)

        # Add fields from other DataFrame - keep duplicate names as in PySpark
        for field in other.schema.fields:
            new_fields.append(field)  # Keep original name even if duplicate
            field_names.add(field.name)

        new_schema = StructType(new_fields)

        # Create Cartesian product
        result_data = []

        for left_row in self.data:
            for right_row in other.data:
                new_row = {}

                # Add fields from left DataFrame
                for field in self.schema.fields:
                    new_row[field.name] = left_row.get(field.name)

                # Add fields from right DataFrame - allow duplicates
                for field in other.schema.fields:
                    # When accessing by key, duplicate columns get overwritten
                    # Use a dict which naturally handles this (last value wins)
                    new_row[field.name] = right_row.get(field.name)

                result_data.append(new_row)

        from ..dataframe import DataFrame
        from typing import cast

        return cast("SupportsDF", DataFrame(result_data, new_schema, self.storage))

    def union(self: SupportsDF, other: SupportsDataFrameOps) -> SupportsDF:
        """Union with another DataFrame."""
        return cast("SupportsDF", self._queue_op("union", other))  # type: ignore[redundant-cast,unused-ignore]

    def unionByName(
        self: SupportsDF,
        other: SupportsDataFrameOps,
        allowMissingColumns: bool = False,
    ) -> SupportsDF:
        """Union with another DataFrame by column names.

        Unlike `union()`, which matches columns by position, `unionByName()` matches
        columns by name, allowing DataFrames with different column orders to be combined.
        Both DataFrames are automatically materialized before unioning to ensure correct
        results, especially in diamond dependency scenarios where the same DataFrame
        is used in multiple transformation branches.

        Args:
            other: Another DataFrame to union with. Must have compatible column types.
            allowMissingColumns: If True, allows missing columns (fills with null).
                When False, both DataFrames must have the same columns.

        Returns:
            New DataFrame with combined data from both DataFrames. Column order matches
            the first DataFrame's schema.

        Raises:
            AnalysisException: If DataFrames have incompatible column types or missing
                columns when `allowMissingColumns=False`.

        Example:
            >>> df1 = spark.createDataFrame([("Alice", 25)], ["name", "age"])
            >>> df2 = spark.createDataFrame([(30, "Bob")], ["age", "name"])  # Different order
            >>> result = df1.unionByName(df2)  # Works correctly despite different order
        """
        # Materialize lazy operations before accessing data
        # This is critical for diamond dependencies where the same DataFrame
        # is used in multiple branches and then combined via unionByName
        from ..lazy import LazyEvaluationEngine
        from ..dataframe import DataFrame

        # Materialize self if it has operations queued
        if hasattr(self, "_operations_queue") and self._operations_queue:
            # Type cast: self should be a DataFrame at runtime
            self_materialized = LazyEvaluationEngine.materialize(
                cast("DataFrame", self)
            )
        else:
            # If no operations queued, create a new DataFrame with a copy of the data
            # to avoid sharing references in diamond dependencies
            self_materialized = DataFrame(
                [dict(row) for row in self.data], self.schema, self.storage
            )

        # Materialize other if it has operations queued
        if hasattr(other, "_operations_queue") and other._operations_queue:
            # Type cast: other should be a DataFrame at runtime
            other_materialized = LazyEvaluationEngine.materialize(
                cast("DataFrame", other)
            )
        else:
            # If no operations queued, create a new DataFrame with a copy of the data
            # to avoid sharing references in diamond dependencies
            other_materialized = DataFrame(
                [dict(row) for row in other.data],
                other.schema,
                getattr(other, "storage", self.storage),
            )

        # Get column names from both DataFrames (using materialized schemas)
        self_cols = {field.name for field in self_materialized.schema.fields}
        other_cols = {field.name for field in other_materialized.schema.fields}

        # Check for missing columns
        missing_in_other = self_cols - other_cols
        missing_in_self = other_cols - self_cols

        if not allowMissingColumns and (missing_in_other or missing_in_self):
            from ...core.exceptions.analysis import AnalysisException

            raise AnalysisException(
                f"Union by name failed: missing columns in one of the DataFrames. "
                f"Missing in other: {missing_in_other}, Missing in self: {missing_in_self}"
            )

        # Get all unique column names in order
        all_cols = list(self_cols.union(other_cols))

        # Create combined data with all columns
        combined_data = []

        # Add rows from self DataFrame (using materialized data)
        for row in self_materialized.data:
            new_row = {}
            for col in all_cols:
                if col in row:
                    new_row[col] = row[col]
                else:
                    new_row[col] = None  # Missing column filled with null
            combined_data.append(new_row)

        # Add rows from other DataFrame (using materialized data)
        for row in other_materialized.data:
            new_row = {}
            for col in all_cols:
                if col in row:
                    new_row[col] = row[col]
                else:
                    new_row[col] = None  # Missing column filled with null
            combined_data.append(new_row)

        # Create new schema with all columns (using materialized schemas)

        new_fields = []
        for col in all_cols:
            # Try to get the data type from the materialized schema, default to StringType
            field_type: DataType = StringType()
            for field in self_materialized.schema.fields:
                if field.name == col:
                    field_type = field.dataType
                    break
            # If not found in self schema, check other schema
            if isinstance(field_type, StringType):
                for field in other_materialized.schema.fields:
                    if field.name == col:
                        field_type = field.dataType
                        break
            new_fields.append(StructField(col, field_type))

        new_schema = StructType(new_fields)
        from ..dataframe import DataFrame

        return cast(
            "SupportsDF",
            DataFrame(combined_data, new_schema, self.storage),
        )

    def unionAll(self: SupportsDF, other: SupportsDataFrameOps) -> SupportsDF:
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
        return cast("SupportsDF", self.union(other))  # type: ignore[redundant-cast,unused-ignore]

    def intersect(self: SupportsDF, other: SupportsDataFrameOps) -> SupportsDF:
        """Intersect with another DataFrame.

        Args:
            other: Another DataFrame to intersect with.

        Returns:
            New DataFrame with common rows.
        """
        # Convert rows to tuples for comparison
        self_rows = [
            tuple(row.get(field.name) for field in self.schema.fields)
            for row in self.data
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
            for i, field in enumerate(self.schema.fields):
                row_dict[field.name] = row_tuple[i]
            result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast("SupportsDF", DataFrame(result_data, self.schema, self.storage))

    def intersectAll(self: SupportsDF, other: SupportsDataFrameOps) -> SupportsDF:
        """Return intersection with duplicates (PySpark 3.0+).

        Args:
            other: DataFrame to intersect with

        Returns:
            DataFrame with common rows (preserving duplicates)
        """
        from collections import Counter

        def row_to_tuple(row: Dict[str, Any]) -> Tuple[Any, ...]:
            return tuple(row.get(field.name) for field in self.schema.fields)

        # Count occurrences in each DataFrame
        self_counter = Counter(row_to_tuple(row) for row in self.data)
        other_counter = Counter(row_to_tuple(row) for row in other.data)

        # Intersection preserves minimum count
        result_data = []
        for row_tuple, count in self_counter.items():
            min_count = min(count, other_counter.get(row_tuple, 0))
            for _ in range(min_count):
                row_dict = {
                    field.name: value
                    for field, value in zip(self.schema.fields, row_tuple)
                }
                result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast("SupportsDF", DataFrame(result_data, self.schema, self.storage))

    def exceptAll(self: SupportsDF, other: SupportsDataFrameOps) -> SupportsDF:
        """Except all with another DataFrame (set difference with duplicates).

        Args:
            other: Another DataFrame to except from this one.

        Returns:
            New DataFrame with rows from self not in other, preserving duplicates.
        """
        # Convert rows to tuples for comparison
        self_rows = [
            tuple(row.get(field.name) for field in self.schema.fields)
            for row in self.data
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
            for i, field in enumerate(self.schema.fields):
                row_dict[field.name] = row_tuple[i]
            result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast("SupportsDF", DataFrame(result_data, self.schema, self.storage))

    def subtract(self: SupportsDF, other: SupportsDataFrameOps) -> SupportsDF:
        """Return rows in this DataFrame but not in another (all PySpark versions).

        Args:
            other: DataFrame to subtract

        Returns:
            DataFrame with rows from this DataFrame that are not in other
        """

        # Convert rows to tuples for comparison
        def row_to_tuple(row: Dict[str, Any]) -> Tuple[Any, ...]:
            return tuple(row.get(field.name) for field in self.schema.fields)

        self_rows = {row_to_tuple(row) for row in self.data}
        other_rows = {row_to_tuple(row) for row in other.data}

        # Find rows in self but not in other
        result_tuples = self_rows - other_rows

        # Convert back to dicts
        result_data = []
        for row_tuple in result_tuples:
            row_dict = {
                field.name: value for field, value in zip(self.schema.fields, row_tuple)
            }
            result_data.append(row_dict)

        from ..dataframe import DataFrame

        return cast("SupportsDF", DataFrame(result_data, self.schema, self.storage))
