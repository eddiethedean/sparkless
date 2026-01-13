"""
Aggregation operations mixin for DataFrame.

This mixin provides aggregation operations that can be mixed into
the DataFrame class to add aggregation capabilities.
"""

from typing import Any, Dict, Generic, List, TYPE_CHECKING, Tuple, TypeVar, Union, cast

from ...core.exceptions.operation import SparkColumnNotFoundError
from ...functions import Column, ColumnOperation, AggregateFunction
from ..grouped import GroupedData
from ..protocols import SupportsDataFrameOps

if TYPE_CHECKING:
    from ...spark_types import StructType

SupportsDF = TypeVar("SupportsDF", bound=SupportsDataFrameOps)


class AggregationOperations(Generic[SupportsDF]):
    """Mixin providing aggregation operations for DataFrame."""

    if TYPE_CHECKING:
        schema: StructType
        data: List[Dict[str, Any]]
        _operations_queue: List[Tuple[str, Any]]

        def _queue_op(self, operation: str, payload: Any) -> SupportsDataFrameOps: ...

    def groupBy(self: SupportsDF, *columns: Union[str, Column]) -> GroupedData:
        """Group DataFrame by columns for aggregation operations.

        Args:
            *columns: Column names or Column objects to group by.
                     Can also accept a list/tuple of column names: df.groupBy(["col1", "col2"])

        Returns:
            GroupedData for aggregation operations.

        Example:
            >>> df.groupBy("category").count()
            >>> df.groupBy("dept", "year").avg("salary")
            >>> df.groupBy(["dept", "year"]).count()  # PySpark-compatible: list of column names
        """
        # PySpark compatibility: if a single list/tuple is passed, unpack it
        # This allows df.groupBy(["col1", "col2"]) to work like df.groupBy("col1", "col2")
        # Also supports df.groupBy(df.columns)
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):  # type: ignore[unreachable]
            # Unpack list/tuple of columns
            columns = tuple(columns[0])  # type: ignore[unreachable]

        col_names = []
        for col in columns:
            if isinstance(col, Column):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                available_columns = [field.name for field in self.schema.fields]
                raise SparkColumnNotFoundError(col_name, available_columns)

        return GroupedData(self, col_names)

    def groupby(
        self: SupportsDF, *cols: Union[str, Column], **kwargs: Any
    ) -> GroupedData:
        """Lowercase alias for groupBy() (all PySpark versions).

        Args:
            *cols: Column names or Column objects to group by
            **kwargs: Additional grouping options

        Returns:
            GroupedData object
        """
        return cast("GroupedData", self.groupBy(*cols, **kwargs))

    def rollup(
        self: SupportsDF, *columns: Union[str, Column]
    ) -> Any:  # Returns RollupGroupedData
        """Create rollup grouped data for hierarchical grouping.

        Args:
            *columns: Columns to rollup.
                     Can also accept a list/tuple of column names: df.rollup(["col1", "col2"])

        Returns:
            RollupGroupedData for hierarchical grouping.

        Example:
            >>> df.rollup("country", "state").sum("sales")
            >>> df.rollup(["country", "state"]).sum("sales")  # PySpark-compatible: list of column names
        """
        # PySpark compatibility: if a single list/tuple is passed, unpack it
        # This allows df.rollup(["col1", "col2"]) to work like df.rollup("col1", "col2")
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):  # type: ignore[unreachable]
            # Unpack list/tuple of columns
            columns = tuple(columns[0])  # type: ignore[unreachable]

        col_names = []
        for col in columns:
            if isinstance(col, Column):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                available_columns = [field.name for field in self.schema.fields]
                raise SparkColumnNotFoundError(col_name, available_columns)

        from ..grouped.rollup import RollupGroupedData

        return RollupGroupedData(self, col_names)

    def cube(
        self: SupportsDF, *columns: Union[str, Column]
    ) -> Any:  # Returns CubeGroupedData
        """Create cube grouped data for multi-dimensional grouping.

        Args:
            *columns: Columns to cube.
                     Can also accept a list/tuple of column names: df.cube(["col1", "col2"])

        Returns:
            CubeGroupedData for multi-dimensional grouping.

        Example:
            >>> df.cube("year", "month").sum("revenue")
            >>> df.cube(["year", "month"]).sum("revenue")  # PySpark-compatible: list of column names
        """
        # PySpark compatibility: if a single list/tuple is passed, unpack it
        # This allows df.cube(["col1", "col2"]) to work like df.cube("col1", "col2")
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):  # type: ignore[unreachable]
            # Unpack list/tuple of columns
            columns = tuple(columns[0])  # type: ignore[unreachable]

        col_names = []
        for col in columns:
            if isinstance(col, Column):
                col_names.append(col.name)
            else:
                col_names.append(col)

        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.schema.fields]:
                available_columns = [field.name for field in self.schema.fields]
                raise SparkColumnNotFoundError(col_name, available_columns)

        from ..grouped.cube import CubeGroupedData

        return CubeGroupedData(self, col_names)

    def agg(
        self: SupportsDF, *exprs: Union[str, Column, ColumnOperation, Dict[str, str]]
    ) -> SupportsDF:
        """Aggregate DataFrame without grouping (global aggregation).

        Args:
            *exprs: Aggregation expressions, column names, or dictionary mapping
                   column names to aggregation functions.

        Returns:
            DataFrame with aggregated results.

        Example:
            >>> df.agg(F.max("age"), F.min("age"))
            >>> df.agg({"age": "max", "salary": "avg"})
        """
        # Handle dictionary syntax: {"col": "agg_func"}
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            from ...functions import F

            agg_dict = exprs[0]
            converted_exprs: List[
                Union[str, Column, ColumnOperation, AggregateFunction]
            ] = []
            for col_name, agg_func in agg_dict.items():
                if agg_func == "sum":
                    converted_exprs.append(F.sum(col_name))
                elif agg_func == "avg" or agg_func == "mean":
                    converted_exprs.append(F.avg(col_name))
                elif agg_func == "max":
                    converted_exprs.append(F.max(col_name))
                elif agg_func == "min":
                    converted_exprs.append(F.min(col_name))
                elif agg_func == "count":
                    converted_exprs.append(F.count(col_name))
                elif agg_func == "stddev":
                    converted_exprs.append(F.stddev(col_name))
                elif agg_func == "variance":
                    converted_exprs.append(F.variance(col_name))
                else:
                    # Fallback to string expression
                    converted_exprs.append(f"{agg_func}({col_name})")
            # Create a grouped data object with empty group columns for global aggregation
            grouped = GroupedData(self, [])
            return cast("SupportsDF", grouped.agg(*converted_exprs))

        # Create a grouped data object with empty group columns for global aggregation
        grouped = GroupedData(self, [])
        return cast("SupportsDF", grouped.agg(*exprs))
