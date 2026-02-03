"""
Aggregation service for DataFrame operations.

This service provides aggregation operations using composition instead of mixin inheritance.
"""

from typing import Any, Dict, List, Tuple, TYPE_CHECKING, Union, cast

from ...core.exceptions.operation import SparkColumnNotFoundError
from ...functions import Column, ColumnOperation, AggregateFunction
from ..grouped import GroupedData

if TYPE_CHECKING:
    from ..dataframe import DataFrame
    from ..protocols import SupportsDataFrameOps


def _resolve_group_column(
    col: Union[str, Column], case_sensitive: bool = False
) -> Tuple[str, str]:
    """Resolve group column to (base_col_name, output_name).

    For F.col('Name').alias('Key'): base='Name', output='Key'.
    For F.col('Name') or 'Name': base='Name', output='Name'.
    """
    if isinstance(col, str):
        return (col, col)
    # Column or ColumnOperation with alias
    output_name = getattr(col, "name", None) or str(col)
    base_col = col
    if hasattr(col, "_original_column") and col._original_column is not None:
        base_col = col._original_column
    if hasattr(col, "_alias_name") and col._alias_name is not None:
        output_name = col._alias_name
    # Recursively get base column name
    while hasattr(base_col, "column") and base_col.column is not None:
        base_col = base_col.column
    base_name = getattr(base_col, "name", None) or str(base_col)
    return (base_name, output_name)


class AggregationService:
    """Service providing aggregation operations for DataFrame."""

    def __init__(self, df: "DataFrame"):
        """Initialize aggregation service with DataFrame instance."""
        self._df = df

    def groupBy(self, *columns: Union[str, Column]) -> GroupedData:
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

        # Resolve (base_col, output_name) for each column (Issue #397: alias support)
        from ..validation.column_validator import ColumnValidator

        case_sensitive = self._df._is_case_sensitive()
        group_base_cols: List[str] = []
        group_output_names: List[str] = []
        for col_spec in columns:
            base_name, output_name = _resolve_group_column(col_spec, case_sensitive)
            ColumnValidator.validate_column_exists(
                self._df.schema, base_name, "groupBy", case_sensitive
            )
            actual_base = (
                ColumnValidator._find_column(self._df.schema, base_name, case_sensitive)
                or base_name
            )
            group_base_cols.append(actual_base)
            group_output_names.append(output_name)

        # Cast to SupportsDataFrameOps to satisfy type checker
        # DataFrame implements the protocol at runtime, but mypy can't verify
        # due to minor signature differences (e.g., approxQuantile accepts List[str])
        return GroupedData(
            cast("SupportsDataFrameOps", self._df),
            group_base_cols,
            group_output_names,
        )

    def groupby(self, *cols: Union[str, Column], **kwargs: Any) -> GroupedData:
        """Lowercase alias for groupBy() (all PySpark versions).

        Args:
            *cols: Column names or Column objects to group by
            **kwargs: Additional grouping options

        Returns:
            GroupedData object
        """
        return self.groupBy(*cols, **kwargs)

    def rollup(self, *columns: Union[str, Column]) -> Any:  # Returns RollupGroupedData
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

        # Validate that all columns exist and resolve case-insensitively
        from ...core.column_resolver import ColumnResolver

        available_cols = [field.name for field in self._df.schema.fields]
        case_sensitive = self._df._is_case_sensitive()
        resolved_col_names = []
        for col_name in col_names:
            resolved_col = ColumnResolver.resolve_column_name(
                col_name, available_cols, case_sensitive
            )
            if resolved_col is None:
                raise SparkColumnNotFoundError(col_name, available_cols)
            resolved_col_names.append(resolved_col)

        from ..grouped.rollup import RollupGroupedData

        # Cast to SupportsDataFrameOps to satisfy type checker
        return RollupGroupedData(
            cast("SupportsDataFrameOps", self._df), resolved_col_names
        )

    def cube(self, *columns: Union[str, Column]) -> Any:  # Returns CubeGroupedData
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

        # Validate that all columns exist and resolve case-insensitively
        from ...core.column_resolver import ColumnResolver

        available_cols = [field.name for field in self._df.schema.fields]
        case_sensitive = self._df._is_case_sensitive()
        resolved_col_names = []
        for col_name in col_names:
            resolved_col = ColumnResolver.resolve_column_name(
                col_name, available_cols, case_sensitive
            )
            if resolved_col is None:
                raise SparkColumnNotFoundError(col_name, available_cols)
            resolved_col_names.append(resolved_col)

        from ..grouped.cube import CubeGroupedData

        # Cast to SupportsDataFrameOps to satisfy type checker
        return CubeGroupedData(
            cast("SupportsDataFrameOps", self._df), resolved_col_names
        )

    def agg(
        self, *exprs: Union[str, Column, ColumnOperation, Dict[str, str]]
    ) -> "SupportsDataFrameOps":
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
            grouped = GroupedData(cast("SupportsDataFrameOps", self._df), [])
            return cast("SupportsDataFrameOps", grouped.agg(*converted_exprs))

        # Create a grouped data object with empty group columns for global aggregation
        grouped = GroupedData(cast("SupportsDataFrameOps", self._df), [])
        return cast("SupportsDataFrameOps", grouped.agg(*exprs))
