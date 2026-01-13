"""
Rollup grouped data implementation for Sparkless.

This module provides rollup grouped data functionality for hierarchical
grouping operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from typing import Any, Dict, List, TYPE_CHECKING, Tuple, Union

from ...functions import Column, ColumnOperation, AggregateFunction
from ..protocols import SupportsDataFrameOps
from .base import GroupedData

if TYPE_CHECKING:
    from ..dataframe import DataFrame


class RollupGroupedData(GroupedData):
    """Mock rollup grouped data for hierarchical grouping operations."""

    def __init__(self, df: SupportsDataFrameOps, rollup_columns: List[str]):
        """Initialize RollupGroupedData.

        Args:
            df: The DataFrame being grouped.
            rollup_columns: List of column names for rollup hierarchy.
        """
        super().__init__(df, rollup_columns)
        self.rollup_columns = rollup_columns

    def agg(
        self,
        *exprs: Union[str, Column, ColumnOperation, AggregateFunction, Dict[str, str]],
    ) -> "DataFrame":
        """Aggregate rollup grouped data with hierarchical grouping.

        Creates all possible combinations of rollup columns, including null values
        for higher-level groupings.

        Args:
            *exprs: Aggregation expressions.

        Returns:
            New DataFrame with rollup aggregated results.
        """
        # Get all unique values for each rollup column
        unique_values = {}
        for col in self.rollup_columns:
            unique_values[col] = list(
                {row.get(col) for row in self.df.data if row.get(col) is not None}
            )

        result_data = []

        # Track which result keys are from count/rank functions (non-nullable)
        non_nullable_keys = set()

        # Generate rollup combinations: for each level i, group by first i columns
        for i in range(len(self.rollup_columns) + 1):
            if i == 0:
                # Grand total - all nulls
                filtered_rows = self.df.data
                result_row = dict.fromkeys(self.rollup_columns)

                # Apply aggregations
                for expr in exprs:
                    if isinstance(expr, str):
                        result_key, result_value = self._evaluate_string_expression(
                            expr, filtered_rows
                        )
                        # Check if this is a count function
                        if expr.startswith("count("):
                            non_nullable_keys.add(result_key)
                        result_row[result_key] = result_value
                    elif hasattr(expr, "function_name"):
                        from typing import cast

                        result_key, result_value = self._evaluate_aggregate_function(
                            cast("AggregateFunction", expr), filtered_rows
                        )
                        # Check if this is a count function
                        if expr.function_name == "count":
                            non_nullable_keys.add(result_key)
                        result_row[result_key] = result_value
                    elif hasattr(expr, "name") and isinstance(
                        expr, (Column, ColumnOperation)
                    ):
                        result_key, result_value = self._evaluate_column_expression(
                            expr, filtered_rows
                        )
                        result_row[result_key] = result_value
                    elif isinstance(expr, dict):
                        # Skip dict expressions - should have been converted already
                        pass

                result_data.append(result_row)
            else:
                # Group by first i columns
                active_columns = self.rollup_columns[:i]
                inactive_columns = self.rollup_columns[i:]

                # Create groups based on active columns
                groups: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
                for row in self.df.data:
                    group_key = tuple(row.get(col) for col in active_columns)
                    if group_key not in groups:
                        groups[group_key] = []
                    groups[group_key].append(row)

                # Process each group
                for group_key, group_rows in groups.items():
                    result_row = {}
                    # Set active column values
                    for j, col in enumerate(active_columns):
                        result_row[col] = group_key[j]
                    # Set inactive column values to None
                    for col in inactive_columns:
                        result_row[col] = None

                    # Apply aggregations to this group
                    for expr in exprs:
                        if isinstance(expr, str):
                            result_key, result_value = self._evaluate_string_expression(
                                expr, group_rows
                            )
                            # Check if this is a count function
                            if expr.startswith("count("):
                                non_nullable_keys.add(result_key)
                            result_row[result_key] = result_value
                        elif hasattr(expr, "function_name"):
                            from typing import cast

                            result_key, result_value = (
                                self._evaluate_aggregate_function(
                                    cast("AggregateFunction", expr), group_rows
                                )
                            )
                            # Check if this is a count function
                            if expr.function_name == "count":
                                non_nullable_keys.add(result_key)
                            result_row[result_key] = result_value
                        elif hasattr(expr, "name") and isinstance(
                            expr, (Column, ColumnOperation)
                        ):
                            result_key, result_value = self._evaluate_column_expression(
                                expr, group_rows
                            )
                            result_row[result_key] = result_value
                        elif isinstance(expr, dict):
                            # Skip dict expressions - should have been converted already
                            pass

                    result_data.append(result_row)

        # Create result DataFrame with proper schema
        from ...spark_types import (
            StructType,
            StructField,
            StringType,
            LongType,
            DoubleType,
        )
        from ..dataframe import DataFrame

        if not result_data:
            return DataFrame(result_data, StructType([]), self.df.storage)

        fields = []
        for key, value in result_data[0].items():
            if key in self.rollup_columns:
                fields.append(StructField(key, StringType()))
            else:
                # Count functions, window ranking functions, and boolean functions are non-nullable in PySpark
                is_count_function = key in non_nullable_keys or any(
                    key.startswith(func)
                    for func in [
                        "count(",
                        "count(1)",
                        "count(DISTINCT",
                        "count_if",
                        "row_number",
                        "rank",
                        "dense_rank",
                        "row_num",
                        "dept_row_num",
                        "global_row",
                        "dept_row",
                        "dept_rank",
                    ]
                )
                is_boolean_function = any(
                    key.startswith(func)
                    for func in ["coalesced_", "is_null_", "is_nan_"]
                )
                nullable = not (is_count_function or is_boolean_function)

                if isinstance(value, int):
                    fields.append(
                        StructField(key, LongType(nullable=nullable), nullable=nullable)
                    )
                elif isinstance(value, float):
                    fields.append(
                        StructField(
                            key, DoubleType(nullable=nullable), nullable=nullable
                        )
                    )
                else:
                    # Fallback for any other type
                    fields.append(
                        StructField(
                            key, StringType(nullable=nullable), nullable=nullable
                        )
                    )
        schema = StructType(fields)
        return DataFrame(result_data, schema, self.df.storage)
