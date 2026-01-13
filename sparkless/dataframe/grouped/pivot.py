"""
Pivot grouped data implementation for Sparkless.

This module provides pivot grouped data functionality for pivot table
operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from typing import Any, Dict, List, TYPE_CHECKING, Tuple, Union

from ...functions import Column, ColumnOperation, AggregateFunction
from ..protocols import SupportsDataFrameOps

if TYPE_CHECKING:
    from ..dataframe import DataFrame


class PivotGroupedData:
    """Mock pivot grouped data for pivot table operations."""

    def __init__(
        self,
        df: SupportsDataFrameOps,
        group_columns: List[str],
        pivot_col: str,
        pivot_values: List[Any],
    ):
        """Initialize PivotGroupedData.

        Args:
            df: The DataFrame being grouped.
            group_columns: List of column names to group by.
            pivot_col: Column to pivot on.
            pivot_values: List of pivot values.
        """
        self.df = df
        self.group_columns = group_columns
        self.pivot_col = pivot_col
        self.pivot_values = pivot_values

    def agg(
        self, *exprs: Union[str, Column, ColumnOperation, AggregateFunction]
    ) -> "DataFrame":
        """Aggregate pivot grouped data.

        Creates pivot table with pivot columns as separate columns.

        Args:
            *exprs: Aggregation expressions.

        Returns:
            New DataFrame with pivot aggregated results.
        """
        # Group data by group columns
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for row in self.df.data:
            group_key = tuple(row.get(col) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)

        result_data = []

        for group_key, group_rows in groups.items():
            result_row = dict(zip(self.group_columns, group_key))

            # For each pivot value, filter rows and apply aggregation
            for pivot_value in self.pivot_values:
                pivot_rows = [
                    row for row in group_rows if row.get(self.pivot_col) == pivot_value
                ]

                for expr in exprs:
                    if isinstance(expr, str):
                        result_key, result_value = self._evaluate_string_expression(
                            expr, pivot_rows
                        )
                        # Create pivot column name
                        pivot_col_name = f"{result_key}_{pivot_value}"
                        result_row[pivot_col_name] = result_value
                    elif hasattr(expr, "function_name"):
                        from typing import cast

                        result_key, result_value = self._evaluate_aggregate_function(
                            cast("AggregateFunction", expr), pivot_rows
                        )
                        # Create pivot column name
                        pivot_col_name = f"{result_key}_{pivot_value}"
                        result_row[pivot_col_name] = result_value
                    elif hasattr(expr, "name"):
                        result_key, result_value = self._evaluate_column_expression(
                            expr, pivot_rows
                        )
                        # Create pivot column name
                        pivot_col_name = f"{result_key}_{pivot_value}"
                        result_row[pivot_col_name] = result_value

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

        if result_data:
            fields = []
            for key, value in result_data[0].items():
                if key in self.group_columns:
                    fields.append(StructField(key, StringType()))
                elif isinstance(value, int):
                    fields.append(StructField(key, LongType()))
                elif isinstance(value, float):
                    fields.append(StructField(key, DoubleType()))
                else:
                    fields.append(StructField(key, StringType()))
            schema = StructType(fields)
            return DataFrame(result_data, schema, self.df.storage)
        else:
            return DataFrame(result_data, StructType([]), self.df.storage)

    def _evaluate_string_expression(
        self, expr: str, group_rows: List[Dict[str, Any]]
    ) -> Tuple[str, Any]:
        """Evaluate string aggregation expression (reused from GroupedData)."""
        if expr.startswith("sum("):
            col_name = expr[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr, sum(values) if values else 0
        elif expr.startswith("avg("):
            col_name = expr[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr, sum(values) / len(values) if values else 0
        elif expr.startswith("count("):
            return expr, len(group_rows)
        elif expr.startswith("max("):
            col_name = expr[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr, max(values) if values else None
        elif expr.startswith("min("):
            col_name = expr[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr, min(values) if values else None
        else:
            return expr, None

    def _evaluate_aggregate_function(
        self, expr: AggregateFunction, group_rows: List[Dict[str, Any]]
    ) -> Tuple[str, Any]:
        """Evaluate AggregateFunction (reused from GroupedData)."""
        func_name = expr.function_name
        col_name = (
            getattr(expr, "column_name", "") if hasattr(expr, "column_name") else ""
        )

        # Check if the function has an alias set
        has_alias = expr.name != expr._generate_name()
        alias_name = expr.name if has_alias else None

        if func_name == "sum":
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"sum({col_name})"
            return result_key, sum(values) if values else 0
        elif func_name == "avg":
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"avg({col_name})"
            return result_key, sum(values) / len(values) if values else 0
        elif func_name == "count":
            if col_name == "*" or col_name == "":
                result_key = alias_name if alias_name else expr._generate_name()
                return result_key, len(group_rows)
            else:
                result_key = alias_name if alias_name else f"count({col_name})"
                return result_key, len(group_rows)
        elif func_name == "max":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"max({col_name})"
            return result_key, max(values) if values else None
        elif func_name == "min":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"min({col_name})"
            return result_key, min(values) if values else None
        else:
            result_key = alias_name if alias_name else f"{func_name}({col_name})"
            return result_key, None

    def _evaluate_column_expression(
        self,
        expr: Union[Column, ColumnOperation],
        group_rows: List[Dict[str, Any]],
    ) -> Tuple[str, Any]:
        """Evaluate Column or ColumnOperation (reused from GroupedData)."""
        expr_name = expr.name
        if expr_name.startswith("sum("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr_name, sum(values) if values else 0
        elif expr_name.startswith("avg("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name, 0)
                for row in group_rows
                if row.get(col_name) is not None
            ]
            return expr_name, sum(values) / len(values) if values else 0
        elif expr_name.startswith("count("):
            return expr_name, len(group_rows)
        elif expr_name.startswith("max("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, max(values) if values else None
        elif expr_name.startswith("min("):
            col_name = expr_name[4:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, min(values) if values else None
        else:
            return expr_name, None
