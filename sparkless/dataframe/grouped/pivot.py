"""
Pivot grouped data implementation for Sparkless.

This module provides pivot grouped data functionality for pivot table
operations, maintaining compatibility with PySpark's GroupedData interface.
"""

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, Union

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

                # If no rows for this pivot value, set to None for all expressions
                if not pivot_rows:
                    for expr in exprs:
                        # Determine column name
                        if len(exprs) == 1:
                            # Check for alias
                            has_alias = False
                            alias_name = None
                            if isinstance(expr, ColumnOperation) and hasattr(
                                expr, "_aggregate_function"
                            ):
                                agg_func = expr._aggregate_function
                                has_alias = (
                                    hasattr(expr, "_alias_name")
                                    and expr._alias_name is not None
                                ) or (
                                    agg_func is not None
                                    and hasattr(agg_func, "name")
                                    and hasattr(agg_func, "_generate_name")
                                    and agg_func.name != agg_func._generate_name()
                                )
                                if has_alias:
                                    alias_name = getattr(expr, "_alias_name", None) or (
                                        agg_func.name if agg_func is not None else None
                                    )
                                    if alias_name is None:
                                        alias_name = "unknown"
                            elif hasattr(expr, "function_name") and not isinstance(
                                expr, str
                            ):
                                # expr is AggregateFunction, has name and _generate_name
                                if hasattr(expr, "name") and hasattr(
                                    expr, "_generate_name"
                                ):
                                    # expr is AggregateFunction here, so it has name attribute
                                    expr_name = getattr(expr, "name")
                                    expr_generated = getattr(expr, "_generate_name")()
                                    if expr_name != expr_generated:
                                        has_alias = True
                                        alias_name = expr_name
                            elif hasattr(expr, "name"):
                                if hasattr(expr, "_alias_name"):
                                    has_alias = expr._alias_name is not None
                                    if has_alias:
                                        alias_name = expr._alias_name

                            if has_alias:
                                pivot_col_name = alias_name
                            else:
                                pivot_col_name = str(pivot_value)
                        else:
                            # Multiple expressions - check for alias and use appropriate format
                            has_alias = False
                            alias_name = None
                            result_key = None

                            if isinstance(expr, ColumnOperation) and hasattr(
                                expr, "_aggregate_function"
                            ):
                                agg_func = expr._aggregate_function
                                # Check for alias on ColumnOperation first
                                if (
                                    hasattr(expr, "_alias_name")
                                    and expr._alias_name is not None
                                ):
                                    has_alias = True
                                    alias_name = expr._alias_name
                                elif agg_func is not None:
                                    result_key = agg_func.name
                                else:
                                    result_key = "unknown"
                            elif hasattr(expr, "function_name") and not isinstance(
                                expr, str
                            ):
                                # Direct AggregateFunction
                                if hasattr(expr, "name") and hasattr(
                                    expr, "_generate_name"
                                ):
                                    # expr is AggregateFunction here
                                    expr_name = getattr(expr, "name")
                                    expr_generated = getattr(expr, "_generate_name")()
                                    if expr_name != expr_generated:
                                        has_alias = True
                                        alias_name = expr_name
                                    else:
                                        result_key = expr_name
                                else:
                                    result_key = getattr(expr, "name", str(expr))
                            elif isinstance(expr, str):
                                result_key = expr
                            else:
                                # Check for alias on ColumnOperation
                                if (
                                    hasattr(expr, "_alias_name")
                                    and expr._alias_name is not None
                                ):
                                    has_alias = True
                                    alias_name = expr._alias_name
                                else:
                                    result_key = expr.name

                            # For multiple expressions: PySpark uses {pivot_value}_{alias} or {pivot_value}_{function_name}
                            if has_alias and alias_name:
                                pivot_col_name = f"{pivot_value}_{alias_name}"
                            else:
                                # result_key should always be set in this branch
                                if result_key is not None:
                                    pivot_col_name = f"{pivot_value}_{result_key}"
                                else:
                                    pivot_col_name = f"{pivot_value}_unknown"
                        # pivot_col_name is always set above
                        assert pivot_col_name is not None, (
                            "pivot_col_name should always be set"
                        )
                        result_row[pivot_col_name] = None
                    continue

                # PySpark column naming rules:
                # - Single expression, no alias: use pivot value as column name
                # - Single expression with alias: use alias as column name (not alias_pivot)
                # - Multiple expressions: use function_name_pivot or alias_pivot
                for expr in exprs:
                    # Check if this is a ColumnOperation wrapping an AggregateFunction
                    agg_func = None
                    if isinstance(expr, ColumnOperation) and hasattr(
                        expr, "_aggregate_function"
                    ):
                        agg_func = expr._aggregate_function

                    if isinstance(expr, str):
                        result_key, result_value = self._evaluate_string_expression(
                            expr, pivot_rows
                        )
                        # Create pivot column name based on number of expressions
                        if len(exprs) == 1:
                            # Single expression: use pivot value as column name
                            pivot_col_name = str(pivot_value)
                        else:
                            # Multiple expressions: use function_name_pivot format
                            pivot_col_name = f"{result_key}_{pivot_value}"
                        result_row[pivot_col_name] = result_value
                    elif agg_func is not None:
                        # ColumnOperation wrapping AggregateFunction
                        result_key, result_value = self._evaluate_aggregate_function(
                            agg_func, pivot_rows
                        )
                        # Check if there's an alias
                        # For ColumnOperation, check _alias_name first, then check if name differs from generated name
                        has_alias = False
                        alias_name = None
                        if (
                            hasattr(expr, "_alias_name")
                            and expr._alias_name is not None
                        ):
                            has_alias = True
                            alias_name = expr._alias_name
                        elif hasattr(agg_func, "name") and hasattr(
                            agg_func, "_generate_name"
                        ):
                            # Check if the aggregate function name differs from generated name
                            if agg_func.name != agg_func._generate_name():
                                has_alias = True
                                alias_name = agg_func.name

                        # Create pivot column name based on number of expressions and alias
                        if len(exprs) == 1:
                            if has_alias and alias_name:
                                # Single expression with alias: use alias as column name
                                pivot_col_name = alias_name
                            else:
                                # Single expression, no alias: use pivot value as column name
                                pivot_col_name = str(pivot_value)
                        else:
                            # Multiple expressions: PySpark uses {pivot_value}_{alias} or {pivot_value}_{function_name}
                            if has_alias and alias_name:
                                pivot_col_name = f"{pivot_value}_{alias_name}"
                            else:
                                # No alias: use {pivot_value}_{function_name}
                                # result_key is always set from _evaluate_aggregate_function
                                pivot_col_name = f"{pivot_value}_{result_key}"
                        result_row[pivot_col_name] = result_value
                    elif hasattr(expr, "function_name") and not isinstance(
                        expr, (str, ColumnOperation)
                    ):
                        # Direct AggregateFunction
                        from typing import cast

                        result_key, result_value = self._evaluate_aggregate_function(
                            cast("AggregateFunction", expr), pivot_rows
                        )
                        # Check if there's an alias (alias != generated name)
                        has_alias_direct = False
                        alias_name_direct: Optional[str] = None
                        if hasattr(expr, "name") and hasattr(expr, "_generate_name"):
                            # expr is AggregateFunction here, so it has name and _generate_name
                            expr_name = getattr(expr, "name")
                            expr_generated = getattr(expr, "_generate_name")()
                            if expr_name != expr_generated:
                                has_alias_direct = True
                                alias_name_direct = expr_name
                        # Create pivot column name based on number of expressions and alias
                        if len(exprs) == 1:
                            if has_alias_direct and alias_name_direct:
                                # Single expression with alias: use alias as column name
                                pivot_col_name = alias_name_direct
                            else:
                                # Single expression, no alias: use pivot value as column name
                                pivot_col_name = str(pivot_value)
                        else:
                            # Multiple expressions: PySpark uses {pivot_value}_{alias} or {pivot_value}_{function_name}
                            if has_alias_direct and alias_name_direct:
                                pivot_col_name = f"{pivot_value}_{alias_name_direct}"
                            else:
                                # No alias: use {pivot_value}_{function_name}
                                pivot_col_name = f"{pivot_value}_{result_key}"
                        result_row[pivot_col_name] = result_value
                    elif hasattr(expr, "name") and not isinstance(expr, str):
                        # Column or ColumnOperation without _aggregate_function
                        # At this point, expr must be Union[Column, ColumnOperation]
                        # based on the previous checks
                        assert isinstance(expr, (Column, ColumnOperation)), (
                            "Expected Column or ColumnOperation"
                        )
                        result_key, result_value = self._evaluate_column_expression(
                            expr, pivot_rows
                        )
                        # Check if this is a ColumnOperation with an alias
                        has_alias = False
                        if hasattr(expr, "_alias_name"):
                            has_alias = expr._alias_name is not None
                        elif (
                            isinstance(expr, ColumnOperation)
                            and hasattr(expr, "column")
                            and hasattr(expr, "operation")
                        ):
                            # Check if name differs from what would be generated
                            expected_name = f"{expr.operation}({expr.column.name if hasattr(expr.column, 'name') else expr.column})"
                            has_alias = expr.name != expected_name
                        # Create pivot column name
                        if len(exprs) == 1:
                            if has_alias:
                                # Single expression with alias: use alias as column name
                                pivot_col_name = expr.name
                            else:
                                # Single expression, no alias: use pivot value as column name
                                pivot_col_name = str(pivot_value)
                        else:
                            # Multiple expressions: PySpark uses {pivot_value}_{alias} or {pivot_value}_{function_name}
                            if has_alias:
                                pivot_col_name = f"{pivot_value}_{expr.name}"
                            else:
                                # No alias: use {pivot_value}_{function_name}
                                pivot_col_name = f"{pivot_value}_{result_key}"
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
        import statistics

        func_name = expr.function_name
        col_name = (
            getattr(expr, "column_name", "") if hasattr(expr, "column_name") else ""
        )

        # Use the name from the aggregate function (already set correctly by _generate_name)
        alias_name = expr.name

        if func_name == "sum":
            # Simple column: validate and sum
            values = []
            for row in group_rows:
                val = row.get(col_name)
                if val is not None:
                    if isinstance(val, bool):
                        val = 1 if val else 0
                    if isinstance(val, str):
                        try:
                            val = float(val) if "." in val else int(val)
                        except ValueError:
                            continue
                    values.append(val)
            result_key = alias_name if alias_name else f"sum({col_name})"
            return result_key, sum(values) if values else 0
        elif func_name == "avg":
            values = []
            for row in group_rows:
                val = row.get(col_name)
                if val is not None:
                    if isinstance(val, bool):
                        val = 1 if val else 0
                    if isinstance(val, str):
                        try:
                            val = float(val) if "." in val else int(val)
                        except ValueError:
                            continue
                    values.append(val)
            result_key = alias_name if alias_name else f"avg({col_name})"
            return result_key, (sum(values) / len(values)) if values else None
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
        elif func_name == "collect_list":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"collect_list({col_name})"
            return result_key, values
        elif func_name == "collect_set":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"collect_set({col_name})"
            return result_key, list(set(values))
        elif func_name == "first":
            ignorenulls = getattr(expr, "ignorenulls", False)
            if ignorenulls:
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                result_key = alias_name if alias_name else f"first({col_name})"
                return result_key, values[0] if values else None
            else:
                result_key = alias_name if alias_name else f"first({col_name})"
                if group_rows:
                    return result_key, group_rows[0].get(col_name)
                else:
                    return result_key, None
        elif func_name == "last":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"last({col_name})"
            return result_key, values[-1] if values else None
        elif func_name == "stddev" or func_name == "std":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"stddev({col_name})"
            if len(values) <= 1:
                return result_key, None
            return result_key, statistics.stdev(values)
        elif func_name == "variance":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"variance({col_name})"
            if len(values) <= 1:
                return result_key, None
            return result_key, statistics.variance(values)
        elif func_name == "mean":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"avg({col_name})"
            return result_key, statistics.mean(values) if values else None
        elif func_name == "approx_count_distinct":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            distinct_count = len(set(values))
            result_key = (
                alias_name if alias_name else f"approx_count_distinct({col_name})"
            )
            return result_key, distinct_count
        elif func_name == "countDistinct":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            distinct_count = len(set(values))
            result_key = alias_name if alias_name else f"count({col_name})"
            return result_key, distinct_count
        elif func_name == "stddev_pop":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"stddev_pop({col_name})"
            return result_key, statistics.pstdev(values) if len(values) > 0 else None
        elif func_name == "stddev_samp":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"stddev_samp({col_name})"
            return result_key, statistics.stdev(values) if len(values) > 1 else None
        elif func_name == "var_pop":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"var_pop({col_name})"
            return result_key, statistics.pvariance(values) if len(values) > 0 else None
        elif func_name == "var_samp":
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            result_key = alias_name if alias_name else f"var_samp({col_name})"
            return result_key, statistics.variance(values) if len(values) > 1 else None
        elif func_name == "skewness":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"skewness({col_name})"
            if values and len(values) > 2:
                mean_val = statistics.mean(values)
                std_val = statistics.stdev(values)
                if std_val > 0:
                    skewness = sum((x - mean_val) ** 3 for x in values) / (
                        len(values) * std_val**3
                    )
                    return result_key, skewness
                else:
                    return result_key, 0.0
            else:
                return result_key, None
        elif func_name == "kurtosis":
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            result_key = alias_name if alias_name else f"kurtosis({col_name})"
            if values and len(values) > 3:
                mean_val = statistics.mean(values)
                std_val = statistics.stdev(values)
                if std_val > 0:
                    kurtosis = (
                        sum((x - mean_val) ** 4 for x in values)
                        / (len(values) * std_val**4)
                        - 3
                    )
                    return result_key, kurtosis
                else:
                    return result_key, 0.0
            else:
                return result_key, None
        else:
            result_key = alias_name if alias_name else f"{func_name}({col_name})"
            return result_key, None

    def _evaluate_column_expression(
        self,
        expr: Union[Column, ColumnOperation],
        group_rows: List[Dict[str, Any]],
    ) -> Tuple[str, Any]:
        """Evaluate Column or ColumnOperation (reused from GroupedData)."""
        import statistics

        # Check if it's a ColumnOperation with an operation
        if isinstance(expr, ColumnOperation) and hasattr(expr, "operation"):
            operation = expr.operation
            if operation == "count":
                col_name = (
                    expr.column.name
                    if hasattr(expr.column, "name")
                    else str(expr.column)
                )
                count_value = sum(
                    1 for row in group_rows if row.get(col_name) is not None
                )
                return expr.name, count_value
            elif operation == "sum":
                col_name = (
                    expr.column.name
                    if hasattr(expr.column, "name")
                    else str(expr.column)
                )
                values = [
                    row.get(col_name, 0)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                return expr.name, sum(values) if values else 0
            elif operation == "avg":
                col_name = (
                    expr.column.name
                    if hasattr(expr.column, "name")
                    else str(expr.column)
                )
                values = [
                    row.get(col_name, 0)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                return expr.name, sum(values) / len(values) if values else 0
            elif operation == "max":
                col_name = (
                    expr.column.name
                    if hasattr(expr.column, "name")
                    else str(expr.column)
                )
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                return expr.name, max(values) if values else None
            elif operation == "min":
                col_name = (
                    expr.column.name
                    if hasattr(expr.column, "name")
                    else str(expr.column)
                )
                values = [
                    row.get(col_name)
                    for row in group_rows
                    if row.get(col_name) is not None
                ]
                return expr.name, min(values) if values else None

        # Fallback to name-based parsing for string expressions
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
            col_name = expr_name[6:-1] if len(expr_name) > 6 else ""
            if col_name == "*" or col_name == "":
                return expr_name, len(group_rows)
            count_value = sum(1 for row in group_rows if row.get(col_name) is not None)
            return expr_name, count_value
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
        elif expr_name.startswith("collect_list("):
            col_name = expr_name[13:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, values
        elif expr_name.startswith("collect_set("):
            col_name = expr_name[12:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, list(set(values))
        elif expr_name.startswith("first("):
            col_name = expr_name[6:-1]
            if group_rows:
                return expr_name, group_rows[0].get(col_name)
            else:
                return expr_name, None
        elif expr_name.startswith("last("):
            col_name = expr_name[5:-1]
            values = [
                row.get(col_name) for row in group_rows if row.get(col_name) is not None
            ]
            return expr_name, values[-1] if values else None
        elif expr_name.startswith("stddev(") or expr_name.startswith("std("):
            col_name = (
                expr_name[7:-1] if expr_name.startswith("stddev(") else expr_name[4:-1]
            )
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            if len(values) <= 1:
                return expr_name, None
            return expr_name, statistics.stdev(values)
        elif expr_name.startswith("variance("):
            col_name = expr_name[9:-1]
            values = [
                row.get(col_name)
                for row in group_rows
                if row.get(col_name) is not None
                and isinstance(row.get(col_name), (int, float))
            ]
            if len(values) <= 1:
                return expr_name, None
            return expr_name, statistics.variance(values)
        else:
            return expr_name, None

    # Convenience methods matching GroupedData API
    def sum(self, *columns: Union[str, Column]) -> "DataFrame":
        """Sum pivot grouped data.

        Args:
            *columns: Columns to sum.

        Returns:
            DataFrame with sum aggregations.
        """
        from ...functions.aggregate import AggregateFunctions

        if not columns:
            return self.agg(AggregateFunctions.sum("1"))

        exprs = []
        for col in columns:
            exprs.append(AggregateFunctions.sum(col))

        return self.agg(*exprs)

    def avg(self, *columns: Union[str, Column]) -> "DataFrame":
        """Average pivot grouped data.

        Args:
            *columns: Columns to average.

        Returns:
            DataFrame with average aggregations.
        """
        from ...functions.aggregate import AggregateFunctions

        if not columns:
            return self.agg(AggregateFunctions.avg("1"))

        exprs = []
        for col in columns:
            exprs.append(AggregateFunctions.avg(col))

        return self.agg(*exprs)

    def mean(self, *columns: Union[str, Column]) -> "DataFrame":
        """Mean pivot grouped data (alias for avg).

        Args:
            *columns: Columns to average.

        Returns:
            DataFrame with mean aggregations.
        """
        return self.avg(*columns)

    def count(self, *columns: Union[str, Column]) -> "DataFrame":
        """Count pivot grouped data.

        Args:
            *columns: Columns to count.

        Returns:
            DataFrame with count aggregations.
        """
        from ...functions.aggregate import AggregateFunctions

        if not columns:
            return self.agg(AggregateFunctions.count())

        exprs = []
        for col in columns:
            exprs.append(AggregateFunctions.count(col))

        return self.agg(*exprs)

    def max(self, *columns: Union[str, Column]) -> "DataFrame":
        """Max pivot grouped data.

        Args:
            *columns: Columns to get max of.

        Returns:
            DataFrame with max aggregations.
        """
        from ...functions.aggregate import AggregateFunctions

        if not columns:
            return self.agg(AggregateFunctions.max("1"))

        exprs = []
        for col in columns:
            exprs.append(AggregateFunctions.max(col))

        return self.agg(*exprs)

    def min(self, *columns: Union[str, Column]) -> "DataFrame":
        """Min pivot grouped data.

        Args:
            *columns: Columns to get min of.

        Returns:
            DataFrame with min aggregations.
        """
        from ...functions.aggregate import AggregateFunctions

        if not columns:
            return self.agg(AggregateFunctions.min("1"))

        exprs = []
        for col in columns:
            exprs.append(AggregateFunctions.min(col))

        return self.agg(*exprs)

    def count_distinct(self, *columns: Union[str, Column]) -> "DataFrame":
        """Count distinct values in columns.

        Args:
            *columns: Columns to count distinct values for.

        Returns:
            DataFrame with count distinct results.
        """
        from ...functions import count_distinct

        exprs = []
        for col in columns:
            exprs.append(count_distinct(col))

        return self.agg(*exprs)

    def collect_list(self, *columns: Union[str, Column]) -> "DataFrame":
        """Collect values into a list.

        Args:
            *columns: Columns to collect values for.

        Returns:
            DataFrame with collect_list results.
        """
        from ...functions import collect_list

        exprs = []
        for col in columns:
            exprs.append(collect_list(col))

        return self.agg(*exprs)

    def collect_set(self, *columns: Union[str, Column]) -> "DataFrame":
        """Collect unique values into a set.

        Args:
            *columns: Columns to collect unique values for.

        Returns:
            DataFrame with collect_set results.
        """
        from ...functions import collect_set

        exprs = []
        for col in columns:
            exprs.append(collect_set(col))

        return self.agg(*exprs)

    def first(self, *columns: Union[str, Column]) -> "DataFrame":
        """Get first value in each group.

        Args:
            *columns: Columns to get first values for.

        Returns:
            DataFrame with first values.
        """
        from ...functions import first

        exprs = []
        for col in columns:
            exprs.append(first(col))

        return self.agg(*exprs)

    def last(self, *columns: Union[str, Column]) -> "DataFrame":
        """Get last value in each group.

        Args:
            *columns: Columns to get last values for.

        Returns:
            DataFrame with last values.
        """
        from ...functions import last

        exprs = []
        for col in columns:
            exprs.append(last(col))

        return self.agg(*exprs)

    def stddev(self, *columns: Union[str, Column]) -> "DataFrame":
        """Calculate standard deviation.

        Args:
            *columns: Columns to calculate standard deviation for.

        Returns:
            DataFrame with standard deviation results.
        """
        from ...functions import stddev

        exprs = []
        for col in columns:
            exprs.append(stddev(col))

        return self.agg(*exprs)

    def variance(self, *columns: Union[str, Column]) -> "DataFrame":
        """Calculate variance.

        Args:
            *columns: Columns to calculate variance for.

        Returns:
            DataFrame with variance results.
        """
        from ...functions import variance

        exprs = []
        for col in columns:
            exprs.append(variance(col))

        return self.agg(*exprs)
