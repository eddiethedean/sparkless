"""
Window function handler for Polars.

This module handles window functions using Polars `.over()` expressions.
"""

from typing import List, Optional
import sys
import polars as pl
from sparkless.functions.window_execution import WindowFunction


class PolarsWindowHandler:
    """Handles window functions using Polars expressions."""

    def _resolve_col(
        self, col_name: str, df: pl.DataFrame, case_sensitive: bool
    ) -> str:
        """Resolve partition/order column name against DataFrame schema."""
        from sparkless.core.column_resolver import ColumnResolver

        resolved = ColumnResolver.resolve_column_name(
            col_name, list(df.columns), case_sensitive
        )
        return resolved if resolved is not None else col_name

    def translate_window_function(
        self,
        window_func: WindowFunction,
        df: pl.DataFrame,
        *,
        case_sensitive: bool = False,
    ) -> pl.Expr:
        """Translate window function to Polars expression.

        Args:
            window_func: WindowFunction instance
            df: Polars DataFrame (for context)
            case_sensitive: Whether to use case-sensitive column matching.

        Returns:
            Polars expression with window function
        """
        function_name = window_func.function_name.upper()
        window_spec = window_func.window_spec

        # Build partition_by
        partition_by: List[pl.Expr] = []
        if hasattr(window_spec, "_partition_by") and window_spec._partition_by:
            for col in window_spec._partition_by:
                if isinstance(col, str):
                    name = self._resolve_col(col, df, case_sensitive)
                    partition_by.append(pl.col(name))
                elif hasattr(col, "name"):
                    name = self._resolve_col(col.name, df, case_sensitive)
                    partition_by.append(pl.col(name))

        # Build order_by - handle multiple columns with different directions
        order_by: List[pl.Expr] = []
        order_descending = False  # Track if any order column is descending
        if hasattr(window_spec, "_order_by") and window_spec._order_by:
            for col in window_spec._order_by:
                if isinstance(col, str):
                    name = self._resolve_col(col, df, case_sensitive)
                    order_by.append(pl.col(name))
                elif hasattr(col, "operation") and col.operation == "desc":
                    order_descending = True  # At least one column is descending
                    # Extract base column name from ColumnOperation
                    if hasattr(col, "column"):
                        if hasattr(col.column, "name"):
                            col_name = col.column.name
                        elif hasattr(col.column, "column") and hasattr(
                            col.column.column, "name"
                        ):
                            col_name = col.column.column.name
                        else:
                            col_name = str(col.column)
                    else:
                        col_name = col.name if hasattr(col, "name") else str(col)
                    if col_name.endswith(" DESC"):
                        col_name = col_name[:-5]
                    name = self._resolve_col(col_name, df, case_sensitive)
                    order_by.append(pl.col(name))
                elif hasattr(col, "operation") and col.operation == "asc":
                    if hasattr(col, "column"):
                        if hasattr(col.column, "name"):
                            col_name = col.column.name
                        elif hasattr(col.column, "column") and hasattr(
                            col.column.column, "name"
                        ):
                            col_name = col.column.column.name
                        else:
                            col_name = str(col.column)
                    else:
                        col_name = col.name if hasattr(col, "name") else str(col)
                    if col_name.endswith(" ASC"):
                        col_name = col_name[:-4]
                    name = self._resolve_col(col_name, df, case_sensitive)
                    order_by.append(pl.col(name))
                else:
                    name = self._resolve_col(col.name, df, case_sensitive)
                    order_by.append(pl.col(name))

        # Check for window frames (rows_between or range_between)
        rows_between = None
        if hasattr(window_spec, "_rows_between") and window_spec._rows_between:
            rows_between = window_spec._rows_between

        # Get column expression if available
        column_expr: Optional[pl.Expr] = None
        # Check for dummy columns (functions without real columns like rank(), row_number())
        dummy_columns = {
            "__rank__",
            "__dense_rank__",
            "__row_number__",
            "__cume_dist__",
            "__percent_rank__",
            "__ntile__",
        }

        # Try to get column from window_func.column_name first (set in WindowFunction.__init__)
        if hasattr(window_func, "column_name") and window_func.column_name:
            # Skip dummy columns - these functions don't take a real column
            if window_func.column_name not in dummy_columns:
                column_expr = pl.col(window_func.column_name)
        elif hasattr(window_func, "function") and hasattr(
            window_func.function, "column"
        ):
            col = window_func.function.column
            if isinstance(col, str):
                if col not in dummy_columns:
                    column_expr = pl.col(col)
            elif hasattr(col, "name") and col.name not in dummy_columns:
                column_expr = pl.col(col.name)

        # Build window expression based on function name
        if function_name == "ROW_NUMBER":
            # Polars doesn't have row_number, use int_range + 1 for 1-based indexing
            if partition_by:
                if order_by:
                    return (pl.int_range(pl.len()) + 1).over(
                        partition_by,
                        order_by=order_by,
                        descending=order_descending,
                    )
                else:
                    return (pl.int_range(pl.len()) + 1).over(partition_by)
            else:
                return pl.int_range(pl.len()) + 1
        elif function_name == "RANK":
            # PySpark rank() uses min tie-breaking (1, 1, 3); Polars defaults to average
            # Note: Do NOT pass descending to .over() - it affects ranking direction.
            # Instead, sort the DataFrame before applying the window function.
            if column_expr is not None:
                if partition_by:
                    if order_by:
                        return column_expr.rank(method="min").over(
                            partition_by,
                            order_by=order_by,
                        )
                    else:
                        return column_expr.rank(method="min").over(partition_by)
                else:
                    if order_by:
                        return column_expr.rank(method="min").over(order_by=order_by)
                    else:
                        return column_expr.rank(method="min")
            else:
                # Fallback: use first order_by column (rank() doesn't take a column)
                # For rank() without a column, PySpark ranks by position in ordered window
                # Polars ranks by value, so we need a workaround
                # This will be handled in operation_executor by adding row_number first
                if order_by:
                    # Use first order column for ranking (rank doesn't take a column argument)
                    order_col = order_by[0]
                    if partition_by:
                        return order_col.rank(method="min").over(
                            partition_by,
                            order_by=order_by,
                        )
                    else:
                        return order_col.rank(method="min").over(order_by=order_by)
                else:
                    # No order_by and no column - use row number as fallback
                    if partition_by:
                        return (pl.int_range(pl.len()) + 1).over(partition_by)
                    else:
                        return pl.int_range(pl.len()) + 1
        elif function_name == "DENSE_RANK":
            # Note: Do NOT pass descending to .over() - it affects ranking direction.
            # Instead, sort the DataFrame before applying the window function.
            if column_expr is not None:
                if partition_by:
                    if order_by:
                        return column_expr.rank(method="dense").over(
                            partition_by,
                            order_by=order_by,
                        )
                    else:
                        return column_expr.rank(method="dense").over(partition_by)
                else:
                    if order_by:
                        return column_expr.rank(method="dense").over(order_by=order_by)
                    else:
                        return column_expr.rank(method="dense")
            else:
                # Fallback: use first order_by column (dense_rank() doesn't take a column)
                if order_by:
                    order_col = order_by[0]
                    if partition_by:
                        return order_col.rank(method="dense").over(
                            partition_by,
                            order_by=order_by,
                        )
                    else:
                        return order_col.rank(method="dense").over(order_by=order_by)
                else:
                    # No order_by and no column - use row number as fallback
                    if partition_by:
                        return (pl.int_range(pl.len()) + 1).over(partition_by)
                    else:
                        return pl.int_range(pl.len()) + 1
        elif function_name == "SUM":
            if column_expr is not None:
                # Handle rows_between frames
                # For complex frames like rowsBetween(currentRow, unboundedFollowing),
                # Polars doesn't support nested window functions, so we raise ValueError
                # to trigger Python evaluation fallback
                if rows_between:
                    start, end = rows_between
                    # For rowsBetween(currentRow, unboundedFollowing): sum from current row to end
                    # This requires reverse cumulative sum which Polars doesn't support natively
                    if (
                        start == 0 and end == sys.maxsize
                    ):  # currentRow to unboundedFollowing
                        # Raise ValueError to trigger Python evaluation
                        raise ValueError(
                            "rowsBetween(currentRow, unboundedFollowing) requires Python evaluation"
                        )
                    # For other frame types, fall through to default behavior

                if partition_by:
                    if order_by:
                        # With orderBy, PySpark uses default frame UNBOUNDED PRECEDING AND CURRENT ROW
                        # So sum() returns a running sum (cumulative sum)
                        # Use cum_sum() which works on expressions
                        return column_expr.cum_sum().over(
                            partition_by,
                            order_by=order_by,
                            descending=order_descending,
                        )
                    else:
                        return column_expr.sum().over(partition_by)
                else:
                    if order_by:
                        # Running sum without partition
                        return column_expr.cum_sum().over(
                            order_by=order_by, descending=order_descending
                        )
                    else:
                        return column_expr.sum()
            else:
                raise ValueError("SUM window function requires a column")
        elif function_name == "AVG" or function_name == "MEAN":
            if column_expr is not None:
                if partition_by:
                    if order_by:
                        # With orderBy, PySpark uses default frame UNBOUNDED PRECEDING AND CURRENT ROW
                        # So avg() returns a running average (cumulative average)
                        # Use cum_sum() / row_number() to compute running average
                        cumsum_expr = column_expr.cum_sum().over(
                            partition_by,
                            order_by=order_by,
                            descending=order_descending,
                        )
                        row_num_expr = (pl.int_range(pl.len()) + 1).over(
                            partition_by,
                            order_by=order_by,
                            descending=order_descending,
                        )
                        return cumsum_expr / row_num_expr
                    else:
                        return column_expr.mean().over(partition_by)
                else:
                    if order_by:
                        # Running average without partition
                        cumsum_expr = column_expr.cum_sum().over(
                            order_by=order_by, descending=order_descending
                        )
                        row_num_expr = (pl.int_range(pl.len()) + 1).over(
                            order_by=order_by, descending=order_descending
                        )
                        return cumsum_expr / row_num_expr
                    else:
                        return column_expr.mean()
            else:
                raise ValueError("AVG window function requires a column")
        elif function_name == "COUNT":
            if column_expr is not None:
                if partition_by:
                    return column_expr.count().over(partition_by)
                else:
                    return column_expr.count()
            else:
                # COUNT(*)
                if partition_by:
                    return pl.len().over(partition_by)
                else:
                    return pl.len()
        elif function_name == "MAX":
            if column_expr is not None:
                if partition_by:
                    return column_expr.max().over(partition_by)
                else:
                    return column_expr.max()
            else:
                raise ValueError("MAX window function requires a column")
        elif function_name == "MIN":
            if column_expr is not None:
                if partition_by:
                    return column_expr.min().over(partition_by)
                else:
                    return column_expr.min()
            else:
                raise ValueError("MIN window function requires a column")
        elif (
            function_name == "COUNTDISTINCT" or function_name == "APPROX_COUNT_DISTINCT"
        ):
            if column_expr is not None:
                if partition_by:
                    # Use n_unique() for approximate distinct count (similar to approx_count_distinct)
                    return column_expr.n_unique().over(partition_by)
                else:
                    return column_expr.n_unique()
            else:
                raise ValueError(
                    "APPROX_COUNT_DISTINCT window function requires a column"
                )
        elif function_name == "LAG":
            if column_expr is not None:
                offset = getattr(window_func, "offset", 1)
                default = getattr(window_func, "default", None)
                # Polars shift() takes periods as first arg, fill_value as keyword
                shift_expr = (
                    column_expr.shift(offset, fill_value=default)
                    if default is not None
                    else column_expr.shift(offset)
                )
                if partition_by:
                    if order_by:
                        return shift_expr.over(
                            partition_by,
                            order_by=order_by,
                            descending=order_descending,
                        )
                    else:
                        return shift_expr.over(partition_by)
                else:
                    return shift_expr
            else:
                raise ValueError("LAG window function requires a column")
        elif function_name == "LEAD":
            if column_expr is not None:
                offset = getattr(window_func, "offset", 1)
                default = getattr(window_func, "default", None)
                # Polars shift() takes periods as first arg, fill_value as keyword
                # LEAD uses negative offset
                shift_expr = (
                    column_expr.shift(-offset, fill_value=default)
                    if default is not None
                    else column_expr.shift(-offset)
                )
                if partition_by:
                    if order_by:
                        return shift_expr.over(
                            partition_by,
                            order_by=order_by,
                            descending=order_descending,
                        )
                    else:
                        return shift_expr.over(partition_by)
                else:
                    return shift_expr
            else:
                raise ValueError("LEAD window function requires a column")
        elif function_name in ("FIRST", "FIRST_VALUE"):
            if column_expr is not None:
                if partition_by:
                    if order_by:
                        return column_expr.first().over(
                            partition_by,
                            order_by=order_by,
                            descending=order_descending,
                        )
                    else:
                        return column_expr.first().over(partition_by)
                else:
                    return column_expr.first()
            else:
                raise ValueError("FIRST/FIRST_VALUE window function requires a column")
        elif function_name in ("LAST", "LAST_VALUE"):
            if column_expr is not None:
                if partition_by:
                    if order_by:
                        # With orderBy, PySpark's default frame is UNBOUNDED PRECEDING AND CURRENT ROW
                        # So last() returns the current row's value (last in the frame up to current row)
                        # Use max() with the same frame to get the current row's value
                        # Actually, we need to use a range frame: rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
                        # But Polars doesn't support range frames directly, so we use a workaround:
                        # For each row, last() with orderBy returns that row's value
                        # We can use max() over the frame, but Polars doesn't have frame support
                        # So we'll use a different approach: use the column itself (current row value)
                        # when orderBy is present, as that's what PySpark does with default frame
                        return column_expr.over(
                            partition_by,
                            order_by=order_by,
                            descending=order_descending,
                        )
                    else:
                        return column_expr.last().over(partition_by)
                else:
                    if order_by:
                        # With orderBy but no partition, return current row value
                        return column_expr.over(
                            order_by=order_by, descending=order_descending
                        )
                    else:
                        return column_expr.last()
            else:
                raise ValueError("LAST/LAST_VALUE window function requires a column")
        elif function_name == "CUME_DIST":
            # CUME_DIST requires rank-based calculation with tie handling
            # Polars approximations don't match PySpark behavior, use Python fallback
            raise ValueError(
                "CUME_DIST requires Python evaluation for correct tie handling"
            )
        elif function_name == "PERCENT_RANK":
            # PERCENT_RANK requires rank-based calculation with tie handling
            # Polars approximations don't match PySpark behavior, use Python fallback
            raise ValueError(
                "PERCENT_RANK requires Python evaluation for correct tie handling"
            )
        elif function_name == "NTH_VALUE":
            # NTH_VALUE is not directly available in Polars, use Python fallback
            raise ValueError("NTH_VALUE requires Python evaluation")
        elif function_name == "NTILE":
            # NTILE is not directly available in Polars, use Python fallback
            raise ValueError("NTILE requires Python evaluation")
        else:
            raise ValueError(f"Unsupported window function: {function_name}")
