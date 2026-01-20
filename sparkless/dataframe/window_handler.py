"""
Window function handler for DataFrame.

This module handles window function evaluation (row_number, rank, lag, lead, etc.)
following the Single Responsibility Principle.
"""

from typing import Any, Dict, List, Tuple
import sys


class WindowFunctionHandler:
    """Handles window function evaluation (row_number, rank, lag, lead, etc.)."""

    def __init__(self, dataframe: Any):
        """Initialize window function handler.

        Args:
            dataframe: The DataFrame instance this handler belongs to
        """
        self.dataframe = dataframe

    def evaluate_window_functions(
        self, data: List[Dict[str, Any]], window_functions: List[Tuple[Any, ...]]
    ) -> List[Dict[str, Any]]:
        """Evaluate window functions across all rows."""
        result_data = data.copy()

        for window_func_info in window_functions:
            # Handle both (col_index, window_func) and (col_name, window_func) formats
            if len(window_func_info) == 2:
                col_index_or_name, window_func = window_func_info
                # If first element is an int, it's an index, use window_func.name
                # Otherwise, it's the column name to use
                if isinstance(col_index_or_name, int):
                    col_name = window_func.name
                else:
                    col_name = col_index_or_name
            else:
                # Fallback: just use window_func.name
                window_func = (
                    window_func_info[-1]
                    if isinstance(window_func_info, tuple)
                    else window_func_info
                )
                col_name = (
                    window_func.name
                    if hasattr(window_func, "name")
                    else "window_result"
                )

            if window_func.function_name == "row_number":
                # For row_number(), we need to handle partitionBy and orderBy
                if hasattr(window_func, "window_spec") and window_func.window_spec:
                    window_spec = window_func.window_spec

                    # Get partition by columns from window spec
                    partition_by_cols = getattr(window_spec, "_partition_by", [])
                    # Get order by columns from window spec
                    order_by_cols = getattr(window_spec, "_order_by", [])

                    if partition_by_cols:
                        # Handle partitioning - group by partition columns
                        partition_groups: Dict[Any, List[int]] = {}
                        for i, row in enumerate(result_data):
                            # Create partition key
                            partition_key = tuple(
                                (
                                    row.get(col.name)
                                    if hasattr(col, "name")
                                    else row.get(str(col))
                                )
                                for col in partition_by_cols
                            )
                            if partition_key not in partition_groups:
                                partition_groups[partition_key] = []
                            partition_groups[partition_key].append(i)

                        # Assign row numbers within each partition
                        for partition_indices in partition_groups.values():
                            if order_by_cols:
                                # Sort within partition by order by columns using corrected ordering logic
                                sorted_partition_indices = (
                                    self._apply_ordering_to_indices(
                                        result_data,
                                        partition_indices,
                                        order_by_cols,
                                    )
                                )
                            else:
                                # No order by - use original order within partition
                                sorted_partition_indices = partition_indices

                            # Assign row numbers starting from 1 within each partition
                            for i, original_index in enumerate(
                                sorted_partition_indices
                            ):
                                result_data[original_index][col_name] = i + 1
                    elif order_by_cols:
                        # No partitioning, just sort by order by columns using corrected ordering logic
                        sorted_indices = self._apply_ordering_to_indices(
                            result_data, list(range(len(result_data))), order_by_cols
                        )

                        # Assign row numbers based on sorted order
                        for i, original_index in enumerate(sorted_indices):
                            result_data[original_index][col_name] = i + 1
                    else:
                        # No partition or order by - just assign sequential row numbers
                        for i in range(len(result_data)):
                            result_data[i][col_name] = i + 1
                else:
                    # No window spec - assign sequential row numbers
                    for i in range(len(result_data)):
                        result_data[i][col_name] = i + 1
            elif window_func.function_name == "lag":
                # Handle lag function - get previous row value
                self._evaluate_lag_lead(
                    result_data, window_func, col_name, is_lead=False
                )
            elif window_func.function_name == "lead":
                # Handle lead function - get next row value
                self._evaluate_lag_lead(
                    result_data, window_func, col_name, is_lead=True
                )
            elif window_func.function_name == "first_value":
                # Handle first_value function - get first value in window
                self._evaluate_first_last_value(
                    result_data, window_func, col_name, is_last=False
                )
            elif window_func.function_name == "last_value":
                # Handle last_value function - get last value in window
                self._evaluate_first_last_value(
                    result_data, window_func, col_name, is_last=True
                )
            elif window_func.function_name in ["rank", "dense_rank"]:
                # Handle rank and dense_rank functions
                self._evaluate_rank_functions(result_data, window_func, col_name)
            elif window_func.function_name in [
                "avg",
                "sum",
                "count",
                "countDistinct",
                "approx_count_distinct",
                "max",
                "min",
            ]:
                # Handle aggregate window functions
                self._evaluate_aggregate_window_functions(
                    result_data, window_func, col_name
                )
            else:
                # For other window functions, assign None for now
                for row in result_data:
                    row[col_name] = None

        return result_data

    def _evaluate_lag_lead(
        self, data: List[Dict[str, Any]], window_func: Any, col_name: str, is_lead: bool
    ) -> None:
        """Evaluate lag or lead window function."""
        if not window_func.column_name:
            # No column specified, set to None
            for row in data:
                row[col_name] = None
            return

        # Get offset and default value
        offset = getattr(window_func, "offset", 1)
        default_value = getattr(window_func, "default_value", None)

        # Handle window specification if present
        if hasattr(window_func, "window_spec") and window_func.window_spec:
            window_spec = window_func.window_spec
            partition_by_cols = getattr(window_spec, "_partition_by", [])
            order_by_cols = getattr(window_spec, "_order_by", [])

            if partition_by_cols:
                # Handle partitioning
                partition_groups: Dict[Any, List[int]] = {}
                for i, row in enumerate(data):
                    partition_key = tuple(
                        row.get(col.name) if hasattr(col, "name") else row.get(str(col))
                        for col in partition_by_cols
                    )
                    if partition_key not in partition_groups:
                        partition_groups[partition_key] = []
                    partition_groups[partition_key].append(i)

                # Process each partition
                for partition_indices in partition_groups.values():
                    # Apply ordering to partition indices
                    ordered_indices = self._apply_ordering_to_indices(
                        data, partition_indices, order_by_cols
                    )
                    self._apply_lag_lead_to_partition(
                        data,
                        ordered_indices,
                        window_func.column_name,
                        col_name,
                        offset,
                        default_value,
                        is_lead,
                    )
            else:
                # No partitioning, apply to entire dataset with ordering
                all_indices = list(range(len(data)))
                ordered_indices = self._apply_ordering_to_indices(
                    data, all_indices, order_by_cols
                )
                self._apply_lag_lead_to_partition(
                    data,
                    ordered_indices,
                    window_func.column_name,
                    col_name,
                    offset,
                    default_value,
                    is_lead,
                )
        else:
            # No window spec, apply to entire dataset
            self._apply_lag_lead_to_partition(
                data,
                list(range(len(data))),
                window_func.column_name,
                col_name,
                offset,
                default_value,
                is_lead,
            )

    def _apply_ordering_to_indices(
        self, data: List[Dict[str, Any]], indices: List[int], order_by_cols: List[Any]
    ) -> List[int]:
        """Apply ordering to a list of indices based on order by columns."""
        if not order_by_cols:
            return indices

        def sort_key(idx: int) -> Tuple[Any, ...]:
            row = data[idx]
            key_values = []
            for col in order_by_cols:
                # Handle ColumnOperation objects (like col("salary").desc())
                operation = None
                nulls_last = True  # Default: nulls last
                is_desc = False

                if hasattr(col, "operation"):
                    operation = col.operation
                    # Check for nulls variant operations
                    if operation == "desc_nulls_last":
                        is_desc = True
                        nulls_last = True
                    elif operation == "desc_nulls_first":
                        is_desc = True
                        nulls_last = False
                    elif operation == "asc_nulls_last":
                        is_desc = False
                        nulls_last = True
                    elif operation == "asc_nulls_first":
                        is_desc = False
                        nulls_last = False
                    elif operation == "desc":
                        is_desc = True
                        nulls_last = True  # Default for desc
                    elif operation == "asc":
                        is_desc = False
                        nulls_last = True  # Default for asc

                if hasattr(col, "column") and hasattr(col.column, "name"):
                    col_name = col.column.name
                elif hasattr(col, "name"):
                    col_name = col.name
                else:
                    col_name = str(col)
                value = row.get(col_name)

                # Handle None values based on nulls_last flag
                if value is None:
                    if nulls_last:
                        # Put nulls at the end: use max value for asc, min value for desc
                        key_values.append(
                            float("inf") if not is_desc else float("-inf")
                        )
                    else:
                        # Put nulls at the start: use min value for asc, max value for desc
                        key_values.append(
                            float("-inf") if not is_desc else float("inf")
                        )
                else:
                    key_values.append(value)
            return tuple(key_values)

        # Check if any column has desc operation
        has_desc = any(
            (
                hasattr(col, "operation")
                and col.operation in ("desc", "desc_nulls_last", "desc_nulls_first")
            )
            for col in order_by_cols
        )

        # Sort indices based on the ordering
        return sorted(indices, key=sort_key, reverse=has_desc)

    def _apply_lag_lead_to_partition(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        source_col: str,
        target_col: str,
        offset: int,
        default_value: Any,
        is_lead: bool,
    ) -> None:
        """Apply lag or lead to a specific partition."""
        if is_lead:
            # Lead: get next row value
            for i, idx in enumerate(indices):
                source_idx = i + offset
                if source_idx < len(indices):
                    actual_idx = indices[source_idx]
                    data[idx][target_col] = data[actual_idx].get(source_col)
                else:
                    data[idx][target_col] = default_value
        else:
            # Lag: get previous row value
            for i, idx in enumerate(indices):
                source_idx = i - offset
                if source_idx >= 0:
                    actual_idx = indices[source_idx]
                    data[idx][target_col] = data[actual_idx].get(source_col)
                else:
                    data[idx][target_col] = default_value

    def _evaluate_first_last_value(
        self, data: List[Dict[str, Any]], window_func: Any, col_name: str, is_last: bool
    ) -> None:
        """Evaluate first_value or last_value window function."""
        if not window_func.column_name:
            # No column specified, set to None
            for row in data:
                row[col_name] = None
            return

        source_col = window_func.column_name

        # Handle window specification if present
        if hasattr(window_func, "window_spec") and window_func.window_spec:
            window_spec = window_func.window_spec
            partition_by_cols = getattr(window_spec, "_partition_by", [])
            order_by_cols = getattr(window_spec, "_order_by", [])

            if partition_by_cols:
                # Handle partitioning
                partition_groups: Dict[Any, List[int]] = {}
                for i, row in enumerate(data):
                    partition_key = tuple(
                        row.get(col.name) if hasattr(col, "name") else row.get(str(col))
                        for col in partition_by_cols
                    )
                    if partition_key not in partition_groups:
                        partition_groups[partition_key] = []
                    partition_groups[partition_key].append(i)

                # Process each partition
                for partition_indices in partition_groups.values():
                    self._apply_first_last_to_partition(
                        data,
                        partition_indices,
                        source_col,
                        col_name,
                        order_by_cols,
                        is_last,
                    )
            else:
                # No partitioning, apply to entire dataset
                self._apply_first_last_to_partition(
                    data,
                    list(range(len(data))),
                    source_col,
                    col_name,
                    order_by_cols,
                    is_last,
                )
        else:
            # No window spec, get first/last value from entire dataset
            self._apply_first_last_to_partition(
                data, list(range(len(data))), source_col, col_name, [], is_last
            )

    def _apply_first_last_to_partition(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        source_col: str,
        target_col: str,
        order_by_cols: List[Any],
        is_last: bool,
    ) -> None:
        """Apply first_value or last_value to a specific partition."""
        if not indices:
            return

        # If there's ordering, sort the partition
        if order_by_cols:
            sorted_indices = self._apply_ordering_to_indices(
                data, indices, order_by_cols
            )
        else:
            sorted_indices = indices

        # Get first or last value based on sorted order
        if is_last:
            # Get last value
            target_idx = sorted_indices[-1]
            value = data[target_idx].get(source_col)
        else:
            # Get first value
            target_idx = sorted_indices[0]
            value = data[target_idx].get(source_col)

        # Assign the value to all rows in the partition
        for idx in indices:
            data[idx][target_col] = value

    def _evaluate_rank_functions(
        self, data: List[Dict[str, Any]], window_func: Any, col_name: str
    ) -> None:
        """Evaluate rank or dense_rank window function."""
        is_dense = window_func.function_name == "dense_rank"

        # Handle window specification if present
        if hasattr(window_func, "window_spec") and window_func.window_spec:
            window_spec = window_func.window_spec
            partition_by_cols = getattr(window_spec, "_partition_by", [])
            order_by_cols = getattr(window_spec, "_order_by", [])

            if partition_by_cols:
                # Handle partitioning
                partition_groups: Dict[Any, List[int]] = {}
                for i, row in enumerate(data):
                    partition_key = tuple(
                        row.get(col.name) if hasattr(col, "name") else row.get(str(col))
                        for col in partition_by_cols
                    )
                    if partition_key not in partition_groups:
                        partition_groups[partition_key] = []
                    partition_groups[partition_key].append(i)

                # Process each partition
                for partition_indices in partition_groups.values():
                    self._apply_rank_to_partition(
                        data, partition_indices, order_by_cols, col_name, is_dense
                    )
            else:
                # No partitioning, apply to entire dataset
                self._apply_rank_to_partition(
                    data, list(range(len(data))), order_by_cols, col_name, is_dense
                )
        else:
            # No window spec, assign ranks based on original order
            for i in range(len(data)):
                data[i][col_name] = i + 1

    def _apply_rank_to_partition(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        order_by_cols: List[Any],
        col_name: str,
        is_dense: bool,
    ) -> None:
        """Apply rank or dense_rank to a specific partition."""
        if not order_by_cols:
            # No order by, assign ranks based on original order
            for i, idx in enumerate(indices):
                data[idx][col_name] = i + 1
            return

        # Sort partition by order by columns using the corrected ordering logic
        sorted_indices = self._apply_ordering_to_indices(data, indices, order_by_cols)

        # Assign ranks in sorted order
        if is_dense:
            # Dense rank: consecutive ranks without gaps
            current_rank = 1
            previous_values = None

            for i, idx in enumerate(sorted_indices):
                row = data[idx]
                current_values = []
                for col in order_by_cols:
                    # Handle ColumnOperation objects (like col("salary").desc())
                    if hasattr(col, "column") and hasattr(col.column, "name"):
                        order_col_name = col.column.name
                    elif hasattr(col, "name"):
                        order_col_name = col.name
                    else:
                        order_col_name = str(col)
                    value = row.get(order_col_name)
                    current_values.append(value)

                if previous_values is not None:  # noqa: SIM102
                    if current_values != previous_values:
                        current_rank += 1

                data[idx][col_name] = current_rank
                previous_values = current_values
        else:
            # Regular rank: ranks with gaps for ties
            current_rank = 1

            for i, idx in enumerate(sorted_indices):
                if i > 0:
                    prev_idx = sorted_indices[i - 1]
                    # Check if current and previous rows have different values
                    row = data[idx]
                    prev_row = data[prev_idx]

                    current_values = []
                    prev_values = []
                    for col in order_by_cols:
                        # Handle ColumnOperation objects (like col("salary").desc())
                        if hasattr(col, "column") and hasattr(col.column, "name"):
                            order_col_name = col.column.name
                        elif hasattr(col, "name"):
                            order_col_name = col.name
                        else:
                            order_col_name = str(col)
                        current_values.append(row.get(order_col_name))
                        prev_values.append(prev_row.get(order_col_name))

                    if current_values != prev_values:
                        current_rank = i + 1
                else:
                    current_rank = 1

                data[idx][col_name] = current_rank

    def _evaluate_aggregate_window_functions(
        self, data: List[Dict[str, Any]], window_func: Any, col_name: str
    ) -> None:
        """Evaluate aggregate window functions like avg, sum, count, etc."""
        # Try to extract column_name from the function if not already set
        if not window_func.column_name and hasattr(window_func, "function"):
            func = window_func.function
            if hasattr(func, "_aggregate_function") and func._aggregate_function:
                # Use the property directly to get column_name
                window_func.column_name = func._aggregate_function.column_name

        # For approx_count_distinct, if column_name is still not set, try to get it from the function
        if (
            not window_func.column_name
            and window_func.function_name == "approx_count_distinct"
            and hasattr(window_func, "function")
        ):
            func = window_func.function
            if hasattr(func, "column") and hasattr(func.column, "name"):
                window_func.column_name = func.column.name
            elif hasattr(func, "_aggregate_function") and func._aggregate_function:
                window_func.column_name = func._aggregate_function.column_name

        if not window_func.column_name and window_func.function_name not in ["count"]:
            # No column specified for functions that need it
            for row in data:
                row[col_name] = None
            return

        # Handle window specification if present
        if hasattr(window_func, "window_spec") and window_func.window_spec:
            window_spec = window_func.window_spec
            partition_by_cols = getattr(window_spec, "_partition_by", [])
            order_by_cols = getattr(window_spec, "_order_by", [])

            if partition_by_cols:
                # Handle partitioning
                partition_groups: Dict[Any, List[int]] = {}
                for i, row in enumerate(data):
                    partition_key = tuple(
                        row.get(col.name) if hasattr(col, "name") else row.get(str(col))
                        for col in partition_by_cols
                    )
                    if partition_key not in partition_groups:
                        partition_groups[partition_key] = []
                    partition_groups[partition_key].append(i)

                # Process each partition
                for partition_indices in partition_groups.values():
                    # Apply ordering to partition indices
                    ordered_indices = self._apply_ordering_to_indices(
                        data, partition_indices, order_by_cols
                    )
                    self._apply_aggregate_to_partition(
                        data, ordered_indices, window_func, col_name
                    )
            else:
                # No partitioning, apply to entire dataset with ordering
                all_indices = list(range(len(data)))
                ordered_indices = self._apply_ordering_to_indices(
                    data, all_indices, order_by_cols
                )
                self._apply_aggregate_to_partition(
                    data, ordered_indices, window_func, col_name
                )
        else:
            # No window spec, apply to entire dataset
            all_indices = list(range(len(data)))
            self._apply_aggregate_to_partition(data, all_indices, window_func, col_name)

    def _apply_aggregate_to_partition(
        self,
        data: List[Dict[str, Any]],
        indices: List[int],
        window_func: Any,
        col_name: str,
    ) -> None:
        """Apply aggregate function to a specific partition."""
        if not indices:
            return

        source_col = window_func.column_name
        # Fallback: try to extract column_name if not set
        if not source_col and hasattr(window_func, "function"):
            func = window_func.function
            if hasattr(func, "_aggregate_function") and func._aggregate_function:
                source_col = func._aggregate_function.column_name
            elif hasattr(func, "column") and hasattr(func.column, "name"):
                source_col = func.column.name

        func_name = window_func.function_name

        # Get window boundaries if specified
        rows_between = (
            getattr(window_func.window_spec, "_rows_between", None)
            if hasattr(window_func, "window_spec") and window_func.window_spec
            else None
        )

        for i, idx in enumerate(indices):
            # Determine the window for this row
            if rows_between:
                start_offset, end_offset = rows_between
                # Handle unboundedFollowing (sys.maxsize)
                if end_offset == sys.maxsize:
                    # rowsBetween(currentRow, unboundedFollowing): from current row to end
                    window_start = i + start_offset
                    window_end = len(indices)
                else:
                    window_start = max(0, i + start_offset)
                    window_end = min(len(indices), i + end_offset + 1)
            else:
                # Default: all rows up to current row
                window_start = 0
                window_end = i + 1

            # Get values in the window
            window_values = []
            for j in range(window_start, window_end):
                if j < len(indices):
                    row_idx = indices[j]
                    if source_col:
                        value = data[row_idx].get(source_col)
                        if value is not None:
                            window_values.append(value)
                    else:
                        # For count(*) - count all rows
                        window_values.append(1)

            # Apply aggregate function
            if func_name == "avg":
                data[idx][col_name] = (
                    sum(window_values) / len(window_values) if window_values else None
                )
            elif func_name == "sum":
                data[idx][col_name] = sum(window_values) if window_values else None
            elif func_name == "count":
                data[idx][col_name] = len(window_values)
            elif func_name == "countDistinct" or func_name == "approx_count_distinct":
                data[idx][col_name] = len(set(window_values)) if window_values else 0
            elif func_name == "max":
                data[idx][col_name] = max(window_values) if window_values else None
            elif func_name == "min":
                data[idx][col_name] = min(window_values) if window_values else None
            else:
                # Unknown function, set to None
                data[idx][col_name] = None
