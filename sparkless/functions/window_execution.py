"""
Window functions for Sparkless.

This module contains window function implementations including row_number, rank, etc.
"""

from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Tuple
import contextlib
import sys

if TYPE_CHECKING:
    from sparkless.sql import WindowSpec
    from sparkless.functions.base import ColumnOperation
else:
    from sparkless.functions.base import ColumnOperation


class WindowFunction:
    """Represents a window function.

    This class handles window functions like row_number(), rank(), etc.
    that operate over a window specification.
    """

    def __init__(self, function: Any, window_spec: "WindowSpec"):
        """Initialize WindowFunction.

        Args:
            function: The window function (e.g., row_number(), rank()).
            window_spec: The window specification.
        """
        self.function = function
        self.window_spec = window_spec

        # Handle ColumnOperation wrapping AggregateFunction (PySpark-compatible)
        # When F.sum().over() is called, function is a ColumnOperation with _aggregate_function
        if (
            hasattr(function, "_aggregate_function")
            and function._aggregate_function is not None
        ):
            # Unwrap the AggregateFunction from ColumnOperation
            agg_func = function._aggregate_function
            self.function_name = getattr(agg_func, "function_name", "window_function")
            # Get column_name from the AggregateFunction's column_name property
            self.column_name = agg_func.column_name
        else:
            # Regular function (not wrapping AggregateFunction)
            self.function_name = getattr(function, "function_name", "window_function")
            self.column_name = getattr(function, "column", None)
        # Process column_name to extract string name
        if self.column_name and isinstance(self.column_name, str):
            # Already a string (from AggregateFunction.column_name property)
            pass  # Keep as is
        elif self.column_name and hasattr(self.column_name, "name"):
            self.column_name = self.column_name.name
        elif self.column_name and hasattr(self.column_name, "column"):
            # Handle Column objects
            if hasattr(self.column_name.column, "name"):
                self.column_name = self.column_name.column.name
            elif isinstance(self.column_name.column, str):
                self.column_name = self.column_name.column
            else:
                self.column_name = None
        else:
            self.column_name = None

        # Extract offset and default for lag/lead functions
        self.offset = 1  # Default offset
        self.default = None  # Default default value
        if (
            hasattr(function, "value")
            and function.value is not None
            and isinstance(function.value, tuple)
            and len(function.value) == 2
        ):
            # lag/lead store (offset, default) as tuple
            self.offset = function.value[0]
            self.default = function.value[1]

        self.name = self._generate_name()

        # Add column property for compatibility with query executor
        self.column = getattr(function, "column", None)

    def _generate_name(self) -> str:
        """Generate a name for this window function."""
        return f"{self.function_name}() OVER ({self.window_spec})"

    def alias(self, name: str) -> "WindowFunction":
        """Create an alias for this window function.

        Args:
            name: The alias name.

        Returns:
            Self for method chaining.
        """
        self.name = name
        return self

    def cast(self, data_type: Any) -> ColumnOperation:
        """Cast the window function result to a different data type.

        Args:
            data_type: The target data type (DataType instance or string type name).

        Returns:
            ColumnOperation representing the cast operation.

        Example:
            >>> F.row_number().over(window_spec).cast("long")
        """
        return ColumnOperation(self, "cast", data_type)

    def __mul__(self, other: Any) -> ColumnOperation:
        """Multiply window function result by a value.

        Args:
            other: The value to multiply by.

        Returns:
            ColumnOperation representing the multiplication.

        Example:
            >>> F.percent_rank().over(window) * 100
        """
        return ColumnOperation(self, "*", other, name=f"({self.name} * {other})")

    def __rmul__(self, other: Any) -> ColumnOperation:
        """Reverse multiply (e.g., 100 * window_func).

        Args:
            other: The value to multiply.

        Returns:
            ColumnOperation representing the multiplication.

        Example:
            >>> 100 * F.percent_rank().over(window)
        """
        return ColumnOperation(self, "*", other, name=f"({other} * {self.name})")

    def __add__(self, other: Any) -> ColumnOperation:
        """Add a value to window function result.

        Args:
            other: The value to add.

        Returns:
            ColumnOperation representing the addition.

        Example:
            >>> F.row_number().over(window) + 1
        """
        return ColumnOperation(self, "+", other, name=f"({self.name} + {other})")

    def __radd__(self, other: Any) -> ColumnOperation:
        """Reverse add (e.g., 1 + window_func).

        Args:
            other: The value to add.

        Returns:
            ColumnOperation representing the addition.

        Example:
            >>> 1 + F.row_number().over(window)
        """
        return ColumnOperation(self, "+", other, name=f"({other} + {self.name})")

    def __sub__(self, other: Any) -> ColumnOperation:
        """Subtract a value from window function result.

        Args:
            other: The value to subtract.

        Returns:
            ColumnOperation representing the subtraction.

        Example:
            >>> F.row_number().over(window) - 1
        """
        return ColumnOperation(self, "-", other, name=f"({self.name} - {other})")

    def __rsub__(self, other: Any) -> ColumnOperation:
        """Reverse subtract (e.g., 10 - window_func).

        Args:
            other: The value to subtract from.

        Returns:
            ColumnOperation representing the subtraction.

        Example:
            >>> 10 - F.row_number().over(window)
        """
        # For reverse subtract, we create: Literal(other) - self
        # This ensures the correct operand order in the expression translator
        from .core.literals import Literal

        return ColumnOperation(
            Literal(other), "-", self, name=f"({other} - {self.name})"
        )

    def __truediv__(self, other: Any) -> ColumnOperation:
        """Divide window function result by a value.

        Args:
            other: The value to divide by.

        Returns:
            ColumnOperation representing the division.

        Example:
            >>> F.row_number().over(window) / 10
        """
        return ColumnOperation(self, "/", other, name=f"({self.name} / {other})")

    def __rtruediv__(self, other: Any) -> ColumnOperation:
        """Reverse divide (e.g., 100 / window_func).

        Args:
            other: The value to divide.

        Returns:
            ColumnOperation representing the division.

        Example:
            >>> 100 / F.row_number().over(window)
        """
        # For reverse divide, we create: Literal(other) / self
        # This ensures the correct operand order in the expression translator
        from .core.literals import Literal

        return ColumnOperation(
            Literal(other), "/", self, name=f"({other} / {self.name})"
        )

    def __neg__(self) -> ColumnOperation:
        """Negate window function result.

        Returns:
            ColumnOperation representing the negation.

        Example:
            >>> -F.row_number().over(window)
        """
        # For negation, we create: Literal(0) - self
        # which is equivalent to -self. This ensures the WindowFunction
        # is on the right side where it can be properly handled.
        from .core.literals import Literal

        return ColumnOperation(Literal(0), "-", self, name=f"(-{self.name})")

    def __eq__(self, other: Any) -> ColumnOperation:  # type: ignore[override]
        """Equality comparison.

        Args:
            other: The value to compare with.

        Returns:
            ColumnOperation representing the equality comparison.

        Example:
            >>> F.row_number().over(window) == 1
        """
        return ColumnOperation(self, "==", other, name=f"({self.name} == {other})")

    def __ne__(self, other: Any) -> ColumnOperation:  # type: ignore[override]
        """Inequality comparison.

        Args:
            other: The value to compare with.

        Returns:
            ColumnOperation representing the inequality comparison.

        Example:
            >>> F.row_number().over(window) != 0
        """
        return ColumnOperation(self, "!=", other, name=f"({self.name} != {other})")

    def __lt__(self, other: Any) -> ColumnOperation:
        """Less than comparison.

        Args:
            other: The value to compare with.

        Returns:
            ColumnOperation representing the less than comparison.

        Example:
            >>> F.row_number().over(window) < 5
        """
        return ColumnOperation(self, "<", other, name=f"({self.name} < {other})")

    def __le__(self, other: Any) -> ColumnOperation:
        """Less than or equal comparison.

        Args:
            other: The value to compare with.

        Returns:
            ColumnOperation representing the less than or equal comparison.

        Example:
            >>> F.row_number().over(window) <= 10
        """
        return ColumnOperation(self, "<=", other, name=f"({self.name} <= {other})")

    def __gt__(self, other: Any) -> ColumnOperation:
        """Greater than comparison.

        Args:
            other: The value to compare with.

        Returns:
            ColumnOperation representing the greater than comparison.

        Example:
            >>> F.row_number().over(window) > 0
        """
        return ColumnOperation(self, ">", other, name=f"({self.name} > {other})")

    def __ge__(self, other: Any) -> ColumnOperation:
        """Greater than or equal comparison.

        Args:
            other: The value to compare with.

        Returns:
            ColumnOperation representing the greater than or equal comparison.

        Example:
            >>> F.row_number().over(window) >= 1
        """
        return ColumnOperation(self, ">=", other, name=f"({self.name} >= {other})")

    def isnull(self) -> ColumnOperation:
        """Check if window function result is null.

        Returns:
            ColumnOperation representing the isnull check.

        Example:
            >>> F.lag("value", 1).over(window).isnull()
        """
        return ColumnOperation(self, "isnull", None, name=f"({self.name} IS NULL)")

    def isnotnull(self) -> ColumnOperation:
        """Check if window function result is not null.

        Returns:
            ColumnOperation representing the isnotnull check.

        Example:
            >>> F.lag("value", 1).over(window).isnotnull()
        """
        return ColumnOperation(
            self, "isnotnull", None, name=f"({self.name} IS NOT NULL)"
        )

    def eqNullSafe(self, other: Any) -> ColumnOperation:
        """Null-safe equality comparison (PySpark eqNullSafe).

        Args:
            other: The value to compare with.

        Returns:
            ColumnOperation representing the null-safe equality comparison.

        Example:
            >>> F.row_number().over(window).eqNullSafe(1)
        """
        return ColumnOperation(
            self, "eqNullSafe", other, name=f"({self.name} <=> {other})"
        )

    def evaluate(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate the window function over the data.

        Args:
            data: List of data rows.

        Returns:
            List of window function results.
        """
        if self.function_name == "row_number":
            return self._evaluate_row_number(data)
        elif self.function_name == "rank":
            return self._evaluate_rank(data)
        elif self.function_name == "dense_rank":
            return self._evaluate_dense_rank(data)
        elif self.function_name == "lag":
            return self._evaluate_lag(data)
        elif self.function_name == "lead":
            return self._evaluate_lead(data)
        elif self.function_name == "nth_value":
            return self._evaluate_nth_value(data)
        elif self.function_name == "ntile":
            return self._evaluate_ntile(data)
        elif self.function_name == "cume_dist":
            return self._evaluate_cume_dist(data)
        elif self.function_name == "percent_rank":
            return self._evaluate_percent_rank(data)
        elif self.function_name == "first":
            return self._evaluate_first(data)
        elif self.function_name == "last":
            return self._evaluate_last(data)
        elif self.function_name == "first_value":
            return self._evaluate_first_value(data)
        elif self.function_name == "last_value":
            return self._evaluate_last_value(data)
        elif self.function_name == "sum":
            return self._evaluate_sum(data)
        elif self.function_name == "avg":
            return self._evaluate_avg(data)
        elif (
            self.function_name == "approx_count_distinct"
            or self.function_name == "countDistinct"
        ):
            return self._evaluate_approx_count_distinct(data)
        else:
            return [None] * len(data)

    def _evaluate_row_number(self, data: List[Dict[str, Any]]) -> List[int]:
        """Evaluate row_number() window function with proper partitioning and ordering."""
        if not data:
            return []

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None  # Single partition

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [0] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            # Assign row numbers (1-indexed) within partition
            for rank, idx in enumerate(sorted_indices, start=1):
                results[idx] = rank

        return results

    def _sort_indices_by_columns(
        self, data: List[Dict[str, Any]], indices: List[int], order_by_cols: List[Any]
    ) -> List[int]:
        """Sort indices by order_by columns."""

        def sort_key(idx: int) -> Tuple[Any, ...]:
            row = data[idx]
            key_values = []
            for col in order_by_cols:
                # Extract column name and operation
                operation = None
                nulls_last = True  # Default: nulls last
                is_desc = False

                if hasattr(col, "column") and hasattr(col.column, "name"):
                    col_name = col.column.name
                    operation = getattr(col, "operation", None)
                elif hasattr(col, "operation"):
                    col_name = col.name if hasattr(col, "name") else str(col)
                    operation = col.operation
                elif hasattr(col, "name"):
                    col_name = col.name
                    operation = getattr(col, "operation", None)
                else:
                    col_name = str(col)
                    operation = None

                # Handle nulls variant operations
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

                value = row.get(col_name)
                # Handle None values based on nulls_last flag
                if value is None:
                    if nulls_last:
                        # Put nulls at the end: use max value for asc, min value for desc
                        key_values.append(
                            (float("inf") if not is_desc else float("-inf"),)
                        )
                    else:
                        # Put nulls at the start: use min value for asc, max value for desc
                        key_values.append(
                            (float("-inf") if not is_desc else float("inf"),)
                        )
                else:
                    key_values.append((value,))

            return tuple(key_values)

        # Check if any column has desc operation
        has_desc = any(
            (
                hasattr(col, "operation")
                and col.operation in ("desc", "desc_nulls_last", "desc_nulls_first")
            )
            or (
                hasattr(col, "column")
                and hasattr(col.column, "operation")
                and col.column.operation
                in ("desc", "desc_nulls_last", "desc_nulls_first")
            )
            for col in order_by_cols
        )

        return sorted(indices, key=sort_key, reverse=has_desc)

    def _evaluate_rank(self, data: List[Dict[str, Any]]) -> List[int]:
        """Evaluate rank() window function with proper partitioning and ordering."""
        if not data:
            return []

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [0] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            # Assign ranks with gaps for ties
            current_rank = 1
            for i, idx in enumerate(sorted_indices):
                if i > 0:
                    # Check if current row has different values than previous
                    prev_idx = sorted_indices[i - 1]
                    if self._rows_differ_by_order_cols(
                        data[idx], data[prev_idx], order_by_cols
                    ):
                        current_rank = i + 1
                else:
                    current_rank = 1

                results[idx] = current_rank

        return results

    def _rows_differ_by_order_cols(
        self, row1: Dict[str, Any], row2: Dict[str, Any], order_by_cols: List[Any]
    ) -> bool:
        """Check if two rows differ by order_by columns."""
        if not order_by_cols:
            return False

        for col in order_by_cols:
            if hasattr(col, "column") and hasattr(col.column, "name"):
                col_name = col.column.name
            elif hasattr(col, "name"):
                col_name = col.name
            else:
                col_name = str(col)

            val1 = row1.get(col_name)
            val2 = row2.get(col_name)
            if val1 != val2:
                return True

        return False

    def _evaluate_dense_rank(self, data: List[Dict[str, Any]]) -> List[int]:
        """Evaluate dense_rank() window function with proper partitioning and ordering."""
        if not data:
            return []

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [0] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            # Assign dense ranks without gaps for ties
            current_rank = 1
            previous_values = None

            for idx in sorted_indices:
                if order_by_cols:
                    current_values = tuple(
                        data[idx].get(
                            col.column.name
                            if hasattr(col, "column") and hasattr(col.column, "name")
                            else col.name
                            if hasattr(col, "name")
                            else str(col)
                        )
                        for col in order_by_cols
                    )
                else:
                    current_values = None

                if previous_values is not None:  # noqa: SIM102
                    if current_values != previous_values:
                        current_rank += 1

                results[idx] = current_rank
                previous_values = current_values

        return results

    def _evaluate_lag(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate lag() window function with proper partitioning and ordering."""
        if not data or not self.column_name:
            return [None] * len(data) if data else []

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [None] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            # Apply lag within sorted partition
            for i, idx in enumerate(sorted_indices):
                source_idx = i - self.offset
                if source_idx >= 0:
                    actual_idx = sorted_indices[source_idx]
                    results[idx] = data[actual_idx].get(self.column_name, self.default)
                else:
                    results[idx] = self.default

        return results

    def _evaluate_lead(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate lead() window function with proper partitioning and ordering."""
        if not data or not self.column_name:
            return [None] * len(data) if data else []

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [None] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            # Apply lead within sorted partition
            for i, idx in enumerate(sorted_indices):
                source_idx = i + self.offset
                if source_idx < len(sorted_indices):
                    actual_idx = sorted_indices[source_idx]
                    results[idx] = data[actual_idx].get(self.column_name, self.default)
                else:
                    results[idx] = self.default

        return results

    def _evaluate_nth_value(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate nth_value() window function with proper partitioning and ordering."""
        if not data or not self.column_name:
            return [None] * len(data) if data else []

        # Extract n from function value
        n = getattr(self.function, "value", 1)
        if not isinstance(n, int) or n < 1:
            return [None] * len(data)

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [None] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            # Get nth value (1-indexed, so n-1 in 0-indexed list)
            # PySpark's nth_value returns the nth value only for rows at or after the nth position
            # For rows before the nth position, it returns NULL
            for i, idx in enumerate(sorted_indices):
                if i >= n - 1:  # Current row is at or after nth position
                    nth_idx = sorted_indices[n - 1]
                    results[idx] = data[nth_idx].get(self.column_name)
                else:
                    # Row is before nth position, return NULL
                    results[idx] = None

        return results

    def _evaluate_ntile(self, data: List[Dict[str, Any]]) -> List[int]:
        """Evaluate ntile() window function with proper partitioning and ordering."""
        if not data:
            return []

        # Extract n (number of buckets) from function value
        n = getattr(self.function, "value", 2)
        if not isinstance(n, int) or n < 1:
            return [1] * len(data)

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [1] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            partition_size = len(sorted_indices)
            if partition_size == 0:
                continue

            # Calculate bucket size (may have remainder)
            bucket_size = partition_size / n
            remainder = partition_size % n

            # Assign buckets
            current_bucket = 1
            items_in_current_bucket = 0
            bucket_capacity = int(bucket_size) + (
                1 if current_bucket <= remainder else 0
            )

            for idx in sorted_indices:
                results[idx] = current_bucket
                items_in_current_bucket += 1

                # Move to next bucket if current is full
                if items_in_current_bucket >= bucket_capacity and current_bucket < n:
                    current_bucket += 1
                    items_in_current_bucket = 0
                    bucket_capacity = int(bucket_size) + (
                        1 if current_bucket <= remainder else 0
                    )

        return results

    def _evaluate_cume_dist(self, data: List[Dict[str, Any]]) -> List[float]:
        """Evaluate cume_dist() window function with proper partitioning and ordering."""
        if not data:
            return []

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [0.0] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            n = len(sorted_indices)
            if n == 0:
                continue

            # For cume_dist: number of rows with value <= current row's value / total rows
            # Since rows are already sorted, we can use position-based calculation
            # For rows with the same value, they all get the same cume_dist (the max position)
            for i, idx in enumerate(sorted_indices):
                current_row = data[idx]
                # Find the last row with the same value as current row
                last_same_idx = i
                for j in range(i + 1, len(sorted_indices)):
                    other_idx = sorted_indices[j]
                    other_row = data[other_idx]
                    # If other row has same value as current, update last_same_idx
                    if not self._rows_differ_by_order_cols(
                        current_row, other_row, order_by_cols
                    ):
                        last_same_idx = j
                    else:
                        break  # Rows are sorted, so no more matches

                # cume_dist = (position of last row with same value + 1) / total
                # Position is 0-indexed, so we add 1 to get 1-indexed position
                results[idx] = (last_same_idx + 1) / n

        return results

    def _row_less_or_equal(
        self, row1: Dict[str, Any], row2: Dict[str, Any], order_by_cols: List[Any]
    ) -> bool:
        """Check if row1 <= row2 by order_by columns."""
        if not order_by_cols:
            return True  # All rows are equal if no ordering

        for col in order_by_cols:
            if hasattr(col, "column") and hasattr(col.column, "name"):
                col_name = col.column.name
                is_desc = getattr(col, "operation", None) == "desc"
            elif hasattr(col, "name"):
                col_name = col.name
                is_desc = getattr(col, "operation", None) == "desc"
            else:
                col_name = str(col)
                is_desc = False

            val1 = row1.get(col_name)
            val2 = row2.get(col_name)

            # Handle None values
            if val1 is None and val2 is None:
                continue
            if val1 is None:
                return not is_desc  # None is last for ascending, first for descending
            if val2 is None:
                return is_desc

            # Compare values
            if is_desc:
                if val1 > val2:
                    return False
                elif val1 < val2:
                    return True
            else:
                if val1 < val2:
                    return True
                elif val1 > val2:
                    return False

        return True  # All values equal

    def _evaluate_percent_rank(self, data: List[Dict[str, Any]]) -> List[float]:
        """Evaluate percent_rank() window function with proper partitioning and ordering."""
        if not data:
            return []

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [0.0] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            n = len(sorted_indices)
            if n == 1:
                # Single row partition: percent_rank = 0.0
                results[sorted_indices[0]] = 0.0
            else:
                # Calculate ranks first (for percent_rank formula)
                ranks = [0] * n
                current_rank = 1
                for i, idx in enumerate(sorted_indices):
                    if i > 0:
                        prev_idx = sorted_indices[i - 1]
                        if self._rows_differ_by_order_cols(
                            data[idx], data[prev_idx], order_by_cols
                        ):
                            current_rank = i + 1
                    else:
                        current_rank = 1
                    ranks[i] = current_rank

                # Calculate percent_rank: (rank - 1) / (n - 1)
                # n > 1 is guaranteed here due to the if n == 1 check above
                for i, idx in enumerate(sorted_indices):
                    results[idx] = (ranks[i] - 1) / (n - 1)

        return results

    def _evaluate_first(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate first() window function with proper partitioning and ordering."""
        # first() behaves the same as first_value() for window functions
        return self._evaluate_first_value(data)

    def _evaluate_last(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate last() window function with proper partitioning and ordering.

        Note: With orderBy, PySpark's default frame is UNBOUNDED PRECEDING AND CURRENT ROW,
        so last() returns the current row's value (last in the frame up to current row),
        not the last value in the entire partition.
        """
        if not data or not self.column_name:
            return [None] * len(data) if data else []

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # If orderBy is specified, last() returns the current row's value
        # (because default frame is UNBOUNDED PRECEDING AND CURRENT ROW)
        if order_by_cols:
            # Create partition groups
            partition_groups: Dict[Any, List[int]] = {}
            for i, row in enumerate(data):
                if partition_by_cols:
                    partition_key = tuple(
                        row.get(col.name if hasattr(col, "name") else str(col))
                        for col in partition_by_cols
                    )
                else:
                    partition_key = None

                if partition_key not in partition_groups:
                    partition_groups[partition_key] = []
                partition_groups[partition_key].append(i)

            # Initialize results
            results = [None] * len(data)

            # Process each partition
            for partition_indices in partition_groups.values():
                # Sort indices by order_by columns
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )

                # For each row in sorted order, last() returns that row's value
                # (because frame up to current row ends at current row)
                for sorted_pos, idx in enumerate(sorted_indices):
                    results[idx] = data[idx].get(self.column_name)

            return results
        else:
            # Without orderBy, last() behaves like last_value() - returns last value in partition
            return self._evaluate_last_value(data)

    def _evaluate_sum(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate sum() window function with proper partitioning."""
        if not data:
            return []

        # Get the column name from the function
        col_name = self.column_name
        if not col_name:
            return [None] * len(data)

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None  # Single partition = all rows

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results: List[Any] = [None] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            # Check for window frame (rowsBetween)
            rows_between = getattr(self.window_spec, "_rows_between", None)
            # Window.unboundedPreceding is -sys.maxsize - 1, Window.currentRow is 0
            unbounded_preceding = -sys.maxsize - 1
            current_row = 0
            if (
                rows_between
                and rows_between[0] == unbounded_preceding
                and rows_between[1] == current_row
            ):
                # Cumulative sum (running total) - sum from start to current row
                cumulative_sum = 0.0
                # Map sorted indices back to original indices
                # sorted_indices contains the indices in sorted order
                # partition_indices contains the original indices
                # We need to assign cumulative sum to rows in sorted order, but store in original positions
                for sorted_pos, sorted_idx in enumerate(sorted_indices):
                    row = data[sorted_idx]
                    if col_name in row and row[col_name] is not None:
                        with contextlib.suppress(ValueError, TypeError):
                            cumulative_sum += float(row[col_name])
                    # Find the original index position for this sorted index
                    # sorted_indices[sorted_pos] = sorted_idx, and we need to find where sorted_idx is in partition_indices
                    original_idx = (
                        sorted_idx  # sorted_idx is already the original index from data
                    )
                    results[original_idx] = cumulative_sum
            else:
                # Calculate sum for entire partition (default behavior)
                partition_sum = 0.0
                for idx in sorted_indices:
                    row = data[idx]
                    if col_name in row and row[col_name] is not None:
                        with contextlib.suppress(ValueError, TypeError):
                            partition_sum += float(row[col_name])

                # Assign same sum to all rows in partition
                for idx in partition_indices:
                    results[idx] = partition_sum

        return results

    def _evaluate_avg(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate avg() window function."""
        if not data:
            return []

        # Get the column name from the function
        col_name = self.column_name
        if not col_name:
            return [None] * len(data)

        # Calculate average for each position
        result: List[Optional[float]] = []
        running_sum = 0.0
        count = 0

        for row in data:
            if col_name in row and row[col_name] is not None:
                try:
                    running_sum += float(row[col_name])
                    count += 1
                except (ValueError, TypeError):
                    pass

            if count > 0:
                result.append(running_sum / count)
            else:
                result.append(None)

        return result

    def _evaluate_approx_count_distinct(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate approx_count_distinct() window function with proper partitioning."""
        if not data:
            return []

        # Get the column name from the function
        col_name = self.column_name
        if not col_name:
            return [None] * len(data)

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None  # Single partition = all rows

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results: List[Any] = [None] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            # For window functions without orderBy, PySpark returns the same value
            # for all rows in the partition (total distinct count in partition)
            # For window functions with orderBy, it's a running distinct count
            if order_by_cols:
                # With orderBy: running distinct count (UNBOUNDED PRECEDING to CURRENT ROW)
                seen_values: Set[Any] = set()
                for i, idx in enumerate(sorted_indices):
                    value = data[idx].get(col_name)
                    if value is not None:
                        seen_values.add(value)
                    results[idx] = len(seen_values)
            else:
                # Without orderBy: same distinct count for all rows in partition
                # Calculate distinct count for entire partition
                partition_values = [
                    data[idx].get(col_name)
                    for idx in partition_indices
                    if data[idx].get(col_name) is not None
                ]
                distinct_count = len(set(partition_values))
                # Assign same value to all rows in partition
                for idx in partition_indices:
                    results[idx] = distinct_count

        return results

    def _evaluate_first_value(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate first_value() window function with proper partitioning and ordering."""
        if not data or not self.column_name:
            return [None] * len(data) if data else []

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [None] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            # Get first value in sorted partition
            if sorted_indices:
                first_idx = sorted_indices[0]
                first_value = data[first_idx].get(self.column_name)

                # Assign first value to all rows in partition
                for idx in partition_indices:
                    results[idx] = first_value

        return results

    def _evaluate_last_value(self, data: List[Dict[str, Any]]) -> List[Any]:
        """Evaluate last_value() window function with proper partitioning and ordering."""
        if not data or not self.column_name:
            return [None] * len(data) if data else []

        # Get partition and order columns from window spec
        partition_by_cols = getattr(self.window_spec, "_partition_by", [])
        order_by_cols = getattr(self.window_spec, "_order_by", [])

        # Create partition groups
        partition_groups: Dict[Any, List[int]] = {}
        for i, row in enumerate(data):
            if partition_by_cols:
                partition_key = tuple(
                    row.get(col.name if hasattr(col, "name") else str(col))
                    for col in partition_by_cols
                )
            else:
                partition_key = None

            if partition_key not in partition_groups:
                partition_groups[partition_key] = []
            partition_groups[partition_key].append(i)

        # Initialize results
        results = [None] * len(data)

        # Process each partition
        for partition_indices in partition_groups.values():
            # Sort indices by order_by columns if specified
            if order_by_cols:
                sorted_indices = self._sort_indices_by_columns(
                    data, partition_indices, order_by_cols
                )
            else:
                sorted_indices = partition_indices

            # Get last value in sorted partition
            if sorted_indices:
                last_idx = sorted_indices[-1]
                last_value = data[last_idx].get(self.column_name)

                # Assign last value to all rows in partition
                for idx in partition_indices:
                    results[idx] = last_value

        return results
