"""
SQLAlchemy-based query builder for lazy evaluation.

This module converts DataFrame operations to SQLAlchemy statements,
providing database-agnostic query building that works with any SQLAlchemy backend.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union
from sqlalchemy import select, Table, and_, or_, func, literal, cast as sa_cast
from sqlalchemy.sql import Select
from sqlalchemy.types import Integer, Float, String, Boolean, BigInteger, Date, DateTime

from ..functions import Column, ColumnOperation, Literal
from ..spark_types import StructType


class SQLAlchemyQueryBuilder:
    """Builds SQLAlchemy queries from DataFrame operations."""

    def __init__(self, table: Table, schema: Optional[StructType] = None):
        """Initialize with a SQLAlchemy Table object.

        Args:
            table: SQLAlchemy Table to query from
            schema: Optional StructType for type information
        """
        self.table = table
        self.schema = schema
        self.select_stmt: Any = select(
            table
        )  # Can be Select or CompoundSelect after union()
        self._with_columns: Dict[str, Any] = {}
        self._join_tables: List[Tuple[Table, Any, str]] = []

    def add_filter(self, condition: ColumnOperation) -> None:
        """Add a WHERE condition using SQLAlchemy expressions."""
        sql_condition = self._column_to_sqlalchemy(condition)
        self.select_stmt = self.select_stmt.where(sql_condition)

    def add_select(self, columns: Tuple[Any, ...]) -> None:
        """Add SELECT columns using SQLAlchemy column expressions."""
        sql_columns: List[Any] = []
        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    sql_columns.extend(self.table.c)
                else:
                    sql_columns.append(self.table.c[col])
            elif isinstance(col, Column):
                if col.name == "*":
                    sql_columns.extend(self.table.c)
                else:
                    sql_columns.append(self.table.c[col.name])
            else:
                # Handle expressions
                sql_columns.append(self._column_to_sqlalchemy(col))

        if sql_columns:
            self.select_stmt = select(*sql_columns).select_from(self.table)

    def add_with_column(
        self, col_name: str, col: Union[Column, ColumnOperation, Literal]
    ) -> None:
        """Add a computed column using SQLAlchemy expressions."""
        sql_expression = self._column_to_sqlalchemy(col)
        self._with_columns[col_name] = sql_expression

    def add_group_by(self, columns: Tuple[Any, ...]) -> None:
        """Add GROUP BY columns using SQLAlchemy."""
        group_cols = []
        for col in columns:
            if isinstance(col, str):
                group_cols.append(self.table.c[col])
            elif isinstance(col, Column):
                group_cols.append(self.table.c[col.name])

        if group_cols:
            self.select_stmt = self.select_stmt.group_by(*group_cols)

    def add_order_by(self, columns: Tuple[Any, ...]) -> None:
        """Add ORDER BY columns using SQLAlchemy."""
        order_cols = []
        for col in columns:
            if isinstance(col, str):
                order_cols.append(self.table.c[col])
            elif isinstance(col, Column):
                order_cols.append(self.table.c[col.name])
            elif isinstance(col, ColumnOperation):
                # Handle desc() operations
                if hasattr(col, "operation") and col.operation == "desc":
                    order_cols.append(self.table.c[col.column.name].desc())
                else:
                    order_cols.append(self.table.c[col.column.name])

        if order_cols:
            self.select_stmt = self.select_stmt.order_by(*order_cols)

    def add_join(
        self, other_table: Table, on: Union[str, List[str]], how: str = "inner"
    ) -> None:
        """Add a JOIN operation using SQLAlchemy."""
        # Build join condition
        if isinstance(on, str):
            condition = self.table.c[on] == other_table.c[on]
        else:
            conditions = [self.table.c[col] == other_table.c[col] for col in on]
            condition = and_(*conditions)

        # Map join type to SQLAlchemy
        isouter = how.lower() in ("left", "left_outer", "leftouter")
        isfull = how.lower() in ("full", "full_outer", "fullouter")

        self._join_tables.append((other_table, condition, how))
        self.select_stmt = self.select_stmt.select_from(
            self.table.join(other_table, condition, isouter=isouter, full=isfull)
        )

    def add_union(self, other_stmt: Select[Any]) -> None:
        """Add a UNION operation using SQLAlchemy."""
        self.select_stmt = self.select_stmt.union(other_stmt)

    def add_limit(self, n: int) -> None:
        """Add LIMIT clause using SQLAlchemy."""
        self.select_stmt = self.select_stmt.limit(n)

    def _column_to_sqlalchemy(self, col: Any) -> Any:
        """Convert a Sparkless column/expression to SQLAlchemy expression."""
        if isinstance(col, Column):
            return self.table.c[col.name]
        elif isinstance(col, ColumnOperation):
            return self._operation_to_sqlalchemy(col)
        elif isinstance(col, Literal):
            return literal(col.value)
        elif hasattr(col, "function_name") and hasattr(col, "window_spec"):
            # Handle window functions
            return self._window_function_to_sqlalchemy(col)
        else:
            return literal(col)

    def _operation_to_sqlalchemy(self, op: ColumnOperation) -> Any:
        """Convert a ColumnOperation to SQLAlchemy expression."""
        if not hasattr(op, "operation") or not hasattr(op, "column"):
            return literal(str(op))

        left = self._column_to_sqlalchemy(op.column)
        right = (
            self._value_to_sqlalchemy(op.value)
            if hasattr(op, "value")
            else literal(op.value)
        )

        # Map operations to SQLAlchemy operators
        operation_map = {
            ">": lambda left, right: left > right,
            "<": lambda left, right: left < right,
            ">=": lambda left, right: left >= right,
            "<=": lambda left, right: left <= right,
            "==": lambda left, right: left == right,
            "!=": lambda left, right: left != right,
            "+": lambda left, right: left + right,
            "-": lambda left, right: left - right,
            "*": lambda left, right: left * right,
            "/": lambda left, right: left / right,
            "%": lambda left, right: left % right,
            "&": lambda left, right: and_(left, right),
            "|": lambda left, right: or_(left, right),
            "cast": lambda left, right: self._handle_cast(left, right),
        }

        # op.operation is guaranteed to be a string in ColumnOperation
        op_operation: str = op.operation  # type: ignore[assignment]
        operation_func = operation_map.get(op_operation)
        if operation_func:
            # Type checker can't infer lambda return types, but we know they return SQLAlchemy expressions
            result: Any = operation_func(left, right)
            return result

        return literal(str(op))

    def _handle_cast(self, column: Any, target_type: str) -> Any:
        """Convert cast to SQLAlchemy cast."""
        type_map = {
            "int": Integer,
            "integer": Integer,
            "long": BigInteger,
            "bigint": BigInteger,
            "double": Float,
            "float": Float,
            "string": String,
            "varchar": String,
            "boolean": Boolean,
            "bool": Boolean,
            "date": Date,
            "timestamp": DateTime,
        }
        # target_type is always str based on function signature
        sa_type = type_map.get(target_type.lower(), String)()
        return sa_cast(column, sa_type)

    def _value_to_sqlalchemy(self, value: Any) -> Any:
        """Convert a value to SQLAlchemy literal or expression."""
        if isinstance(value, (Column, ColumnOperation, Literal)):
            return self._column_to_sqlalchemy(value)
        else:
            return literal(value)

    def _window_function_to_sqlalchemy(self, window_func: Any) -> Any:
        """Convert a window function to SQLAlchemy expression."""
        # Get the function name (e.g., "rank", "row_number")
        function_name = getattr(window_func, "function_name", "window_function")

        # Get the appropriate SQLAlchemy function
        sql_func = getattr(func, function_name.lower(), func.window_function)()

        # Build the OVER clause
        over_clause = self._window_spec_to_dict(window_func.window_spec)

        return sql_func.over(**over_clause)

    def _window_spec_to_dict(self, window_spec: Any) -> Dict[str, Any]:
        """Convert a window specification to SQLAlchemy over() kwargs."""
        kwargs = {}

        # Handle PARTITION BY
        if hasattr(window_spec, "_partition_by") and window_spec._partition_by:
            partition_cols = []
            for col in window_spec._partition_by:
                if isinstance(col, str):
                    partition_cols.append(self.table.c[col])
                elif isinstance(col, Column):
                    partition_cols.append(self.table.c[col.name])
                else:
                    partition_cols.append(self._column_to_sqlalchemy(col))
            kwargs["partition_by"] = partition_cols

        # Handle ORDER BY
        if hasattr(window_spec, "_order_by") and window_spec._order_by:
            order_cols = []
            for col in window_spec._order_by:
                if isinstance(col, str):
                    order_cols.append(self.table.c[col])
                elif isinstance(col, ColumnOperation):
                    # Handle desc() operations in window specs
                    if hasattr(col, "operation") and col.operation == "desc":
                        order_cols.append(self.table.c[col.column.name].desc())
                    else:
                        order_cols.append(self.table.c[col.column.name])
                elif isinstance(col, Column):
                    order_cols.append(self.table.c[col.name])
                else:
                    order_cols.append(self._column_to_sqlalchemy(col))
            kwargs["order_by"] = order_cols

        # Handle window frames (ROWS BETWEEN, RANGE BETWEEN)
        if hasattr(window_spec, "_rows_between") and window_spec._rows_between:
            start, end = window_spec._rows_between
            # SQLAlchemy handles rows between - convert bounds and create tuple
            start_bound = self._frame_bound_to_sqlalchemy(start)
            end_bound = self._frame_bound_to_sqlalchemy(end)
            # SQLAlchemy accepts tuple[Optional[int], Optional[int]] for rows
            kwargs["rows"] = (start_bound, end_bound)  # type: ignore[assignment]
        elif hasattr(window_spec, "_range_between") and window_spec._range_between:
            start, end = window_spec._range_between
            start_bound = self._frame_bound_to_sqlalchemy(start)
            end_bound = self._frame_bound_to_sqlalchemy(end)
            # SQLAlchemy accepts tuple[Optional[int], Optional[int]] for range_
            kwargs["range_"] = (start_bound, end_bound)  # type: ignore[assignment]

        return kwargs

    def _frame_bound_to_sqlalchemy(self, bound: Any) -> Optional[int]:
        """Convert a window frame bound to SQLAlchemy representation."""
        if hasattr(bound, "__name__"):
            bound_name = bound.__name__.upper()
            if "UNBOUNDED" in bound_name and "PRECEDING" in bound_name:
                return None  # SQLAlchemy uses None for unbounded preceding
            elif "CURRENT" in bound_name:
                return 0  # Current row
            elif hasattr(bound, "value"):
                value = bound.value
                return int(value) if value is not None else None
        # For integer literals or other numeric types, convert to int
        if isinstance(bound, (int, float)):
            return int(bound)
        # Default: return None for unknown types
        return None

    def build_select(self) -> Any:  # Can be Select or CompoundSelect
        """Build the final SQLAlchemy Select statement."""
        # Apply computed columns if any
        if self._with_columns:
            # Get current columns
            if self.select_stmt._raw_columns:
                current_cols = list(self.select_stmt._raw_columns)
            else:
                current_cols = list(self.table.c)

            # Add computed columns
            for col_name, expression in self._with_columns.items():
                current_cols.append(expression.label(col_name))

            # Rebuild select with all columns
            self.select_stmt = select(*current_cols).select_from(self.table)

        return self.select_stmt
