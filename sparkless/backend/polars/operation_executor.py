from __future__ import annotations

# DataFrame operation executor for Polars.
# Provides execution of DataFrame operations (filter, select, join, etc.)
# using the Polars DataFrame API.

import json
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Tuple, Union, cast
import polars as pl
from .window_handler import PolarsWindowHandler
from sparkless import config
from sparkless.functions import Column, ColumnOperation
from sparkless.functions.window_execution import WindowFunction
from sparkless.spark_types import StructType
from sparkless.core.ddl_adapter import parse_ddl_schema
from sparkless.utils.profiling import profiled

if TYPE_CHECKING:
    from .expression_translator import PolarsExpressionTranslator
    from sparkless.dataframe.evaluation.expression_evaluator import (
        ExpressionEvaluator,
    )


class PolarsOperationExecutor:
    """Executes DataFrame operations using Polars DataFrame API."""

    def __init__(self, expression_translator: PolarsExpressionTranslator):
        """Initialize operation executor.

        Args:
            expression_translator: Polars expression translator instance
        """
        self.translator = expression_translator
        self.window_handler = PolarsWindowHandler()
        self._shortcuts_enabled = config.is_feature_enabled(
            "enable_polars_vectorized_shortcuts"
        )
        self._struct_field_cache: Dict[Tuple[str, str], List[str]] = {}

    def _extract_window_function_with_arithmetic(
        self, col: Any
    ) -> Tuple[Optional[WindowFunction], List[Tuple[str, Any, bool]]]:
        """Recursively extract WindowFunction and all arithmetic operations applied to it.

        Args:
            col: Column, WindowFunction, or ColumnOperation

        Returns:
            Tuple of (WindowFunction or None, list of (operation, value, is_reverse) tuples)
            Operations are in order from innermost to outermost.
        """
        from sparkless.functions.core.literals import Literal

        if isinstance(col, WindowFunction):
            return (col, [])
        elif isinstance(col, ColumnOperation):
            # Skip alias-only wrappers. .alias() sets _alias_name on the same op;
            # it does not create operation="alias". Callers use getattr(col, "_alias_name", None).
            if col.operation == "alias":
                return self._extract_window_function_with_arithmetic(col.column)
            # Check if this is a reverse operation (Literal - WindowFunction or Literal / WindowFunction)
            if isinstance(col.column, Literal) and isinstance(
                col.value, WindowFunction
            ):
                # Reverse operation: Literal op WindowFunction
                window_func, inner_ops = self._extract_window_function_with_arithmetic(
                    col.value
                )
                if window_func:
                    # Add reverse operation at the end
                    inner_ops.append(
                        (cast("str", col.operation), col.column.value, True)
                    )
                    return (window_func, inner_ops)
            # Check if column is WindowFunction or contains one
            elif isinstance(col.column, WindowFunction):
                # Direct: WindowFunction op value
                window_func, inner_ops = self._extract_window_function_with_arithmetic(
                    col.column
                )
                if window_func:
                    inner_ops.append((cast("str", col.operation), col.value, False))
                    return (window_func, inner_ops)
            # Recursively check nested ColumnOperation
            elif isinstance(col.column, ColumnOperation):
                window_func, inner_ops = self._extract_window_function_with_arithmetic(
                    col.column
                )
                if window_func:
                    # Add this operation to the list (operations are in order from innermost to outermost)
                    inner_ops.append((cast("str", col.operation), col.value, False))
                    return (window_func, inner_ops)
            # Also check if value is a ColumnOperation (for cases like (A * B) where both are operations)
            elif isinstance(col.value, ColumnOperation):
                # This handles cases where both operands are ColumnOperations
                # For now, we only handle WindowFunction on the left side
                pass
            # Handle Column op WindowFunction (e.g., F.col("value") - F.lag("value", 1).over(window))
            elif isinstance(col.value, WindowFunction):
                # Left-side operation: Column op WindowFunction
                window_func, inner_ops = self._extract_window_function_with_arithmetic(
                    col.value
                )
                if window_func:
                    # Record operation with is_reverse=False (it's left op window, not reverse)
                    # For subtraction, we'll need special handling in arithmetic application
                    inner_ops.append((cast("str", col.operation), col.column, False))
                    return (window_func, inner_ops)
        return (None, [])

    def _arith_operand_to_polars(self, val: Any, df: pl.DataFrame) -> Any:
        """Convert an arithmetic operand to a form Polars accepts (scalar or pl.Expr).

        Scalars are returned as-is (Polars accepts them in binary ops). Column/ColumnOperation
        are translated to pl.Expr for e.g. window_expr - pl.col("x").
        """
        if isinstance(val, (int, float, bool, str, type(None))):
            return val
        if isinstance(val, (Column, ColumnOperation)):
            case_sensitive = self._get_case_sensitive()
            return self.translator.translate(
                val,
                available_columns=list(df.columns),
                case_sensitive=case_sensitive,
            )
        return val

    def _get_case_sensitive(self) -> bool:
        """Get case sensitivity setting from active session.

        Returns:
            True if case-sensitive mode is enabled, False otherwise.
            Defaults to False (case-insensitive) to match PySpark behavior.
        """
        try:
            from sparkless.session.core.session import SparkSession

            active_sessions = getattr(SparkSession, "_active_sessions", [])
            if active_sessions:
                session = active_sessions[-1]
                if hasattr(session, "conf"):
                    return bool(session.conf.is_case_sensitive())
        except Exception:
            pass
        return False  # Default to case-insensitive (matching PySpark)

    def _find_column(
        self, df: pl.DataFrame, column_name: str, case_sensitive: bool = False
    ) -> Optional[str]:
        """Find column name in Polars DataFrame using ColumnResolver.

        Args:
            df: Polars DataFrame to search in.
            column_name: Column name to find.
            case_sensitive: Whether to use case-sensitive matching.

        Returns:
            Actual column name if found, None otherwise.
        """
        from sparkless.core.column_resolver import ColumnResolver

        available_columns = list(df.columns)
        result = ColumnResolver.resolve_column_name(
            column_name, available_columns, case_sensitive
        )
        return str(result) if result else None

    def _resolve_window_col_name(
        self, df: pl.DataFrame, col_name: str, case_sensitive: bool
    ) -> str:
        """Resolve window partition/order column name against DataFrame schema."""
        resolved = self._find_column(df, col_name, case_sensitive)
        return resolved if resolved is not None else col_name

    @profiled("polars.apply_filter", category="polars")
    def apply_filter(self, df: pl.DataFrame, condition: Any) -> pl.DataFrame:
        """Apply a filter operation.

        Args:
            df: Source Polars DataFrame
            condition: Filter condition (ColumnOperation or expression)

        Returns:
            Filtered Polars DataFrame
        """
        # Check if condition is a WindowFunction comparison or isnull/isnotnull
        is_window_function_comparison = (
            isinstance(condition, ColumnOperation)
            and condition.operation in [">", "<", ">=", "<=", "==", "!=", "eqNullSafe"]
            and isinstance(condition.column, WindowFunction)
        )
        is_window_function_isnull = (
            isinstance(condition, ColumnOperation)
            and condition.operation in ["isnull", "isnotnull"]
            and isinstance(condition.column, WindowFunction)
        )

        if is_window_function_comparison or is_window_function_isnull:
            # Handle comparison operations or isnull/isnotnull operations with WindowFunction
            # First, apply the window function to get a temporary column
            window_func = condition.column
            operation = condition.operation
            operation_value = condition.value

            # Apply window function to get a temporary column
            temp_col_name = "__window_func_temp_filter"
            df_with_window = self.apply_with_column(
                df, temp_col_name, window_func, expected_field=None
            )

            # Now create the operation expression: temp_col_name op value (or isnull/isnotnull)
            # Note: Column is already imported at the top of the file

            temp_col = Column(temp_col_name)
            if operation == ">":
                operation_expr = temp_col > operation_value
            elif operation == "<":
                operation_expr = temp_col < operation_value
            elif operation == ">=":
                operation_expr = temp_col >= operation_value
            elif operation == "<=":
                operation_expr = temp_col <= operation_value
            elif operation == "==":
                operation_expr = temp_col == operation_value
            elif operation == "!=":
                operation_expr = temp_col != operation_value
            elif operation == "eqNullSafe":
                operation_expr = temp_col.eqNullSafe(operation_value)
            elif operation == "isnull":
                operation_expr = temp_col.isnull()
            elif operation == "isnotnull":
                operation_expr = temp_col.isnotnull()
            else:
                operation_expr = ColumnOperation(temp_col, operation, operation_value)

            # Translate and apply the filter
            case_sensitive = self._get_case_sensitive()
            filter_expr = self.translator.translate(
                operation_expr,
                available_columns=list(df_with_window.columns),
                case_sensitive=case_sensitive,
            )

            # Apply filter and drop temporary column
            result_df = df_with_window.filter(filter_expr).drop(temp_col_name)
            return result_df

        # Extract column dtype if condition is a ColumnOperation with isin
        # This enables type coercion for mixed types
        input_col_dtype = None
        if (
            isinstance(condition, ColumnOperation)
            and condition.operation == "isin"
            and hasattr(condition, "column")
            and hasattr(condition.column, "name")
        ):
            # Get the column name from the condition
            col_name = condition.column.name
            # Find column using ColumnResolver
            case_sensitive = self._get_case_sensitive()
            actual_col_name = self._find_column(df, col_name, case_sensitive)
            if actual_col_name and actual_col_name in df.columns:
                # Get the column's dtype
                input_col_dtype = df[actual_col_name].dtype

        case_sensitive = self._get_case_sensitive()
        try:
            # Check if condition contains struct field paths that need special handling
            # (e.g., F.col("StructVal")["E1"] creates Column("StructVal.E1"))
            needs_struct_handling = False
            struct_field_col = None
            if isinstance(condition, ColumnOperation):
                # Check if the column part is a Column with struct field path
                if (
                    isinstance(condition.column, Column)
                    and "." in condition.column.name
                ):
                    needs_struct_handling = True
                    struct_field_col = condition.column
            elif isinstance(condition, Column) and "." in condition.name:
                needs_struct_handling = True
                struct_field_col = condition

            if needs_struct_handling and struct_field_col:
                # Handle struct field access in filter by creating temporary column
                # Extract the struct field path and apply it as a withColumn first
                temp_col_name = f"__struct_field_temp_{id(condition)}"
                df_with_temp = self.apply_with_column(
                    df, temp_col_name, struct_field_col, expected_field=None
                )
                # Now create the filter condition using the temp column
                if isinstance(condition, ColumnOperation):
                    # Recreate the operation with the temp column
                    # Note: Column is already imported at the top of the file
                    from sparkless.functions.core.column import Column as Col

                    temp_col = Col(temp_col_name)
                    if condition.operation == ">":
                        filter_condition = temp_col > condition.value
                    elif condition.operation == "<":
                        filter_condition = temp_col < condition.value
                    elif condition.operation == ">=":
                        filter_condition = temp_col >= condition.value
                    elif condition.operation == "<=":
                        filter_condition = temp_col <= condition.value
                    elif condition.operation == "==":
                        filter_condition = temp_col == condition.value
                    elif condition.operation == "!=":
                        filter_condition = temp_col != condition.value
                    else:
                        # For other operations, use the temp column directly
                        filter_condition = temp_col
                else:
                    # Simple Column - just use the temp column
                    # Note: Column is already imported at the top of the file
                    from sparkless.functions.core.column import Column as Col

                    filter_condition = Col(temp_col_name)

                filter_expr = self.translator.translate(
                    filter_condition,
                    available_columns=list(df_with_temp.columns),
                    case_sensitive=case_sensitive,
                )
                result_df = df_with_temp.filter(filter_expr).drop(temp_col_name)
                return result_df

            filter_expr = self.translator.translate(
                condition,
                input_col_dtype=input_col_dtype,
                available_columns=list(df.columns),
                case_sensitive=case_sensitive,
            )
        except ValueError as e:
            # Check if this is a WindowFunction comparison that should be handled
            error_msg = str(e)
            if "WindowFunction comparison" in error_msg and isinstance(
                condition, ColumnOperation
            ):
                # Recursively handle it (should have been caught above, but handle as fallback)
                return self.apply_filter(df, condition)
            raise
        return df.filter(filter_expr)

    @profiled("polars.apply_select", category="polars")
    def apply_select(self, df: pl.DataFrame, columns: Tuple[Any, ...]) -> pl.DataFrame:
        """Apply a select operation.

        Args:
            df: Source Polars DataFrame
            columns: Columns to select

        Returns:
            Selected Polars DataFrame
        """
        select_exprs = []
        select_names = []
        map_op_indices = set()  # Track which columns are map operations
        python_columns: List[Tuple[str, List[Any]]] = []
        rows_cache: Optional[List[Dict[str, Any]]] = None
        evaluator: Union[ExpressionEvaluator, None] = None

        # First pass: handle map_keys, map_values, map_entries using struct operations
        for i, col in enumerate(columns):
            # Check if this is a map_keys, map_values, or map_entries operation
            is_map_op = False
            map_op_name = None
            map_col_name = None
            if hasattr(col, "operation"):
                if col.operation in [
                    "map_keys",
                    "map_values",
                    "map_entries",
                    "map_concat",
                ]:
                    is_map_op = True
                    map_op_name = col.operation
                    if hasattr(col, "column") and hasattr(col.column, "name"):
                        map_col_name = col.column.name
            elif hasattr(col, "function_name") and col.function_name in [
                "map_keys",
                "map_values",
                "map_entries",
                "map_concat",
            ]:
                is_map_op = True
                map_op_name = col.function_name
                if hasattr(col, "column") and hasattr(col.column, "name"):
                    map_col_name = col.column.name

            if is_map_op and map_col_name and map_col_name in df.columns:
                # Get the struct dtype for this column
                struct_dtype = df[map_col_name].dtype
                if hasattr(struct_dtype, "fields") and struct_dtype.fields:
                    # Build expression using struct.field() checks
                    field_names = self._get_struct_field_names(
                        map_col_name, struct_dtype
                    )
                    alias_name = (
                        getattr(col, "name", None) or f"{map_op_name}({map_col_name})"
                    )
                    if map_op_name == "map_keys":
                        # Get only non-null field names
                        keys_expr = pl.concat_list(
                            [
                                pl.when(
                                    pl.col(map_col_name)
                                    .struct.field(fname)
                                    .is_not_null()
                                )
                                .then(pl.lit(fname))
                                .otherwise(None)
                                for fname in field_names
                            ]
                        ).list.drop_nulls()
                        select_exprs.append(keys_expr.alias(alias_name))
                        select_names.append(alias_name)
                        map_op_indices.add(i)
                    elif map_op_name == "map_values":
                        # Get only non-null field values
                        values_expr = pl.concat_list(
                            [
                                pl.when(
                                    pl.col(map_col_name)
                                    .struct.field(fname)
                                    .is_not_null()
                                )
                                .then(pl.col(map_col_name).struct.field(fname))
                                .otherwise(None)
                                for fname in field_names
                            ]
                        ).list.drop_nulls()
                        select_exprs.append(values_expr.alias(alias_name))
                        select_names.append(alias_name)
                        map_op_indices.add(i)
                    elif map_op_name == "map_entries":
                        # Create array of structs with key and value
                        entries_list = pl.concat_list(
                            [
                                pl.struct(
                                    [
                                        pl.lit(fname).cast(pl.Utf8).alias("key"),
                                        pl.col(map_col_name)
                                        .struct.field(fname)
                                        .alias("value"),
                                    ]
                                )
                                for fname in field_names
                            ]
                        ).list.filter(pl.element().struct.field("value").is_not_null())
                        select_exprs.append(entries_list.alias(alias_name))
                        select_names.append(alias_name)
                        map_op_indices.add(i)
                    elif map_op_name == "map_concat":
                        # map_concat(*cols) - merge multiple maps
                        # col.value contains additional columns (first column is in col.column)
                        if (
                            hasattr(col, "value")
                            and col.value
                            and isinstance(col.value, (list, tuple))
                        ):
                            # Get all map column names
                            map_cols = [map_col_name]  # Start with first column
                            for other_col in col.value:
                                if isinstance(other_col, str):
                                    map_cols.append(other_col)
                                elif hasattr(other_col, "name"):
                                    map_cols.append(other_col.name)
                                elif hasattr(other_col, "column") and hasattr(
                                    other_col.column, "name"
                                ):
                                    # Handle nested column references
                                    map_cols.append(other_col.column.name)
                                else:
                                    # Try to get name from string representation or other attributes
                                    col_str = str(other_col)
                                    if col_str in df.columns:
                                        map_cols.append(col_str)

                            # Verify all map columns exist in DataFrame
                            available_map_cols = [
                                mc for mc in map_cols if mc in df.columns
                            ]
                            if len(available_map_cols) < len(map_cols):
                                # Some columns missing - this shouldn't happen but handle gracefully
                                map_cols = available_map_cols

                            # Merge all struct columns - combine all fields from all maps
                            # Get all field names from all struct columns
                            all_field_names: Set[str] = set()
                            for map_col in map_cols:
                                if map_col in df.columns:
                                    struct_dtype = df[map_col].dtype
                                    if hasattr(struct_dtype, "fields"):
                                        field_names = self._get_struct_field_names(
                                            map_col, struct_dtype
                                        )
                                        all_field_names.update(field_names)
                            sorted_field_names = sorted(all_field_names)

                            # Build merged struct: for each field, take value from later maps first (they override)
                            # Later maps override earlier ones (PySpark behavior)
                            # Then filter out null fields per row (PySpark only includes non-null keys)
                            struct_field_exprs = []
                            for fname in sorted_field_names:
                                # Check each map column in reverse order (later maps override earlier)
                                value_exprs = []
                                for map_col in reversed(map_cols):
                                    if map_col in df.columns:
                                        struct_dtype = df[map_col].dtype
                                        if hasattr(struct_dtype, "fields") and any(
                                            f.name == fname for f in struct_dtype.fields
                                        ):
                                            value_exprs.append(
                                                pl.col(map_col).struct.field(fname)
                                            )

                                if value_exprs:
                                    # Use coalesce to take first non-null value (later maps first)
                                    if len(value_exprs) == 1:
                                        struct_field_exprs.append(
                                            value_exprs[0].alias(fname)
                                        )
                                    else:
                                        struct_field_exprs.append(
                                            pl.coalesce(value_exprs).alias(fname)
                                        )

                            # Create merged struct with all fields
                            merged_struct = pl.struct(struct_field_exprs)

                            # Filter out null fields per row using map_elements
                            # PySpark only includes keys that have non-null values
                            filtered_merged = merged_struct.map_elements(
                                lambda x: {k: v for k, v in x.items() if v is not None}
                                if isinstance(x, dict)
                                else (
                                    {
                                        k: getattr(x, k)
                                        for k in dir(x)
                                        if not k.startswith("_")
                                        and getattr(x, k, None) is not None
                                    }
                                    if hasattr(x, "__dict__")
                                    else None
                                )
                                if x is not None
                                else None,
                                return_dtype=pl.Object,
                            )

                            select_exprs.append(filtered_merged.alias(alias_name))
                            select_names.append(alias_name)
                            map_op_indices.add(i)

        # Second pass: handle all other columns (skip map operations already handled)
        for i, col in enumerate(columns):
            if i in map_op_indices:
                continue  # Skip map operations already handled
            if isinstance(col, str):
                if col == "*":
                    # Select all columns - return original DataFrame
                    return df
                elif "." in col:
                    # Handle nested struct field access (e.g., "Person.name")
                    # Split into struct column and field name
                    parts = col.split(".", 1)
                    struct_col = parts[0]
                    field_name = parts[1]
                    if struct_col in df.columns:
                        # Get struct dtype
                        struct_dtype = df[struct_col].dtype
                        if hasattr(struct_dtype, "fields") and struct_dtype.fields:
                            # Resolve field name case-insensitively within struct
                            from ...core.column_resolver import ColumnResolver

                            case_sensitive = self._get_case_sensitive()
                            field_names = [f.name for f in struct_dtype.fields]
                            resolved_field = ColumnResolver.resolve_column_name(
                                field_name, field_names, case_sensitive
                            )
                            if resolved_field:
                                # Use Polars struct.field() syntax for nested access
                                select_exprs.append(
                                    pl.col(struct_col)
                                    .struct.field(resolved_field)
                                    .alias(col)
                                )
                                select_names.append(col)
                                continue
                    # If nested access failed, fall through to error handling
                    raise ValueError(
                        f"Cannot access nested field '{col}' - struct column '{struct_col}' or field '{field_name}' not found"
                    )
                else:
                    # Resolve column name to find actual column (case-insensitive matching)
                    # PySpark behavior:
                    # - If there's only one match: use the original column name
                    # - If there are multiple matches (different cases): use the requested column name
                    from ...core.column_resolver import ColumnResolver

                    case_sensitive = self._get_case_sensitive()
                    resolved_col_name = ColumnResolver.resolve_column_name(
                        col, list(df.columns), case_sensitive
                    )
                    if resolved_col_name is None:
                        raise ValueError(f"Column '{col}' not found in DataFrame")

                    # Check if there are multiple matches (different cases)
                    column_name_lower = col.lower()
                    matches = [c for c in df.columns if c.lower() == column_name_lower]
                    has_multiple_matches = len(matches) > 1

                    # Use resolved column name for lookup
                    # If multiple matches exist, alias with requested name (PySpark behavior for issue #297)
                    # If single match, use original column name (PySpark default behavior)
                    if has_multiple_matches:
                        # Multiple matches: use requested name as output (issue #297)
                        select_exprs.append(pl.col(resolved_col_name).alias(col))
                        select_names.append(col)
                    elif resolved_col_name == col:
                        # Exact match: no alias needed
                        select_exprs.append(pl.col(col))
                        select_names.append(col)
                    else:
                        # Case-insensitive match but single match: use original column name
                        select_exprs.append(pl.col(resolved_col_name))
                        select_names.append(resolved_col_name)
            elif isinstance(col, ColumnOperation) and col.operation == "json_tuple":
                # json_tuple(col, *fields) expands to multiple output columns (c0, c1, ...)
                fields = list(col.value) if isinstance(col.value, (list, tuple)) else []

                case_sensitive = self._get_case_sensitive()
                json_expr = self.translator.translate(
                    col.column,
                    available_columns=list(df.columns),
                    case_sensitive=case_sensitive,
                )

                import json as _json

                def _extract_field(val: Any, field: str) -> Any:
                    if val is None:
                        return None
                    if not isinstance(val, str):
                        val = str(val)
                    try:
                        obj = _json.loads(val)
                    except Exception:
                        return None
                    if not isinstance(obj, dict):
                        return None
                    v = obj.get(field)
                    return None if v is None else str(v)

                for j, f in enumerate(fields):
                    name = f"c{j}"
                    select_exprs.append(
                        json_expr.map_elements(
                            lambda x, field=f: _extract_field(x, field),  # noqa: B023
                            return_dtype=pl.Utf8,
                        ).alias(name)
                    )
                    select_names.append(name)
            elif isinstance(col, WindowFunction) or (
                isinstance(col, ColumnOperation)
                and (
                    col.operation == "cast"
                    or col.operation == "alias"
                    or col.operation in ["*", "+", "-", "/", "**"]
                )
            ):
                # Handle window functions or ColumnOperation wrapping WindowFunction
                # Use helper to recursively extract WindowFunction and all arithmetic operations
                window_func, arithmetic_ops = (
                    self._extract_window_function_with_arithmetic(col)
                )

                # Also check for cast operation
                cast_type = None
                if isinstance(col, ColumnOperation) and col.operation == "cast":
                    if isinstance(col.column, WindowFunction):
                        cast_type = col.value
                    elif isinstance(col.column, ColumnOperation):
                        # Cast might be applied to arithmetic result
                        # Extract window function from nested operation
                        nested_window_func, _ = (
                            self._extract_window_function_with_arithmetic(col.column)
                        )
                        if nested_window_func:
                            window_func = nested_window_func
                            cast_type = col.value
                            # Re-extract arithmetic ops from the nested operation
                            _, nested_ops = (
                                self._extract_window_function_with_arithmetic(
                                    col.column
                                )
                            )
                            arithmetic_ops = nested_ops

                if window_func is None:
                    # Not a window function (e.g. 2 * F.col("x")) - use regular handling
                    alias_name = getattr(col, "name", None) or getattr(
                        col, "_alias_name", None
                    )
                    # Check for struct field access - need to check original column name if aliased
                    # When F.col("StructValue.E1").alias("E1-Extract") is used, col.name is the alias,
                    # but we need to check col._original_column._name or col.column.name for the struct path
                    struct_field_path = None
                    if hasattr(col, "name") and "." in col.name:
                        struct_field_path = col.name
                    elif hasattr(col, "_original_column") and hasattr(
                        col._original_column, "_name"
                    ):
                        # Check original column name for struct field path
                        original_col = col._original_column
                        if (
                            original_col is not None
                            and hasattr(original_col, "_name")
                            and "." in original_col._name
                        ):
                            struct_field_path = original_col._name
                    elif hasattr(col, "column") and hasattr(col.column, "name"):
                        # For ColumnOperation, check the column attribute
                        col_attr = col.column
                        if (
                            col_attr is not None
                            and hasattr(col_attr, "name")
                            and "." in col_attr.name
                        ):
                            struct_field_path = col_attr.name
                    elif isinstance(col, ColumnOperation) and hasattr(col, "column"):
                        # For ColumnOperation, check if column is a Column with struct field
                        col_attr = col.column
                        if col_attr is not None:
                            if hasattr(col_attr, "_name") and "." in col_attr._name:
                                struct_field_path = col_attr._name
                            elif hasattr(col_attr, "name") and "." in col_attr.name:
                                struct_field_path = col_attr.name

                    if struct_field_path:
                        parts = struct_field_path.split(".", 1)
                        struct_col, field_name = parts[0], parts[1]
                        from ...core.column_resolver import ColumnResolver

                        case_sensitive = self._get_case_sensitive()
                        resolved_struct_col = ColumnResolver.resolve_column_name(
                            struct_col, list(df.columns), case_sensitive
                        )
                        if resolved_struct_col and resolved_struct_col in df.columns:
                            struct_dtype = df[resolved_struct_col].dtype
                            if hasattr(struct_dtype, "fields") and struct_dtype.fields:
                                field_names = [f.name for f in struct_dtype.fields]
                                resolved_field = ColumnResolver.resolve_column_name(
                                    field_name,
                                    field_names,
                                    case_sensitive,
                                )
                                if resolved_field:
                                    expr = pl.col(resolved_struct_col).struct.field(
                                        resolved_field
                                    )
                                    if alias_name:
                                        expr = expr.alias(alias_name)
                                    select_exprs.append(expr)
                                    select_names.append(alias_name or struct_field_path)
                                    continue
                    try:
                        case_sensitive = self._get_case_sensitive()
                        expr = self.translator.translate(
                            col,
                            available_columns=list(df.columns),
                            case_sensitive=case_sensitive,
                        )
                        if alias_name:
                            expr = expr.alias(alias_name)
                            select_exprs.append(expr)
                            select_names.append(alias_name)
                        else:
                            select_exprs.append(expr)
                            col_name = getattr(col, "name", None)
                            select_names.append(
                                col_name
                                if col_name is not None
                                else f"col_{len(select_exprs)}"
                            )
                    except ValueError:
                        if rows_cache is None:
                            rows_cache = df.to_dicts()
                        if evaluator is None:
                            from sparkless.dataframe.evaluation.expression_evaluator import (
                                ExpressionEvaluator,
                            )

                            evaluator = ExpressionEvaluator()
                        values = [
                            self._evaluate_python_expression(row, col, evaluator)
                            for row in rows_cache
                        ]
                        column_name_candidate = alias_name or getattr(col, "name", None)
                        if not column_name_candidate:
                            column_name_candidate = (
                                f"col_{len(select_exprs) + len(python_columns) + 1}"
                            )
                        column_name = str(column_name_candidate)
                        if isinstance(col, ColumnOperation) and col.operation in {
                            "to_json",
                            "to_csv",
                        }:
                            struct_alias = self._format_struct_alias(col.column)
                            column_name = f"{col.operation}({struct_alias})"
                        python_columns.append((column_name, values))
                        select_names.append(column_name)
                    continue
                else:
                    # We found a WindowFunction - process it
                    # Save the original col for alias extraction later
                    original_col_for_alias = col
                    # Ensure function_name is set correctly
                    if (
                        not hasattr(window_func, "function_name")
                        or not window_func.function_name
                        or window_func.function_name == "window_function"
                    ) and hasattr(window_func, "function"):
                        function_name_from_func = getattr(
                            window_func.function, "function_name", None
                        )
                        if function_name_from_func:
                            window_func.function_name = function_name_from_func

                    window_spec = window_func.window_spec
                    function_name = getattr(window_func, "function_name", "").upper()
                    case_sensitive = self._get_case_sensitive()

                    # Build sort columns from partition_by and order_by
                    # Window functions need the DataFrame sorted before evaluation
                    sort_cols = []
                    has_order_by = bool(
                        hasattr(window_spec, "_order_by") and window_spec._order_by
                    )
                    has_partition_by = bool(
                        hasattr(window_spec, "_partition_by")
                        and window_spec._partition_by
                    )

                    # Sort DataFrame for this window function before building expression
                    if has_order_by:
                        # Add partition_by columns first
                        if has_partition_by:
                            for col in window_spec._partition_by:
                                if isinstance(col, str):
                                    name = col
                                elif hasattr(col, "name"):
                                    name = col.name
                                else:
                                    name = None
                                if name:
                                    resolved = self._resolve_window_col_name(
                                        df, name, case_sensitive
                                    )
                                    sort_cols.append(resolved)
                        # Add order_by columns
                        # Build sort parameters for window functions
                        window_sort_cols = []
                        window_desc_flags = []
                        window_nulls_last_flags = []
                        for col in window_spec._order_by:
                            col_name = None
                            is_desc = False
                            nulls_last = None  # None means default
                            if isinstance(col, str):
                                col_name = col
                                is_desc = False  # Default ascending
                                nulls_last = None
                            elif hasattr(col, "operation"):
                                operation = col.operation
                                if hasattr(col, "column") and hasattr(
                                    col.column, "name"
                                ):
                                    col_name = col.column.name
                                elif hasattr(col, "name"):
                                    col_name = col.name
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
                                    nulls_last = (
                                        True  # PySpark default: nulls last for desc()
                                    )
                                elif operation == "asc":
                                    is_desc = False
                                    nulls_last = (
                                        True  # PySpark default: nulls last for asc()
                                    )
                            elif hasattr(col, "name"):
                                col_name = col.name
                                is_desc = False
                                nulls_last = True  # PySpark default: nulls last

                            if col_name:
                                resolved_name = self._resolve_window_col_name(
                                    df, col_name, case_sensitive
                                )
                                window_sort_cols.append(resolved_name)
                                window_desc_flags.append(is_desc)
                                window_nulls_last_flags.append(nulls_last)
                                # Also add to sort_cols for compatibility with existing code
                                sort_cols.append(resolved_name)

                        # Sort window function data with proper nulls handling
                        if window_sort_cols:
                            has_nulls_spec = any(
                                n is not None for n in window_nulls_last_flags
                            )
                            # Polars sort: for single column, descending should be bool, not list
                            if len(window_sort_cols) == 1:
                                descending_single: bool = (
                                    window_desc_flags[0] if window_desc_flags else False
                                )
                                nulls_last_single: Optional[bool] = (
                                    window_nulls_last_flags[0]
                                    if window_nulls_last_flags
                                    and window_nulls_last_flags[0] is not None
                                    else None
                                )
                                if has_nulls_spec and nulls_last_single is not None:
                                    df = df.sort(
                                        window_sort_cols[0],
                                        descending=descending_single,
                                        nulls_last=nulls_last_single,
                                    )
                                else:
                                    df = df.sort(
                                        window_sort_cols[0],
                                        descending=descending_single,
                                    )
                            else:
                                descending_list: List[bool] = window_desc_flags
                                nulls_last_list: Optional[List[Optional[bool]]] = (
                                    window_nulls_last_flags if has_nulls_spec else None
                                )
                                if has_nulls_spec and nulls_last_list is not None:
                                    df = df.sort(
                                        window_sort_cols,
                                        descending=descending_list,
                                        nulls_last=nulls_last_list,
                                    )
                                else:
                                    df = df.sort(
                                        window_sort_cols, descending=descending_list
                                    )

                    # Sort if we have string column names (and haven't already sorted with expressions)
                    # CRITICAL: For lag/lead functions, we MUST sort before applying the window function
                    if function_name in ("LAG", "LEAD") and has_order_by:
                        # Rebuild sort_cols if needed to ensure we sort
                        if not sort_cols or not all(
                            isinstance(c, str) for c in sort_cols
                        ):
                            sort_cols = []
                            if has_partition_by:
                                for col in window_spec._partition_by:
                                    if isinstance(col, str):
                                        name = col
                                    elif hasattr(col, "name"):
                                        name = col.name
                                    else:
                                        name = None
                                    if name:
                                        sort_cols.append(
                                            self._resolve_window_col_name(
                                                df, name, case_sensitive
                                            )
                                        )
                            for col in window_spec._order_by:
                                if isinstance(col, str):
                                    name = col
                                elif hasattr(col, "name"):
                                    name = col.name
                                else:
                                    name = None
                                if name:
                                    sort_cols.append(
                                        self._resolve_window_col_name(
                                            df, name, case_sensitive
                                        )
                                    )
                        if sort_cols and all(isinstance(c, str) for c in sort_cols):
                            # Use the same descending flags as window_sort_cols
                            if len(sort_cols) == len(window_desc_flags):
                                if len(sort_cols) == 1:
                                    df = df.sort(
                                        sort_cols[0], descending=window_desc_flags[0]
                                    )
                                else:
                                    df = df.sort(
                                        sort_cols, descending=window_desc_flags
                                    )
                            else:
                                df = df.sort(sort_cols)
                    elif sort_cols and all(isinstance(c, str) for c in sort_cols):
                        # Use the same descending flags as window_sort_cols if available
                        if window_sort_cols and len(sort_cols) == len(
                            window_desc_flags
                        ):
                            if len(sort_cols) == 1:
                                df = df.sort(
                                    sort_cols[0], descending=window_desc_flags[0]
                                )
                            else:
                                df = df.sort(sort_cols, descending=window_desc_flags)
                        else:
                            df = df.sort(sort_cols)

                    # Special handling for rank()/dense_rank() without column_expr
                    # Polars ranks by value, but PySpark ranks by position in ordered window
                    # Solution: use row_number() to get position, then min(row_number) over value
                    # Check if column_expr would be None (no column argument to rank/dense_rank)
                    column_expr_would_be_none = (
                        not hasattr(window_func, "column_name")
                        or window_func.column_name is None
                        or (
                            hasattr(window_func, "column_name")
                            and window_func.column_name
                            in {
                                "__rank__",
                                "__dense_rank__",
                                "__row_number__",
                                "__cume_dist__",
                                "__percent_rank__",
                                "__ntile__",
                            }
                        )
                    )
                    # For rank()/dense_rank() without a column, PySpark ranks by position
                    # Polars ranks by value, so we need position-based ranking
                    needs_position_based_rank = (
                        function_name in ("RANK", "DENSE_RANK")
                        and column_expr_would_be_none
                        and has_order_by
                    )

                    try:
                        if needs_position_based_rank:
                            # Add row_number column to DataFrame (since it's already sorted)
                            # IMPORTANT: The DataFrame should already be sorted by the window's orderBy
                            # at this point, so row_numbers reflect the position in the ordered window
                            row_num_col = "__row_number_for_rank__"
                            row_num_expr = pl.int_range(pl.len()) + 1
                            df = df.with_columns(row_num_expr.alias(row_num_col))
                            # Debug: verify row_num_col is in df
                            if row_num_col not in df.columns:
                                import warnings

                                warnings.warn(
                                    f"row_num_col {row_num_col} not in df.columns: {df.columns}"
                                )

                            # Get the order column name for grouping
                            order_col_name = None
                            if window_spec._order_by:
                                first_order_col = window_spec._order_by[0]
                                if isinstance(first_order_col, str):
                                    order_col_name = self._resolve_window_col_name(
                                        df, first_order_col, case_sensitive
                                    )
                                elif hasattr(first_order_col, "column") and hasattr(
                                    first_order_col.column, "name"
                                ):
                                    order_col_name = self._resolve_window_col_name(
                                        df, first_order_col.column.name, case_sensitive
                                    )
                                elif hasattr(first_order_col, "name"):
                                    order_col_name = self._resolve_window_col_name(
                                        df, first_order_col.name, case_sensitive
                                    )

                            if order_col_name:
                                # Use min(row_number) over order column to get position-based rank
                                # For dense_rank, use rank(method="dense") on row_numbers
                                # Debug: verify order_col_name is in df
                                if order_col_name not in df.columns:
                                    import warnings

                                    warnings.warn(
                                        f"order_col_name {order_col_name} not in df.columns: {df.columns}"
                                    )
                                if function_name == "DENSE_RANK":
                                    # For dense_rank: use min(row_number) over order to get position,
                                    # then add as a column and rank(method='dense') on that
                                    # We can't nest window expressions, so we need to do it in two steps
                                    if has_partition_by:
                                        partition_cols = []
                                        for p_col in window_spec._partition_by:
                                            if isinstance(p_col, str):
                                                p_name = self._resolve_window_col_name(
                                                    df, p_col, case_sensitive
                                                )
                                            elif hasattr(p_col, "name"):
                                                p_name = self._resolve_window_col_name(
                                                    df, p_col.name, case_sensitive
                                                )
                                            else:
                                                continue
                                            if p_name:
                                                partition_cols.append(p_name)
                                        # Compute min(row_number) over partition+order
                                        min_rn_col = f"__min_rn_for_dense_rank_{len(select_exprs)}__"
                                        min_expr = (
                                            pl.col(row_num_col)
                                            .min()
                                            .over(partition_cols + [order_col_name])
                                        )
                                        df = df.with_columns(min_expr.alias(min_rn_col))
                                        # Then rank(method='dense') on that column
                                        window_expr = pl.col(min_rn_col).rank(
                                            method="dense"
                                        )
                                    else:
                                        # Compute min(row_number) over order
                                        min_rn_col = f"__min_rn_for_dense_rank_{len(select_exprs)}__"
                                        min_expr = (
                                            pl.col(row_num_col)
                                            .min()
                                            .over([order_col_name])
                                        )
                                        df = df.with_columns(min_expr.alias(min_rn_col))
                                        # Then rank(method='dense') on that column
                                        # Note: rank(method='dense') ranks globally, which is what we want
                                        # because min_rn already groups by order_col_name
                                        window_expr = pl.col(min_rn_col).rank(
                                            method="dense"
                                        )
                                else:
                                    # For RANK, use min(row_number) over order column
                                    if has_partition_by:
                                        partition_cols = []
                                        for p_col in window_spec._partition_by:
                                            if isinstance(p_col, str):
                                                p_name = self._resolve_window_col_name(
                                                    df, p_col, case_sensitive
                                                )
                                            elif hasattr(p_col, "name"):
                                                p_name = self._resolve_window_col_name(
                                                    df, p_col.name, case_sensitive
                                                )
                                            else:
                                                continue
                                            if p_name:
                                                partition_cols.append(p_name)
                                        window_expr = (
                                            pl.col(row_num_col)
                                            .min()
                                            .over(partition_cols + [order_col_name])
                                        )
                                    else:
                                        window_expr = (
                                            pl.col(row_num_col)
                                            .min()
                                            .over([order_col_name])
                                        )
                            else:
                                # Fallback to regular rank if we can't extract order column
                                window_expr = (
                                    self.window_handler.translate_window_function(
                                        window_func, df, case_sensitive=case_sensitive
                                    )
                                )
                        else:
                            window_expr = self.window_handler.translate_window_function(
                                window_func, df, case_sensitive=case_sensitive
                            )

                        # Apply all arithmetic operations in order (innermost to outermost)
                        # (applies to both position-based and regular window functions)
                        for op, val, is_reverse in arithmetic_ops:
                            right = self._arith_operand_to_polars(val, df)
                            if op == "*":
                                window_expr = window_expr * right
                            elif op == "+":
                                window_expr = window_expr + right
                            elif op == "-":
                                if is_reverse:
                                    # Literal - WindowFunction
                                    window_expr = pl.lit(val) - window_expr
                                elif isinstance(val, (Column, ColumnOperation)):
                                    # Column - WindowFunction (left - window)
                                    window_expr = right - window_expr
                                else:
                                    # WindowFunction - value
                                    window_expr = window_expr - right
                            elif op == "/":
                                if is_reverse:
                                    window_expr = pl.lit(val) / window_expr
                                else:
                                    window_expr = window_expr / right
                            elif op == "**":
                                window_expr = window_expr.pow(
                                    val if isinstance(val, (int, float)) else right
                                )

                        # Apply cast if this is a cast operation
                        if cast_type is not None:
                            from .type_mapper import mock_type_to_polars_dtype
                            from sparkless.spark_types import (
                                StringType,
                                IntegerType,
                                LongType,
                                DoubleType,
                                FloatType,
                                BooleanType,
                                DateType,
                                TimestampType,
                                ShortType,
                                ByteType,
                            )

                            # Handle string type names
                            if isinstance(cast_type, str):
                                type_name_map = {
                                    "string": StringType(),
                                    "str": StringType(),
                                    "int": IntegerType(),
                                    "integer": IntegerType(),
                                    "long": LongType(),
                                    "bigint": LongType(),
                                    "double": DoubleType(),
                                    "float": FloatType(),
                                    "boolean": BooleanType(),
                                    "bool": BooleanType(),
                                    "date": DateType(),
                                    "timestamp": TimestampType(),
                                    "short": ShortType(),
                                    "byte": ByteType(),
                                }
                                cast_type = type_name_map.get(cast_type.lower())

                            if cast_type is not None:
                                polars_dtype = mock_type_to_polars_dtype(cast_type)
                                window_expr = window_expr.cast(
                                    polars_dtype, strict=False
                                )

                        # Extract alias from the original col (before any processing)
                        # Check _alias_name first (from .alias() call), then name, then fallback to default
                        alias_name = (
                            getattr(original_col_for_alias, "_alias_name", None)
                            or getattr(original_col_for_alias, "name", None)
                            or (
                                f"{window_func.function_name.lower()}_window"
                                if hasattr(window_func, "function_name")
                                else "window_result"
                            )
                        )
                        select_exprs.append(window_expr.alias(alias_name))
                        select_names.append(alias_name)
                    except ValueError:
                        # Fallback to Python evaluation for unsupported window functions
                        # This path needs to handle arithmetic operations too
                        # IMPORTANT: rows_cache must be built from the sorted DataFrame
                        # Rebuild rows_cache from the current (sorted) df to ensure correct order
                        rows_cache = df.to_dicts()
                        if not hasattr(self, "_python_window_functions"):
                            self._python_window_functions: List[Any] = []

                        # Evaluate window function first
                        # rows_cache should be built from sorted DataFrame (done above)
                        results = window_func.evaluate(rows_cache)

                        # Apply arithmetic operations
                        for op, val, is_reverse in arithmetic_ops:
                            if op == "*":
                                results = [
                                    r * val if r is not None else None for r in results
                                ]
                            elif op == "+":
                                results = [
                                    r + val if r is not None else None for r in results
                                ]
                            elif op == "-":
                                if is_reverse:
                                    results = [
                                        val - r if r is not None else None
                                        for r in results
                                    ]
                                else:
                                    results = [
                                        r - val if r is not None else None
                                        for r in results
                                    ]
                            elif op == "/":
                                if is_reverse:
                                    results = [
                                        val / r if r is not None and r != 0 else None
                                        for r in results
                                    ]
                                else:
                                    results = [
                                        r / val if r is not None else None
                                        for r in results
                                    ]
                            elif op == "**":
                                results = [
                                    r**val if r is not None else None for r in results
                                ]

                        # Apply cast if needed
                        if cast_type is not None:
                            from sparkless.dataframe.casting.type_converter import (
                                TypeConverter,
                            )
                            from sparkless.spark_types import (
                                StringType,
                                IntegerType,
                                LongType,
                                DoubleType,
                                FloatType,
                                BooleanType,
                                DateType,
                                TimestampType,
                                ShortType,
                                ByteType,
                            )

                            # Handle string type names
                            if isinstance(cast_type, str):
                                type_name_map = {
                                    "string": StringType(),
                                    "str": StringType(),
                                    "int": IntegerType(),
                                    "integer": IntegerType(),
                                    "long": LongType(),
                                    "bigint": LongType(),
                                    "double": DoubleType(),
                                    "float": FloatType(),
                                    "boolean": BooleanType(),
                                    "bool": BooleanType(),
                                    "date": DateType(),
                                    "timestamp": TimestampType(),
                                    "short": ShortType(),
                                    "byte": ByteType(),
                                }
                                cast_type = type_name_map.get(cast_type.lower())

                            if cast_type is not None:
                                results = [
                                    TypeConverter.cast_to_type(r, cast_type)
                                    if r is not None
                                    else None
                                    for r in results
                                ]

                        # Extract alias from the original col (before any processing)
                        # Check _alias_name first (from .alias() call), then name, then fallback to default
                        alias_name = (
                            getattr(original_col_for_alias, "_alias_name", None)
                            or getattr(original_col_for_alias, "name", None)
                            or (
                                f"{window_func.function_name.lower()}_window"
                                if hasattr(window_func, "function_name")
                                else "window_result"
                            )
                        )
                        # Store for later addition to result (will be added after select_exprs are processed)
                        # Format: (alias_name, results_list, arithmetic_ops, cast_type)
                        if not hasattr(self, "_python_window_functions"):
                            self._python_window_functions = []
                        self._python_window_functions.append(
                            (alias_name, results, arithmetic_ops, cast_type)
                        )
                        select_names.append(alias_name)
                        continue
            else:
                alias_name = getattr(col, "name", None) or getattr(
                    col, "_alias_name", None
                )

                # Handle nested struct field access for Column objects (e.g., F.col("Person.name"))
                # When F.col("StructValue.E1").alias("E1-Extract") is used, col.name is the alias,
                # but we need to check col._original_column._name for the struct field path
                struct_field_path = None
                if hasattr(col, "name") and "." in col.name:
                    struct_field_path = col.name
                elif (
                    hasattr(col, "_original_column")
                    and hasattr(col._original_column, "_name")
                    and "." in col._original_column._name
                ):
                    # Check original column name for struct field path (for aliased columns)
                    struct_field_path = col._original_column._name

                if struct_field_path:
                    # Split into struct column and field name
                    parts = struct_field_path.split(".", 1)
                    struct_col = parts[0]
                    field_name = parts[1]

                    # Resolve struct column name case-insensitively
                    from ...core.column_resolver import ColumnResolver

                    case_sensitive = self._get_case_sensitive()
                    resolved_struct_col = ColumnResolver.resolve_column_name(
                        struct_col, list(df.columns), case_sensitive
                    )
                    if resolved_struct_col and resolved_struct_col in df.columns:
                        # Get struct dtype
                        struct_dtype = df[resolved_struct_col].dtype
                        if hasattr(struct_dtype, "fields") and struct_dtype.fields:
                            # Resolve field name case-insensitively within struct
                            field_names = [f.name for f in struct_dtype.fields]
                            resolved_field = ColumnResolver.resolve_column_name(
                                field_name, field_names, case_sensitive
                            )
                            if resolved_field:
                                # Use Polars struct.field() syntax for nested access
                                expr = pl.col(resolved_struct_col).struct.field(
                                    resolved_field
                                )
                                if alias_name:
                                    expr = expr.alias(alias_name)
                                select_exprs.append(expr)
                                select_names.append(alias_name or struct_field_path)
                                continue

                try:
                    # Pass available columns and case sensitivity for column resolution
                    case_sensitive = self._get_case_sensitive()
                    expr = self.translator.translate(
                        col,
                        available_columns=list(df.columns),
                        case_sensitive=case_sensitive,
                    )
                    if alias_name:
                        expr = expr.alias(alias_name)
                        select_exprs.append(expr)
                        select_names.append(alias_name)
                    else:
                        select_exprs.append(expr)
                        if hasattr(col, "name"):
                            select_names.append(col.name)
                        elif isinstance(col, str):
                            select_names.append(col)
                        else:
                            select_names.append(f"col_{len(select_exprs)}")
                except ValueError:
                    # Fallback to Python evaluation for unsupported expressions
                    if rows_cache is None:
                        rows_cache = df.to_dicts()
                    if evaluator is None:
                        from sparkless.dataframe.evaluation.expression_evaluator import (
                            ExpressionEvaluator,
                        )

                        evaluator = ExpressionEvaluator()
                    values = [
                        self._evaluate_python_expression(row, col, evaluator)
                        for row in rows_cache
                    ]
                    column_name_candidate = alias_name or getattr(col, "name", None)
                    if not column_name_candidate:
                        column_name_candidate = (
                            f"col_{len(select_exprs) + len(python_columns) + 1}"
                        )
                    column_name = str(column_name_candidate)
                    if isinstance(col, ColumnOperation) and col.operation in {
                        "to_json",
                        "to_csv",
                    }:
                        struct_alias = self._format_struct_alias(col.column)
                        column_name = f"{col.operation}({struct_alias})"
                    python_columns.append((column_name, values))
                    select_names.append(column_name)
                    continue

        if not select_exprs and not python_columns:
            return df

        # Check if any column uses explode or explode_outer operation
        has_explode = False
        has_explode_outer = False
        explode_index = None
        explode_outer_index = None
        for i, col in enumerate(columns):
            col_operation = getattr(col, "operation", None) or getattr(
                col, "function_name", None
            )
            if col_operation == "explode":
                has_explode = True
                explode_index = i
            elif col_operation == "explode_outer":
                has_explode_outer = True
                explode_outer_index = i

        if select_exprs:
            try:
                if has_explode or has_explode_outer:
                    result = df.select(select_exprs)
                    exploded_col_name = None
                    if (
                        has_explode
                        and explode_index is not None
                        and explode_index < len(select_names)
                    ):
                        exploded_col_name = select_names[explode_index]
                    elif (
                        has_explode_outer
                        and explode_outer_index is not None
                        and explode_outer_index < len(select_names)
                    ):
                        exploded_col_name = select_names[explode_outer_index]
                    if exploded_col_name:
                        result = result.explode(exploded_col_name)
                else:
                    result = df.select(select_exprs)
            except Exception as e:
                # Catch Polars InvalidOperationError for unsupported casts
                # (e.g., string to boolean which Polars doesn't support directly)
                import polars.exceptions

                error_msg = str(e)
                error_type = type(e).__name__
                is_invalid_cast = (
                    isinstance(e, polars.exceptions.InvalidOperationError)
                    or "InvalidOperationError" in error_type
                    or (
                        "not supported" in error_msg.lower()
                        and "casting" in error_msg.lower()
                    )
                )

                if is_invalid_cast:
                    # Fallback to Python evaluation for all columns when Polars cast fails
                    # This handles cases like string to boolean where Polars doesn't support the cast
                    if rows_cache is None:
                        rows_cache = df.to_dicts()
                    if evaluator is None:
                        from sparkless.dataframe.evaluation.expression_evaluator import (
                            ExpressionEvaluator,
                        )

                        evaluator = ExpressionEvaluator()

                    # Evaluate all columns in Python (both translated and Python ones)
                    all_python_columns: List[Tuple[str, List[Any]]] = []
                    all_python_names: List[str] = []

                    # Process all columns (including ones that were in select_exprs)
                    for col in columns:
                        alias_name = None
                        if isinstance(col, tuple):
                            col, alias_name = col
                        values = [
                            self._evaluate_python_expression(row, col, evaluator)
                            for row in rows_cache
                        ]
                        column_name_candidate = alias_name or getattr(col, "name", None)
                        if not column_name_candidate:
                            column_name_candidate = f"col_{len(all_python_columns) + 1}"
                        column_name = str(column_name_candidate)
                        if isinstance(col, ColumnOperation) and col.operation in {
                            "to_json",
                            "to_csv",
                        }:
                            struct_alias = self._format_struct_alias(col.column)
                            column_name = f"{col.operation}({struct_alias})"
                        all_python_columns.append((column_name, values))
                        all_python_names.append(column_name)

                    # Build result from Python-evaluated columns
                    if all_python_columns:
                        data_dict = dict(all_python_columns)
                        result = pl.DataFrame(data_dict)
                    else:
                        return df
                else:
                    # Re-raise if it's not a cast-related error
                    raise
        elif python_columns:
            # Only Python-evaluated columns; build DataFrame from values
            data_dict = dict(python_columns)
            result = pl.DataFrame(data_dict)
        else:
            return df

        # Special handling: if we're selecting only literals (no column references),
        # Polars returns 1 row by default. We need to ensure the literal broadcasts
        # to all rows in the source DataFrame.
        # Check if result has fewer rows than source and we're selecting expressions
        # (not string column names)
        if select_exprs and len(result) == 1 and len(df) > 1:
            # Check if all selected items are expressions (not string column names)
            # If all are expressions and none reference columns from df, they're literals
            has_column_reference = False
            for col in columns:
                if isinstance(col, str):
                    # String column name - definitely references a column
                    has_column_reference = True
                    break
                # Check if expression references columns from the DataFrame
                # We can't easily inspect Polars expressions, so we use a heuristic:
                # If the result has 1 row and source has >1 rows, and we're not selecting
                # string column names, it's likely all literals

            # If no column references and result is shorter, replicate
            if not has_column_reference and len(result) < len(df):
                # Replicate the single row to match DataFrame length
                result = pl.concat([result] * len(df))

        # Append Python-evaluated columns
        for name, values in python_columns:
            result = result.with_columns(pl.Series(name, values))

        # Evaluate window functions that require Python evaluation
        # These need to be evaluated across all rows, not row-by-row
        # Initialize had_python_window_functions before the if block to avoid UnboundLocalError
        had_python_window_functions = hasattr(
            self, "_python_window_functions"
        ) and bool(getattr(self, "_python_window_functions", None))
        if hasattr(self, "_python_window_functions") and self._python_window_functions:
            # Check if we have pre-computed results (with arithmetic) or need to evaluate
            first_item = self._python_window_functions[0]
            if len(first_item) == 4:
                # New format: (alias_name, results, arithmetic_ops, cast_type)
                # Results are already computed with arithmetic applied
                for (
                    alias_name,
                    results,
                    arithmetic_ops,
                    cast_type,
                ) in self._python_window_functions:
                    # Apply cast if needed
                    if cast_type is not None:
                        from sparkless.dataframe.casting.type_converter import (
                            TypeConverter,
                        )
                        from sparkless.spark_types import (
                            StringType,
                            IntegerType,
                            LongType,
                            DoubleType,
                            FloatType,
                            BooleanType,
                            DateType,
                            TimestampType,
                            ShortType,
                            ByteType,
                        )

                        # Handle string type names
                        if isinstance(cast_type, str):
                            type_name_map = {
                                "string": StringType(),
                                "str": StringType(),
                                "int": IntegerType(),
                                "integer": IntegerType(),
                                "long": LongType(),
                                "bigint": LongType(),
                                "double": DoubleType(),
                                "float": FloatType(),
                                "boolean": BooleanType(),
                                "bool": BooleanType(),
                                "date": DateType(),
                                "timestamp": TimestampType(),
                                "short": ShortType(),
                                "byte": ByteType(),
                            }
                            cast_type = type_name_map.get(cast_type.lower())

                        if cast_type is not None:
                            results = [
                                TypeConverter.cast_to_type(r, cast_type)
                                if r is not None
                                else None
                                for r in results
                            ]
                    result = result.with_columns(pl.Series(alias_name, results))
            else:
                # Old format: (alias_name, window_func, _)
                from sparkless.dataframe.window_handler import WindowFunctionHandler
                from sparkless.dataframe import DataFrame

                # Use the cached rows for window function evaluation
                data_rows = rows_cache if rows_cache else result.to_dicts()

                # Create a temporary DataFrame for window function evaluation
                from sparkless.spark_types import StructType

                temp_df = DataFrame(data_rows, StructType([]), None)
                window_handler = WindowFunctionHandler(temp_df)

                # Evaluate all window functions
                for alias_name, window_func, _ in self._python_window_functions:
                    # Evaluate window function across all rows
                    window_handler.evaluate_window_functions(
                        data_rows, [(alias_name, window_func)]
                    )
                    # Extract values from evaluated data
                    values = [row.get(alias_name) for row in data_rows]
                    result = result.with_columns(pl.Series(alias_name, values))

            # Clean up
            if had_python_window_functions:
                delattr(self, "_python_window_functions")

        # Only reorder if we have python_columns AND the order doesn't match
        # This ensures we preserve all columns while matching the requested order
        # Note: When aliases are applied, result.columns should already match select_names,
        # so reordering should preserve the aliased column names
        if select_names and (python_columns or had_python_window_functions):
            existing_cols = list(result.columns)
            # Check if reordering is needed and safe
            # select_names contains the requested column names (with aliases applied)
            # existing_cols should match select_names if aliases were applied correctly
            if existing_cols != select_names and all(
                name in existing_cols for name in select_names
            ):
                # Reorder using select_names - these should already be in the DataFrame from aliases
                result = result.select(select_names)

        return result

    def _evaluate_python_expression(
        self,
        row: Dict[str, Any],
        expression: Any,
        evaluator: ExpressionEvaluator,
    ) -> Any:
        """Evaluate expressions that require Python fallbacks."""
        if isinstance(expression, ColumnOperation):
            op_name = expression.operation
            if op_name == "from_json":
                return self._python_from_json(row, expression)
            if op_name == "to_json":
                return self._python_to_json(row, expression)
            if op_name == "to_csv":
                return self._python_to_csv(row, expression)
        return evaluator.evaluate_expression(row, expression)

    def _get_struct_field_names(self, column_name: str, struct_dtype: Any) -> List[str]:
        """Return struct field names, caching results when shortcuts are enabled."""

        if not hasattr(struct_dtype, "fields") or not struct_dtype.fields:
            return []

        cache_key = (column_name, repr(struct_dtype))
        if self._shortcuts_enabled:
            cached = self._struct_field_cache.get(cache_key)
            if cached is not None:
                return cached

        field_names = [field.name for field in struct_dtype.fields]

        if self._shortcuts_enabled:
            # Store a shallow copy in case downstream users mutate the list.
            self._struct_field_cache[cache_key] = list(field_names)

        return field_names

    def _python_from_json(
        self, row: Dict[str, Any], expression: ColumnOperation
    ) -> Any:
        column_name = self._extract_column_name(expression.column)
        if not column_name:
            return None

        raw_value = row.get(column_name)
        if raw_value is None:
            return None

        schema_spec, _ = self._unpack_schema_and_options(expression)
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return None

        schema = self._resolve_struct_schema(schema_spec)
        if schema is None:
            return parsed

        if not isinstance(parsed, dict):
            return None

        return {field.name: parsed.get(field.name) for field in schema.fields}

    def _python_to_json(
        self, row: Dict[str, Any], expression: ColumnOperation
    ) -> Union[str, None]:
        field_names = self._extract_struct_field_names(expression.column)
        if not field_names:
            return None
        struct_dict = {name: row.get(name) for name in field_names}
        return json.dumps(struct_dict, ensure_ascii=False, separators=(",", ":"))

    def _python_to_csv(
        self, row: Dict[str, Any], expression: ColumnOperation
    ) -> Union[str, None]:
        field_names = self._extract_struct_field_names(expression.column)
        if not field_names:
            return None

        values = []
        for name in field_names:
            val = row.get(name)
            values.append("" if val is None else str(val))
        return ",".join(values)

    def _extract_column_name(self, expr: Any) -> Optional[str]:
        if isinstance(expr, Column):
            name = expr.name
            return str(name) if name is not None else None
        if isinstance(expr, ColumnOperation) and hasattr(expr, "name"):
            name_attr = getattr(expr, "name", None)
            if name_attr is not None:
                return str(name_attr)
            return None
        if isinstance(expr, str):
            return expr
        name_attr = getattr(expr, "name", None)
        if name_attr is not None:
            return str(name_attr)
        return None

    def _extract_struct_field_names(self, expr: Any) -> List[str]:
        names: List[str] = []
        if isinstance(expr, ColumnOperation) and expr.operation == "struct":
            first = self._extract_column_name(expr.column)
            if first:
                names.append(first)
            additional = expr.value
            if isinstance(additional, tuple):
                for item in additional:
                    name = self._extract_column_name(item)
                    if name:
                        names.append(name)
        else:
            name = self._extract_column_name(expr)
            if name:
                names.append(name)
        return names

    def _format_struct_alias(self, expr: Any) -> str:
        names = self._extract_struct_field_names(expr)
        if names:
            return f"struct({', '.join(names)})"
        return "struct(...)"

    def _unpack_schema_and_options(
        self, expression: ColumnOperation
    ) -> Tuple[Any, Dict[str, Any]]:
        schema_spec: Any = None
        options: Dict[str, Any] = {}

        raw_value = getattr(expression, "value", None)
        if isinstance(raw_value, tuple):
            if len(raw_value) >= 1:
                schema_spec = raw_value[0]
            if len(raw_value) >= 2 and isinstance(raw_value[1], dict):
                options = dict(raw_value[1])
        elif isinstance(raw_value, dict):
            options = dict(raw_value)

        return schema_spec, options

    def _resolve_struct_schema(self, schema_spec: Any) -> Union[StructType, None]:
        if schema_spec is None:
            return None

        if isinstance(schema_spec, StructType):
            return schema_spec

        if hasattr(schema_spec, "value"):
            return self._resolve_struct_schema(schema_spec.value)

        if isinstance(schema_spec, str):
            try:
                return parse_ddl_schema(schema_spec)
            except Exception:
                return StructType([])

        return None

    @profiled("polars.apply_with_column", category="polars")
    def apply_with_column(
        self,
        df: pl.DataFrame,
        column_name: str,
        expression: Any,
        expected_field: Any = None,
    ) -> pl.DataFrame:
        """Apply a withColumn operation.

        Args:
            df: Source Polars DataFrame
            column_name: Name of new/updated column
            expression: Expression for the column

        Returns:
            DataFrame with new column
        """
        # Check if expression is a ColumnOperation containing window function arithmetic
        # This must be checked before the simple window function check
        window_func, arithmetic_ops = self._extract_window_function_with_arithmetic(
            expression
        )
        cast_type = None

        # Check for cast operation on window function arithmetic
        if isinstance(expression, ColumnOperation) and expression.operation == "cast":
            if window_func:
                cast_type = expression.value
            elif isinstance(expression.column, ColumnOperation):
                # Cast might be applied to arithmetic result
                nested_window_func, nested_ops = (
                    self._extract_window_function_with_arithmetic(expression.column)
                )
                if nested_window_func:
                    window_func = nested_window_func
                    arithmetic_ops = nested_ops
                    cast_type = expression.value

        # Check if expression is a WindowFunction or a ColumnOperation wrapping a WindowFunction (e.g., WindowFunction.cast())
        is_window_function = isinstance(expression, WindowFunction)
        is_window_function_cast = (
            isinstance(expression, ColumnOperation)
            and expression.operation == "cast"
            and isinstance(expression.column, WindowFunction)
        )

        if window_func is not None or is_window_function or is_window_function_cast:
            # Window functions need special handling
            # For window functions with order_by, we need to sort the DataFrame first
            # to ensure correct window function evaluation

            # Extract the WindowFunction if it's wrapped in a ColumnOperation
            # Use extracted window_func if available, otherwise use existing logic
            if window_func is None:
                window_func = (
                    expression
                    if isinstance(expression, WindowFunction)
                    else expression.column
                )
                cast_type = expression.value if is_window_function_cast else None
            # If window_func was extracted from arithmetic, arithmetic_ops and cast_type are already set

            # Ensure function_name is set correctly - extract from function if needed
            if (
                not hasattr(window_func, "function_name")
                or not window_func.function_name
                or window_func.function_name == "window_function"
            ) and hasattr(window_func, "function"):
                function_name_from_func = getattr(
                    window_func.function, "function_name", None
                )
                if function_name_from_func:
                    window_func.function_name = function_name_from_func

            window_spec = window_func.window_spec
            function_name = getattr(window_func, "function_name", "").upper()
            case_sensitive = self._get_case_sensitive()

            # Build sort columns from partition_by and order_by
            sort_cols = []
            has_order_by = hasattr(window_spec, "_order_by") and window_spec._order_by
            has_partition_by = (
                hasattr(window_spec, "_partition_by") and window_spec._partition_by
            )

            if has_order_by:
                # Add partition_by columns first
                if has_partition_by:
                    for col in window_spec._partition_by:
                        if isinstance(col, str):
                            name = col
                        elif hasattr(col, "name"):
                            name = col.name
                        else:
                            name = None
                        if name:
                            resolved = self._resolve_window_col_name(
                                df, name, case_sensitive
                            )
                            sort_cols.append(resolved)
                # Add order_by columns
                # Build sort parameters for window functions
                window_sort_cols = []
                window_desc_flags = []
                window_nulls_last_flags = []
                for col in window_spec._order_by:
                    col_name = None
                    is_desc = False
                    nulls_last = None  # None means default
                    if isinstance(col, str):
                        col_name = col
                        is_desc = False  # Default ascending
                        nulls_last = None
                    elif hasattr(col, "operation"):
                        operation = col.operation
                        if hasattr(col, "column") and hasattr(col.column, "name"):
                            col_name = col.column.name
                        elif hasattr(col, "name"):
                            col_name = col.name
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
                            nulls_last = True  # PySpark default: nulls last for desc()
                        elif operation == "asc":
                            is_desc = False
                            nulls_last = True  # PySpark default: nulls last for asc()
                    elif hasattr(col, "name"):
                        col_name = col.name
                        is_desc = False
                        nulls_last = True  # PySpark default: nulls last

                    if col_name:
                        resolved_name = self._resolve_window_col_name(
                            df, col_name, case_sensitive
                        )
                        window_sort_cols.append(resolved_name)
                        window_desc_flags.append(is_desc)
                        window_nulls_last_flags.append(nulls_last)
                        # Also add to sort_cols for compatibility with existing code
                        sort_cols.append(resolved_name)

                # Sort window function data with proper nulls handling
                if window_sort_cols:
                    has_nulls_spec = any(n is not None for n in window_nulls_last_flags)
                    if has_nulls_spec:
                        df = df.sort(
                            window_sort_cols,
                            descending=window_desc_flags,
                            nulls_last=window_nulls_last_flags,
                        )
                    else:
                        df = df.sort(window_sort_cols, descending=window_desc_flags)

            # Sort if we have string column names (and haven't already sorted with expressions)
            # CRITICAL: For lag/lead functions, we MUST sort before applying the window function
            if function_name in ("LAG", "LEAD") and has_order_by:
                # Rebuild sort_cols if needed to ensure we sort
                if not sort_cols or not all(isinstance(c, str) for c in sort_cols):
                    sort_cols = []
                    if has_partition_by:
                        for col in window_spec._partition_by:
                            if isinstance(col, str):
                                name = col
                            elif hasattr(col, "name"):
                                name = col.name
                            else:
                                name = None
                            if name:
                                sort_cols.append(
                                    self._resolve_window_col_name(
                                        df, name, case_sensitive
                                    )
                                )
                    for col in window_spec._order_by:
                        if isinstance(col, str):
                            name = col
                        elif hasattr(col, "name"):
                            name = col.name
                        else:
                            name = None
                        if name:
                            sort_cols.append(
                                self._resolve_window_col_name(df, name, case_sensitive)
                            )
                if sort_cols and all(isinstance(c, str) for c in sort_cols):
                    df = df.sort(sort_cols)
            elif sort_cols and all(isinstance(c, str) for c in sort_cols):
                df = df.sort(sort_cols)

            try:
                window_expr = self.window_handler.translate_window_function(
                    window_func, df, case_sensitive=case_sensitive
                )

                # Apply all arithmetic operations in order (innermost to outermost)
                for op, val, is_reverse in arithmetic_ops:
                    right = self._arith_operand_to_polars(val, df)
                    if op == "*":
                        window_expr = window_expr * right
                    elif op == "+":
                        window_expr = window_expr + right
                    elif op == "-":
                        if is_reverse:
                            # Literal - WindowFunction
                            window_expr = pl.lit(val) - window_expr
                        elif isinstance(val, ColumnOperation) or (
                            hasattr(val, "name") and hasattr(val, "column_type")
                        ):
                            # Column - WindowFunction (left - window)
                            # Check for Column by checking for Column-like attributes
                            window_expr = right - window_expr
                        else:
                            # WindowFunction - value
                            window_expr = window_expr - right
                    elif op == "/":
                        if is_reverse:
                            window_expr = pl.lit(val) / window_expr
                        else:
                            window_expr = window_expr / right
                    elif op == "**":
                        window_expr = window_expr.pow(
                            val if isinstance(val, (int, float)) else right
                        )

                # Apply cast if this is a cast operation
                if (
                    is_window_function_cast or cast_type is not None
                ) and cast_type is not None:
                    from .type_mapper import mock_type_to_polars_dtype
                    from sparkless.spark_types import (
                        StringType,
                        IntegerType,
                        LongType,
                        DoubleType,
                        FloatType,
                        BooleanType,
                        DateType,
                        TimestampType,
                        ShortType,
                        ByteType,
                    )

                    # Handle string type names
                    if isinstance(cast_type, str):
                        type_name_map = {
                            "string": StringType(),
                            "str": StringType(),
                            "int": IntegerType(),
                            "integer": IntegerType(),
                            "long": LongType(),
                            "bigint": LongType(),
                            "double": DoubleType(),
                            "float": FloatType(),
                            "boolean": BooleanType(),
                            "bool": BooleanType(),
                            "date": DateType(),
                            "timestamp": TimestampType(),
                            "short": ShortType(),
                            "byte": ByteType(),
                        }
                        cast_type = type_name_map.get(cast_type.lower())

                    if cast_type is not None:
                        polars_dtype = mock_type_to_polars_dtype(cast_type)
                        window_expr = window_expr.cast(polars_dtype, strict=False)

                result = df.with_columns(window_expr.alias(column_name))

            except ValueError:
                # Fallback to Python evaluation for unsupported window functions
                # Ensure data is sorted before evaluation (df should already be sorted above)
                # Convert Polars DataFrame to list of dicts for Python evaluation
                data = df.to_dicts()

                # Evaluate window function using Python implementation
                # WindowFunction.evaluate() expects sorted data for correct results
                results = window_func.evaluate(data)

                # Apply arithmetic operations
                for op, val, is_reverse in arithmetic_ops:
                    if op == "*":
                        results = [r * val if r is not None else None for r in results]
                    elif op == "+":
                        results = [r + val if r is not None else None for r in results]
                    elif op == "-":
                        if is_reverse:
                            results = [
                                val - r if r is not None else None for r in results
                            ]
                        else:
                            results = [
                                r - val if r is not None else None for r in results
                            ]
                    elif op == "/":
                        if is_reverse:
                            results = [
                                val / r if r is not None and r != 0 else None
                                for r in results
                            ]
                        else:
                            results = [
                                r / val if r is not None else None for r in results
                            ]
                    elif op == "**":
                        results = [r**val if r is not None else None for r in results]

                # Apply cast if this is a cast operation
                if (
                    is_window_function_cast or cast_type is not None
                ) and cast_type is not None:
                    from sparkless.dataframe.casting.type_converter import TypeConverter
                    from sparkless.spark_types import (
                        StringType,
                        IntegerType,
                        LongType,
                        DoubleType,
                        FloatType,
                        BooleanType,
                        DateType,
                        TimestampType,
                        ShortType,
                        ByteType,
                    )

                    # Handle string type names
                    if isinstance(cast_type, str):
                        type_name_map = {
                            "string": StringType(),
                            "str": StringType(),
                            "int": IntegerType(),
                            "integer": IntegerType(),
                            "long": LongType(),
                            "bigint": LongType(),
                            "double": DoubleType(),
                            "float": FloatType(),
                            "boolean": BooleanType(),
                            "bool": BooleanType(),
                            "date": DateType(),
                            "timestamp": TimestampType(),
                            "short": ShortType(),
                            "byte": ByteType(),
                        }
                        cast_type = type_name_map.get(cast_type.lower())

                    if cast_type is not None:
                        results = [
                            TypeConverter.cast_to_type(r, cast_type)
                            if r is not None
                            else None
                            for r in results
                        ]

                # Add results as new column
                result = df.with_columns(pl.Series(column_name, results))

            # For lag/lead, ensure result maintains sort order
            # CRITICAL: Must sort result to preserve correct window function values
            if function_name in ("LAG", "LEAD"):
                # Rebuild sort_cols if needed
                if not sort_cols or not all(isinstance(c, str) for c in sort_cols):
                    sort_cols = []
                    if has_partition_by:
                        for col in window_spec._partition_by:
                            if isinstance(col, str):
                                sort_cols.append(col)
                            elif hasattr(col, "name"):
                                sort_cols.append(col.name)
                    if has_order_by:
                        for col in window_spec._order_by:
                            if isinstance(col, str):
                                sort_cols.append(col)
                            elif hasattr(col, "name"):
                                sort_cols.append(col.name)
                if sort_cols and all(isinstance(c, str) for c in sort_cols):
                    result = result.sort(sort_cols)

            return result
        else:
            # Check if this is an explode or explode_outer operation
            # These operations need special handling: they explode arrays into multiple rows
            is_explode = (
                isinstance(expression, ColumnOperation)
                and expression.operation == "explode"
            )
            is_explode_outer = (
                isinstance(expression, ColumnOperation)
                and expression.operation == "explode_outer"
            )

            if is_explode or is_explode_outer:
                # For explode operations, we need to:
                # 1. Get the source column (the array column to explode)
                # 2. Add it as a new column with the array values
                # 3. Explode the DataFrame on that new column (this creates multiple rows)
                from sparkless.functions import Column as ColumnType

                # Get the source column name
                source_col = expression.column
                if isinstance(source_col, ColumnType):
                    source_col_name = source_col.name
                elif isinstance(source_col, str):
                    source_col_name = source_col
                else:
                    # Fallback: try to get name from column attribute
                    source_col_name_attr = getattr(source_col, "name", None)
                    if source_col_name_attr is None:
                        raise ValueError(
                            f"Cannot determine source column for explode operation: {expression}"
                        )
                    source_col_name = str(source_col_name_attr)

                # Resolve column name (case-insensitive)
                from ...core.column_resolver import ColumnResolver

                case_sensitive = self._get_case_sensitive()
                resolved_col_name = ColumnResolver.resolve_column_name(
                    source_col_name, list(df.columns), case_sensitive
                )
                if resolved_col_name is None:
                    raise ValueError(
                        f"Column '{source_col_name}' not found in DataFrame for explode operation"
                    )

                # Check if the column exists
                if resolved_col_name not in df.columns:
                    raise ValueError(
                        f"Column '{resolved_col_name}' not found in DataFrame"
                    )

                # Add the new column with array values from the source column
                # Then explode the DataFrame on this new column
                # This will create multiple rows, one for each element in the array
                result = df.with_columns(pl.col(resolved_col_name).alias(column_name))

                # Explode the DataFrame on the new column
                result = result.explode(column_name)

                # For regular explode, filter out rows where the exploded value is None
                # (PySpark drops rows with null/empty arrays)
                # For explode_outer, keep all rows (including those with None)
                if not is_explode_outer:
                    # Filter out rows where ExplodedValue is None
                    result = result.filter(pl.col(column_name).is_not_null())

                return result

            # Check if this is a to_timestamp operation and if the input column is a string
            # This helps us choose the right method (str.strptime vs map_elements)
            input_col_dtype = None
            # ColumnOperation is already imported at the top of the file

            if (
                isinstance(expression, ColumnOperation)
                and expression.operation == "to_timestamp"
            ):
                # Check if the input column is a simple Column (direct column reference)
                from sparkless.functions import Column as ColumnType

                if isinstance(expression.column, ColumnType) and not isinstance(
                    expression.column, ColumnOperation
                ):
                    # Check the dtype of the input column in the DataFrame
                    col_name = expression.column.name
                    if col_name in df.columns:
                        input_col_dtype = df[col_name].dtype

                elif isinstance(expression.column, ColumnOperation):
                    # For ColumnOperation chains, check if the result is a string type
                    # This handles cases like regexp_replace().cast("string")
                    col_op = expression.column
                    # Check if it's a cast to string
                    if col_op.operation == "cast":
                        cast_target = col_op.value
                        if isinstance(cast_target, str) and cast_target.lower() in [
                            "string",
                            "varchar",
                        ]:
                            input_col_dtype = pl.Utf8
                    # Check if it's a string operation (regexp_replace, substring, etc.)
                    elif col_op.operation in [
                        "regexp_replace",
                        "substring",
                        "concat",
                        "upper",
                        "lower",
                        "trim",
                        "ltrim",
                        "rtrim",
                    ]:
                        input_col_dtype = pl.Utf8
                    # For nested ColumnOperations, check recursively
                    elif isinstance(col_op.column, ColumnOperation):
                        # Recursively check the inner operation
                        inner_op = col_op.column
                        if inner_op.operation == "cast":
                            cast_target = inner_op.value
                            if isinstance(cast_target, str) and cast_target.lower() in [
                                "string",
                                "varchar",
                            ]:
                                input_col_dtype = pl.Utf8
                        elif inner_op.operation in [
                            "regexp_replace",
                            "substring",
                            "concat",
                            "upper",
                            "lower",
                            "trim",
                            "ltrim",
                            "rtrim",
                        ]:
                            input_col_dtype = pl.Utf8

            # Check if expression is a comparison operation with WindowFunction
            # (e.g., F.row_number().over(w) > 0)
            # Also check for isnull/isnotnull operations on WindowFunction
            is_window_function_comparison = (
                isinstance(expression, ColumnOperation)
                and expression.operation
                in [">", "<", ">=", "<=", "==", "!=", "eqNullSafe"]
                and isinstance(expression.column, WindowFunction)
            )
            is_window_function_isnull = (
                isinstance(expression, ColumnOperation)
                and expression.operation in ["isnull", "isnotnull"]
                and isinstance(expression.column, WindowFunction)
            )

            if is_window_function_comparison or is_window_function_isnull:
                # Handle comparison operations or isnull/isnotnull operations with WindowFunction
                # First, apply the window function to get a temporary column
                window_func = expression.column
                operation = expression.operation
                operation_value = expression.value

                # Apply window function to get a temporary column
                temp_col_name = f"__window_func_temp_{column_name}"
                df_with_window = self.apply_with_column(
                    df, temp_col_name, window_func, expected_field=None
                )

                # Now create the operation expression: temp_col_name op value (or isnull/isnotnull)
                from sparkless.functions.core.column import Column

                temp_col = Column(temp_col_name)
                if operation == ">":
                    operation_expr = temp_col > operation_value
                elif operation == "<":
                    operation_expr = temp_col < operation_value
                elif operation == ">=":
                    operation_expr = temp_col >= operation_value
                elif operation == "<=":
                    operation_expr = temp_col <= operation_value
                elif operation == "==":
                    operation_expr = temp_col == operation_value
                elif operation == "!=":
                    operation_expr = temp_col != operation_value
                elif operation == "eqNullSafe":
                    operation_expr = temp_col.eqNullSafe(operation_value)
                elif operation == "isnull":
                    operation_expr = temp_col.isnull()
                elif operation == "isnotnull":
                    operation_expr = temp_col.isnotnull()
                else:
                    operation_expr = ColumnOperation(
                        temp_col, operation, operation_value
                    )

                # Translate and apply the operation
                case_sensitive = self._get_case_sensitive()
                operation_pl_expr = self.translator.translate(
                    operation_expr,
                    available_columns=list(df_with_window.columns),
                    case_sensitive=case_sensitive,
                )

                # Apply the operation and drop the temporary column
                result_df = (
                    df_with_window.with_columns(
                        pl.col(temp_col_name).alias(column_name)
                    )
                    .with_columns(operation_pl_expr.alias(column_name))
                    .drop(temp_col_name)
                )

                return result_df

            # Check if expression is a CaseWhen that contains WindowFunction comparisons
            from sparkless.functions.conditional import CaseWhen

            if isinstance(expression, CaseWhen):
                # Check if any condition contains a WindowFunction comparison or isnull/isnotnull
                has_window_function_comparison = False
                for condition, _ in expression.conditions:
                    if (
                        isinstance(condition, ColumnOperation)
                        and isinstance(condition.column, WindowFunction)
                        and condition.operation
                        in [
                            ">",
                            "<",
                            ">=",
                            "<=",
                            "==",
                            "!=",
                            "eqNullSafe",
                            "isnull",
                            "isnotnull",
                        ]
                    ):
                        has_window_function_comparison = True
                        break

                if has_window_function_comparison:
                    # Handle CaseWhen with WindowFunction comparisons
                    # First, apply all window functions to get temporary columns
                    temp_cols: Dict[int, str] = {}
                    current_df = df
                    for condition, _ in expression.conditions:
                        if isinstance(condition, ColumnOperation) and isinstance(
                            condition.column, WindowFunction
                        ):
                            operation = condition.operation
                            if operation in [
                                ">",
                                "<",
                                ">=",
                                "<=",
                                "==",
                                "!=",
                                "eqNullSafe",
                                "isnull",
                                "isnotnull",
                            ]:
                                window_func = condition.column
                                # Create a unique temp column name
                                temp_col_name = f"__window_func_temp_{len(temp_cols)}"
                                temp_cols[id(window_func)] = temp_col_name
                                # Apply window function
                                current_df = self.apply_with_column(
                                    current_df,
                                    temp_col_name,
                                    window_func,
                                    expected_field=None,
                                )

                    # Now replace WindowFunction comparisons with column comparisons
                    from sparkless.functions.core.column import Column

                    new_conditions = []
                    for condition, value in expression.conditions:
                        if isinstance(condition, ColumnOperation) and isinstance(
                            condition.column, WindowFunction
                        ):
                            operation = condition.operation
                            if operation in [
                                ">",
                                "<",
                                ">=",
                                "<=",
                                "==",
                                "!=",
                                "eqNullSafe",
                                "isnull",
                                "isnotnull",
                            ]:
                                window_func = condition.column
                                temp_col_name = temp_cols[id(window_func)]
                                temp_col = Column(temp_col_name)
                                operation_value = condition.value

                                # Create new operation expression
                                if operation == ">":
                                    new_condition = temp_col > operation_value
                                elif operation == "<":
                                    new_condition = temp_col < operation_value
                                elif operation == ">=":
                                    new_condition = temp_col >= operation_value
                                elif operation == "<=":
                                    new_condition = temp_col <= operation_value
                                elif operation == "==":
                                    new_condition = temp_col == operation_value
                                elif operation == "!=":
                                    new_condition = temp_col != operation_value
                                elif operation == "eqNullSafe":
                                    new_condition = temp_col.eqNullSafe(operation_value)
                                elif operation == "isnull":
                                    new_condition = temp_col.isnull()
                                elif operation == "isnotnull":
                                    new_condition = temp_col.isnotnull()
                                else:
                                    # This branch should never be reached due to the if check above
                                    # But include it for completeness
                                    # For other operations, create ColumnOperation
                                    # operation_value can be None for unary operations
                                    # ColumnOperation accepts Any for value parameter (including None)
                                    op_str: str = operation  # type: ignore[assignment]
                                    new_condition = ColumnOperation(
                                        temp_col, op_str, None, name=None
                                    )
                                new_conditions.append((new_condition, value))
                            else:
                                new_conditions.append((condition, value))
                        else:
                            new_conditions.append((condition, value))

                    # Create new CaseWhen with replaced conditions
                    new_case_when = CaseWhen()
                    new_case_when.conditions = new_conditions
                    new_case_when.default_value = expression.default_value

                    # Translate the new CaseWhen
                    case_sensitive = self._get_case_sensitive()
                    expr = self.translator.translate(
                        new_case_when,
                        available_columns=list(current_df.columns),
                        case_sensitive=case_sensitive,
                    )

                    # Apply the expression and drop temporary columns
                    result_df = current_df.with_columns(expr.alias(column_name))
                    if temp_cols:
                        result_df = result_df.drop(list(temp_cols.values()))
                    return result_df

            # Check if expression is a Column with struct field path (e.g., "StructVal.E1")
            # This needs special handling to use Polars struct.field() syntax
            from sparkless.functions.core.column import Column

            if isinstance(expression, Column) and "." in expression.name:
                # Handle struct field access
                struct_field_path = expression.name
                parts = struct_field_path.split(".", 1)
                struct_col = parts[0]
                field_name = parts[1]

                # Resolve struct column name case-insensitively
                from ...core.column_resolver import ColumnResolver

                case_sensitive = self._get_case_sensitive()
                resolved_struct_col = ColumnResolver.resolve_column_name(
                    struct_col, list(df.columns), case_sensitive
                )
                if resolved_struct_col and resolved_struct_col in df.columns:
                    # Get struct dtype
                    struct_dtype = df[resolved_struct_col].dtype
                    if hasattr(struct_dtype, "fields") and struct_dtype.fields:
                        # Resolve field name case-insensitively within struct
                        field_names = [f.name for f in struct_dtype.fields]
                        resolved_field = ColumnResolver.resolve_column_name(
                            field_name, field_names, case_sensitive
                        )
                        if resolved_field:
                            # Use Polars struct.field() syntax for nested access
                            expr = pl.col(resolved_struct_col).struct.field(
                                resolved_field
                            )
                            return df.with_columns(expr.alias(column_name))

            try:
                # Pass case sensitivity for column resolution
                case_sensitive = self._get_case_sensitive()
                expr = self.translator.translate(
                    expression,
                    input_col_dtype=input_col_dtype,
                    available_columns=list(df.columns),
                    case_sensitive=case_sensitive,
                )
            except ValueError as e:
                # Fallback to Python evaluation for unsupported operations (e.g., withField, + with strings, WindowFunction)
                error_msg = str(e)
                # Check if this is a WindowFunction comparison that should be handled
                is_window_function_comparison_fallback = (
                    isinstance(expression, ColumnOperation)
                    and expression.operation
                    in [">", "<", ">=", "<=", "==", "!=", "eqNullSafe"]
                    and isinstance(expression.column, WindowFunction)
                ) or (
                    "WindowFunction comparison" in error_msg
                    and isinstance(expression, ColumnOperation)
                    and expression.operation
                    in [">", "<", ">=", "<=", "==", "!=", "eqNullSafe"]
                )

                if is_window_function_comparison_fallback:
                    # Handle comparison operations with WindowFunction
                    # First, apply the window function to get a temporary column
                    window_func = expression.column
                    comparison_op = expression.operation
                    comparison_value = expression.value

                    # Apply window function to get a temporary column
                    temp_col_name = f"__window_func_temp_{column_name}"
                    df_with_window = self.apply_with_column(
                        df, temp_col_name, window_func, expected_field=None
                    )

                    # Now create a comparison expression: temp_col_name op value
                    from sparkless.functions.core.column import Column

                    temp_col = Column(temp_col_name)
                    if comparison_op == ">":
                        comparison_expr = temp_col > comparison_value
                    elif comparison_op == "<":
                        comparison_expr = temp_col < comparison_value
                    elif comparison_op == ">=":
                        comparison_expr = temp_col >= comparison_value
                    elif comparison_op == "<=":
                        comparison_expr = temp_col <= comparison_value
                    elif comparison_op == "==":
                        comparison_expr = temp_col == comparison_value
                    elif comparison_op == "!=":
                        comparison_expr = temp_col != comparison_value
                    elif comparison_op == "eqNullSafe":
                        comparison_expr = temp_col.eqNullSafe(comparison_value)
                    else:
                        comparison_expr = ColumnOperation(
                            temp_col, comparison_op, comparison_value
                        )

                    # Translate and apply the comparison
                    case_sensitive = self._get_case_sensitive()
                    comparison_pl_expr = self.translator.translate(
                        comparison_expr,
                        available_columns=list(df_with_window.columns),
                        case_sensitive=case_sensitive,
                    )

                    # Apply the comparison and drop the temporary column
                    result_df = (
                        df_with_window.with_columns(
                            pl.col(temp_col_name).alias(column_name)
                        )
                        .with_columns(comparison_pl_expr.alias(column_name))
                        .drop(temp_col_name)
                    )

                    return result_df

                # Check if this is a WindowFunction cast that should be handled above
                # Check both the expression structure and the error message
                is_window_function_cast_fallback = (
                    isinstance(expression, ColumnOperation)
                    and expression.operation == "cast"
                    and isinstance(expression.column, WindowFunction)
                ) or (
                    "WindowFunction" in error_msg
                    and isinstance(expression, ColumnOperation)
                    and expression.operation == "cast"
                )

                if is_window_function_cast_fallback:
                    # This should have been caught above, but handle it here as fallback
                    # This is the same logic as above, but as a safety net
                    # Extract WindowFunction and cast type
                    window_func = expression.column
                    cast_type = expression.value

                    # Window functions need sorting - reuse the same logic from above
                    window_spec = window_func.window_spec
                    sort_cols = []
                    has_order_by = (
                        hasattr(window_spec, "_order_by") and window_spec._order_by
                    )
                    has_partition_by = (
                        hasattr(window_spec, "_partition_by")
                        and window_spec._partition_by
                    )

                    # Build sort columns
                    if has_order_by:
                        if has_partition_by:
                            for col in window_spec._partition_by:
                                if isinstance(col, str):
                                    sort_cols.append(col)
                                elif hasattr(col, "name"):
                                    sort_cols.append(col.name)
                        for col in window_spec._order_by:
                            if isinstance(col, str):
                                sort_cols.append(col)
                            elif hasattr(col, "name"):
                                sort_cols.append(col.name)

                    # Sort DataFrame if needed for window function evaluation
                    # Window functions need sorted data for correct evaluation
                    df_sorted = (
                        df.sort(sort_cols)
                        if sort_cols and all(isinstance(c, str) for c in sort_cols)
                        else df
                    )

                    # Evaluate window function on sorted data
                    # WindowFunction.evaluate() handles sorting internally, but we need to sort first for correctness
                    data = df_sorted.to_dicts()

                    results = window_func.evaluate(data)

                    # Apply cast
                    if cast_type is not None:
                        from sparkless.dataframe.casting.type_converter import (
                            TypeConverter,
                        )
                        from sparkless.spark_types import (
                            StringType,
                            IntegerType,
                            LongType,
                            DoubleType,
                            FloatType,
                            BooleanType,
                            DateType,
                            TimestampType,
                            ShortType,
                            ByteType,
                        )

                        # Handle string type names
                        if isinstance(cast_type, str):
                            type_name_map = {
                                "string": StringType(),
                                "str": StringType(),
                                "int": IntegerType(),
                                "integer": IntegerType(),
                                "long": LongType(),
                                "bigint": LongType(),
                                "double": DoubleType(),
                                "float": FloatType(),
                                "boolean": BooleanType(),
                                "bool": BooleanType(),
                                "date": DateType(),
                                "timestamp": TimestampType(),
                                "short": ShortType(),
                                "byte": ByteType(),
                            }
                            cast_type = type_name_map.get(cast_type.lower())

                        if cast_type is not None:
                            results = [
                                TypeConverter.cast_to_type(r, cast_type)
                                if r is not None
                                else None
                                for r in results
                            ]

                    # Add results as new column to sorted DataFrame
                    # Results are in the same order as df_sorted
                    result = df_sorted.with_columns(pl.Series(column_name, results))

                    # If we sorted, we need to restore original order
                    # Create a row identifier to match back to original DataFrame
                    if df_sorted is not df and sort_cols:
                        # Use all columns as join keys to match rows
                        join_cols = list(df.columns)
                        # Add row number to both DataFrames for matching
                        original_with_row_num = df.with_row_count("__original_index__")
                        result_with_row_num = result.with_row_count("__sorted_index__")

                        # Join to restore original order
                        result_joined = (
                            original_with_row_num.join(
                                result_with_row_num.select(
                                    join_cols + [column_name, "__sorted_index__"]
                                ),
                                on=join_cols,
                                how="left",
                            )
                            .select(join_cols + [column_name])
                            .drop("__original_index__")
                        )

                        return result_joined

                    return result
                elif (
                    "withField" in error_msg
                    or (
                        isinstance(expression, ColumnOperation)
                        and expression.operation == "withField"
                    )
                    or "+ operation requires Python evaluation" in error_msg
                    or "format_string operation requires Python evaluation" in error_msg
                    or "array function requires Python evaluation" in error_msg
                ):
                    # Convert Polars DataFrame to list of dicts for Python evaluation
                    data = df.to_dicts()
                    # Evaluate using ExpressionEvaluator
                    from sparkless.dataframe.evaluation.expression_evaluator import (
                        ExpressionEvaluator,
                    )

                    # Get dataframe context from expression if available (for case sensitivity)
                    dataframe_context = None
                    if hasattr(expression, "_dataframe_context"):
                        dataframe_context = expression._dataframe_context
                    elif hasattr(expression, "column") and hasattr(
                        expression.column, "_dataframe_context"
                    ):
                        dataframe_context = expression.column._dataframe_context

                    evaluator = ExpressionEvaluator(dataframe_context=dataframe_context)
                    results = [
                        evaluator.evaluate_expression(row, expression) for row in data
                    ]
                    # Add results as new column
                    # Polars will automatically infer struct type from dict values
                    # Create Series - use strict=False if available to handle mixed types
                    # This is needed for conditional expressions and other complex cases
                    # For array() function, results are lists which may contain mixed types
                    # Use Object dtype for array() to handle mixed types correctly
                    is_array_function = (
                        isinstance(expression, ColumnOperation)
                        and expression.operation == "array"
                    )
                    try:
                        if is_array_function:
                            # Array function may have mixed types - use Object dtype
                            result_series = pl.Series(
                                column_name, results, dtype=pl.Object
                            )
                        else:
                            # Try with strict=False first (Polars 0.19+)
                            result_series = pl.Series(
                                column_name, results, strict=False
                            )
                    except TypeError:
                        # Fallback: try without strict parameter or use Object dtype
                        try:
                            result_series = pl.Series(column_name, results)
                        except TypeError:
                            # Last resort: use Object dtype explicitly
                            result_series = pl.Series(
                                column_name, results, dtype=pl.Object
                            )
                    # Replace the column (if it exists) or add new column
                    result = df.with_columns(result_series)
                    return result
                else:
                    # Re-raise if it's not a withField error
                    raise

            # If expected_field is provided, use it to explicitly cast the result
            # This fixes issue #151 where Polars was expecting String but got datetime
            # for to_timestamp() operations
            if expected_field is not None:
                from sparkless.spark_types import TimestampType
                from .type_mapper import mock_type_to_polars_dtype

                # Check if the expected type is TimestampType
                if isinstance(expected_field.dataType, TimestampType):
                    # Explicitly cast to pl.Datetime to ensure Polars recognizes the correct type
                    # This is critical for to_timestamp operations to avoid schema validation errors
                    polars_dtype = mock_type_to_polars_dtype(expected_field.dataType)
                    # Cast immediately to ensure type is correct before any operations
                    expr = expr.cast(polars_dtype)

            # Apply with_columns - with schema inference fix, this should work correctly
            # The expression translator already handles cast operations correctly
            # For to_timestamp operations with TimestampType expected_field, evaluate eagerly
            # and use hstack to add column without creating lazy frame
            if expected_field is not None:
                from sparkless.spark_types import TimestampType

                if isinstance(expected_field.dataType, TimestampType):
                    # Evaluate the expression eagerly and add as Series to avoid lazy validation
                    # This avoids Polars' lazy frame schema validation that checks input types
                    try:
                        # For to_timestamp, use with_columns directly with explicit cast
                        # The cast ensures Polars recognizes the output type before validation
                        from .type_mapper import mock_type_to_polars_dtype

                        polars_dtype = mock_type_to_polars_dtype(
                            expected_field.dataType
                        )
                        # Cast the expression to the expected type before using with_columns
                        # Use strict=False to handle edge cases gracefully
                        cast_expr = expr.cast(polars_dtype, strict=False)
                        # Use with_columns - the cast should prevent validation errors
                        result = df.with_columns([cast_expr.alias(column_name)])
                        return result
                    except Exception:
                        pass  # Fall through to with_columns

            # For to_date() operations on datetime columns, use .dt.date() directly
            # This avoids schema validation issues that map_elements can cause
            if (
                isinstance(expression, ColumnOperation)
                and expression.operation == "to_date"
            ):
                from sparkless.functions.core.column import Column

                # Check if the input column is a simple Column reference (not a ColumnOperation)
                input_col = expression.column
                if isinstance(input_col, Column) and not isinstance(
                    input_col, ColumnOperation
                ):
                    # Simple column reference - check if it's a datetime type in the DataFrame
                    col_name = input_col.name
                    if col_name in df.columns:
                        # Check the actual Polars dtype
                        col_dtype = df[col_name].dtype
                        is_datetime = (
                            isinstance(col_dtype, pl.Datetime)
                            or str(col_dtype).startswith("Datetime")
                            or (hasattr(pl, "Datetime") and col_dtype == pl.Datetime)
                        )
                        is_date = col_dtype == pl.Date

                        if is_datetime or is_date:
                            # For datetime/date columns, use .dt.date() directly
                            # This avoids schema validation issues
                            try:
                                if expression.value is None:
                                    # No format - use .dt.date() for datetime/date columns
                                    date_expr = pl.col(col_name).dt.date()
                                    result = df.with_columns(
                                        date_expr.alias(column_name)
                                    )
                                    return result
                                else:
                                    # With format - still need to use map_elements for string parsing
                                    # But try select to avoid validation
                                    all_exprs = [pl.col(c) for c in df.columns] + [
                                        expr.alias(column_name)
                                    ]
                                    result = df.select(all_exprs)
                                    return result
                            except Exception:
                                pass  # Fall back to with_columns

                # For complex expressions or string columns, try using select to avoid validation
                try:
                    # Use select to avoid schema validation issues
                    # This works for both StringType and TimestampType inputs
                    all_exprs = [pl.col(c) for c in df.columns] + [
                        expr.alias(column_name)
                    ]
                    result = df.select(all_exprs)
                    return result
                except Exception:
                    pass  # Fall through to with_columns

            # Try to execute with Polars, but catch ColumnNotFoundError for Python evaluation fallback
            try:
                result = df.with_columns(expr.alias(column_name))
                return result
            except pl.exceptions.ColumnNotFoundError as e:
                # Polars couldn't find a column - this might be a case sensitivity issue
                # Fall back to Python evaluation
                error_msg = str(e)
                # Check if this looks like a case sensitivity issue
                # (column name exists but with different case)
                missing_col_match = None
                for col in df.columns:
                    # Check if the missing column name matches any existing column case-insensitively
                    if col.lower() in error_msg.lower() or any(
                        missing.lower() == col.lower()
                        for missing in error_msg.split('"')
                        if missing and missing.strip()
                    ):
                        missing_col_match = col
                        break

                if missing_col_match:
                    # Column exists but with different case - use Python evaluation
                    data = df.to_dicts()
                    from sparkless.dataframe.evaluation.expression_evaluator import (
                        ExpressionEvaluator,
                    )

                    # Get dataframe context from expression if available (for case sensitivity)
                    dataframe_context = None
                    if hasattr(expression, "_dataframe_context"):
                        dataframe_context = expression._dataframe_context
                    elif hasattr(expression, "column") and hasattr(
                        expression.column, "_dataframe_context"
                    ):
                        dataframe_context = expression.column._dataframe_context

                    evaluator = ExpressionEvaluator(dataframe_context=dataframe_context)
                    results = [
                        evaluator.evaluate_expression(row, expression) for row in data
                    ]
                    try:
                        result_series = pl.Series(column_name, results, strict=False)
                    except TypeError:
                        try:
                            result_series = pl.Series(column_name, results)
                        except TypeError:
                            result_series = pl.Series(
                                column_name, results, dtype=pl.Object
                            )
                    result = df.with_columns(result_series)
                    return result
                else:
                    # Re-raise if it's not a case sensitivity issue
                    raise

    def _coerce_join_key_types(
        self,
        df1: pl.DataFrame,
        df2: pl.DataFrame,
        join_keys: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
    ) -> Tuple[
        pl.DataFrame,
        pl.DataFrame,
        Optional[List[str]],
        Optional[List[str]],
        Optional[List[str]],
    ]:
        """Coerce join key types to match if needed (numeric vs string).

        PySpark allows joining on columns with different types (e.g., i64 vs str),
        automatically casting string keys to numeric. This method replicates that behavior.

        Args:
            df1: Left DataFrame
            df2: Right DataFrame
            join_keys: List of column names for on= joins (both DataFrames use same column names)
            left_on: List of column names from df1 for left_on/right_on joins
            right_on: List of column names from df2 for left_on/right_on joins

        Returns:
            Tuple of (casted_df1, casted_df2, updated_join_keys, updated_left_on, updated_right_on)
        """
        # Define numeric types
        numeric_types = (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Float32,
            pl.Float64,
        )
        string_type = pl.Utf8

        cast_exprs_df1 = []
        cast_exprs_df2 = []
        result_df1 = df1
        result_df2 = df2

        # Handle on= joins (same column names in both DataFrames)
        if join_keys is not None:
            for key in join_keys:
                if key not in df1.columns or key not in df2.columns:
                    continue  # Skip if column doesn't exist (will error later)

                dtype1 = df1[key].dtype
                dtype2 = df2[key].dtype

                # If types match, no coercion needed
                if dtype1 == dtype2:
                    continue

                # Check if one is numeric and one is string
                is_numeric1 = dtype1 in numeric_types
                is_numeric2 = dtype2 in numeric_types
                is_string1 = dtype1 == string_type
                is_string2 = dtype2 == string_type

                if (is_numeric1 and is_string2) or (is_string1 and is_numeric2):
                    # One is numeric, one is string - cast string to numeric
                    if is_numeric1 and is_string2:
                        # df2[key] is string, cast to df1[key]'s numeric type
                        cast_exprs_df2.append(pl.col(key).cast(dtype1, strict=False))
                    else:
                        # df1[key] is string, cast to df2[key]'s numeric type
                        cast_exprs_df1.append(pl.col(key).cast(dtype2, strict=False))
                elif is_numeric1 and is_numeric2:
                    # Both numeric but different types - prefer larger type
                    # Int8 < Int16 < Int32 < Int64, Float32 < Float64
                    # Prefer Int64 over Int32, Float64 over Float32
                    # Prefer Float over Int
                    if isinstance(dtype1, (pl.Float32, pl.Float64)) or isinstance(
                        dtype2, (pl.Float32, pl.Float64)
                    ):
                        # At least one is float, use Float64
                        target_dtype = pl.Float64
                    else:
                        # Both integers, use Int64
                        target_dtype = pl.Int64

                    if dtype1 != target_dtype:
                        cast_exprs_df1.append(
                            pl.col(key).cast(target_dtype, strict=False)
                        )
                    if dtype2 != target_dtype:
                        cast_exprs_df2.append(
                            pl.col(key).cast(target_dtype, strict=False)
                        )
                else:
                    # Types can't be coerced (e.g., boolean vs string, date vs string)
                    raise ValueError(
                        f"Cannot join on column '{key}' with incompatible types: "
                        f"left={dtype1}, right={dtype2}. "
                        f"PySpark only supports joining numeric types with string types."
                    )

        # Handle left_on/right_on joins (different column names)
        elif left_on is not None and right_on is not None:
            if len(left_on) != len(right_on):
                raise ValueError(
                    f"left_on and right_on must have the same length: "
                    f"left_on={left_on}, right_on={right_on}"
                )

            for i, (left_key, right_key) in enumerate(zip(left_on, right_on)):
                if left_key not in df1.columns or right_key not in df2.columns:
                    continue  # Skip if column doesn't exist (will error later)

                dtype1 = df1[left_key].dtype
                dtype2 = df2[right_key].dtype

                # If types match, no coercion needed
                if dtype1 == dtype2:
                    continue

                # Check if one is numeric and one is string
                is_numeric1 = dtype1 in numeric_types
                is_numeric2 = dtype2 in numeric_types
                is_string1 = dtype1 == string_type
                is_string2 = dtype2 == string_type

                if (is_numeric1 and is_string2) or (is_string1 and is_numeric2):
                    # One is numeric, one is string - cast string to numeric
                    if is_numeric1 and is_string2:
                        # df2[right_key] is string, cast to df1[left_key]'s numeric type
                        cast_exprs_df2.append(
                            pl.col(right_key).cast(dtype1, strict=False)
                        )
                    else:
                        # df1[left_key] is string, cast to df2[right_key]'s numeric type
                        cast_exprs_df1.append(
                            pl.col(left_key).cast(dtype2, strict=False)
                        )
                elif is_numeric1 and is_numeric2:
                    # Both numeric but different types - prefer larger type
                    if isinstance(dtype1, (pl.Float32, pl.Float64)) or isinstance(
                        dtype2, (pl.Float32, pl.Float64)
                    ):
                        target_dtype = pl.Float64
                    else:
                        target_dtype = pl.Int64

                    if dtype1 != target_dtype:
                        cast_exprs_df1.append(
                            pl.col(left_key).cast(target_dtype, strict=False)
                        )
                    if dtype2 != target_dtype:
                        cast_exprs_df2.append(
                            pl.col(right_key).cast(target_dtype, strict=False)
                        )
                else:
                    # Types can't be coerced
                    raise ValueError(
                        f"Cannot join on columns '{left_key}' (left) and '{right_key}' (right) "
                        f"with incompatible types: left={dtype1}, right={dtype2}. "
                        f"PySpark only supports joining numeric types with string types."
                    )

        # Apply casts if any
        if cast_exprs_df1:
            result_df1 = df1.with_columns(cast_exprs_df1)
        if cast_exprs_df2:
            result_df2 = df2.with_columns(cast_exprs_df2)

        return result_df1, result_df2, join_keys, left_on, right_on

    @profiled("polars.apply_join", category="polars")
    def apply_join(
        self,
        df1: pl.DataFrame,
        df2: pl.DataFrame,
        on: Optional[Union[str, List[str], ColumnOperation]] = None,
        how: str = "inner",
    ) -> pl.DataFrame:
        """Apply a join operation.

        Args:
            df1: Left DataFrame
            df2: Right DataFrame
            on: Join key(s) - column name(s), list of column names, or ColumnOperation with ==
            how: Join type ("inner", "left", "right", "outer", "cross", "semi", "anti")

        Returns:
            Joined DataFrame
        """
        # Extract column names from join condition if it's a ColumnOperation
        join_keys: Optional[List[str]] = None
        left_on: Optional[List[str]] = None
        right_on: Optional[List[str]] = None
        expression_condition: Optional[ColumnOperation] = None

        if isinstance(on, ColumnOperation):
            operation = getattr(on, "operation", None)
            if operation in ("==", "eqNullSafe"):
                # Equality-based join - extract column names
                if not hasattr(on, "column") or not hasattr(on, "value"):
                    raise ValueError(
                        "Join condition must have column and value attributes"
                    )
                left_col = (
                    on.column.name if hasattr(on.column, "name") else str(on.column)
                )
                right_col = (
                    on.value.name if hasattr(on.value, "name") else str(on.value)
                )
                left_col_str = str(left_col)
                right_col_str = str(right_col)

                # Always use left_on/right_on for different column names
                # This ensures proper column matching when columns have different names
                if left_col_str == right_col_str:
                    # Same column name - check if it exists in both DataFrames
                    if left_col_str in df1.columns and left_col_str in df2.columns:
                        join_keys = [left_col_str]
                    else:
                        # Column name doesn't match - use left_on/right_on anyway
                        left_on = [left_col_str]
                        right_on = [right_col_str]
                else:
                    # Different column names - always use left_on and right_on
                    left_on = [left_col_str]
                    right_on = [right_col_str]
            else:
                # Expression-based join (e.g., array_contains, other expressions)
                # Store the expression for evaluation after cross join
                expression_condition = on
        elif on is None:
            common_cols = set(df1.columns) & set(df2.columns)
            if not common_cols:
                raise ValueError("No common columns found for join")
            join_keys = list(common_cols)
        elif isinstance(on, str):
            join_keys = [on]
        elif isinstance(on, list):
            join_keys = list(on)
        else:
            raise ValueError("Join keys must be column name(s) or a ColumnOperation")

        # Map join types
        join_type_map = {
            "inner": "inner",
            "left": "left",
            "right": "right",
            "outer": "outer",
            "full": "outer",
            "full_outer": "outer",
            "cross": "cross",
        }

        polars_how = join_type_map.get(how.lower(), "inner")

        # Handle expression-based joins (e.g., array_contains)
        if expression_condition is not None:
            return self._apply_expression_join(
                df1, df2, expression_condition, polars_how, how.lower()
            )

        # Resolve join_keys using ColumnResolver if they are strings
        case_sensitive = self._get_case_sensitive()
        resolved_join_keys = None
        left_on_keys = []
        right_on_keys = []
        use_left_right_on = False
        if join_keys is not None:
            resolved_join_keys = []
            for col in join_keys:
                if isinstance(col, str):
                    # Resolve column name using ColumnResolver in both DataFrames
                    actual_col_df1 = self._find_column(df1, col, case_sensitive)
                    actual_col_df2 = self._find_column(df2, col, case_sensitive)
                    if actual_col_df1 is None or actual_col_df2 is None:
                        # Will be handled in the else branch below
                        resolved_join_keys.append(col)
                    elif actual_col_df1 != actual_col_df2:
                        # Column names differ - need to use left_on/right_on
                        use_left_right_on = True
                        left_on_keys.append(actual_col_df1)
                        right_on_keys.append(actual_col_df2)
                        # Don't add to resolved_join_keys when using left_on/right_on
                    else:
                        # Same column name, can use on=
                        resolved_join_keys.append(actual_col_df1)
                # Note: join_keys is List[str], so all items are strings
                # Non-string items would be handled above

        # Coerce join key types if needed (e.g., numeric vs string)
        # PySpark allows joining on columns with different types, casting string to numeric
        # Coerce types after resolving column names (case-insensitively)
        if use_left_right_on and left_on_keys and right_on_keys:
            # Use left_on/right_on for coercion
            df1, df2, _, coerced_left_on, coerced_right_on = (
                self._coerce_join_key_types(df1, df2, None, left_on_keys, right_on_keys)
            )
            left_on_keys = (
                coerced_left_on if coerced_left_on is not None else left_on_keys
            )
            right_on_keys = (
                coerced_right_on if coerced_right_on is not None else right_on_keys
            )
        elif resolved_join_keys:
            # Use resolved_join_keys for coercion
            df1, df2, coerced_join_keys, _, _ = self._coerce_join_key_types(
                df1, df2, resolved_join_keys, None, None
            )
            resolved_join_keys = (
                coerced_join_keys
                if coerced_join_keys is not None
                else resolved_join_keys
            )
        elif join_keys:
            # Fallback to original join_keys
            df1, df2, coerced_join_keys, _, _ = self._coerce_join_key_types(
                df1, df2, join_keys, None, None
            )
            if coerced_join_keys is not None:
                resolved_join_keys = coerced_join_keys

        # Handle semi and anti joins (Polars doesn't support natively)
        if how.lower() in ("semi", "left_semi"):
            # Semi join: return rows from left where match exists in right
            # Do inner join, then select only left columns and distinct
            if use_left_right_on and left_on_keys and right_on_keys:
                joined = df1.join(
                    df2, left_on=left_on_keys, right_on=right_on_keys, how="inner"
                )
            elif resolved_join_keys:
                joined = df1.join(df2, on=resolved_join_keys, how="inner")
            else:
                joined = df1.join(df2, on=join_keys, how="inner")
            # Select only columns from df1 (preserve original column order)
            left_cols = [col for col in df1.columns if col in joined.columns]
            return joined.select(left_cols).unique()
        elif how.lower() in ("anti", "left_anti"):
            # Anti join: return rows from left where no match exists in right
            # Do left join, then filter where right columns are null
            if use_left_right_on and left_on_keys and right_on_keys:
                joined = df1.join(
                    df2, left_on=left_on_keys, right_on=right_on_keys, how="left"
                )
            elif resolved_join_keys:
                joined = df1.join(df2, on=resolved_join_keys, how="left")
            else:
                joined = df1.join(df2, on=join_keys, how="left")
            # Find right-side columns (columns in df2 but not in df1)
            right_cols = [col for col in df2.columns if col not in df1.columns]
            if right_cols:
                # Filter where any right column is null
                filter_expr = pl.col(right_cols[0]).is_null()
                for col in right_cols[1:]:
                    filter_expr = filter_expr | pl.col(col).is_null()
                joined = joined.filter(filter_expr)
            else:
                # If no right columns (all match left), check if join key exists
                # This case shouldn't happen, but handle it
                keys_to_use = resolved_join_keys if resolved_join_keys else join_keys
                if keys_to_use is not None and len(keys_to_use) > 0:
                    joined = joined.filter(pl.col(keys_to_use[0]).is_null())
            # Select only columns from df1
            left_cols = [col for col in joined.columns if col in df1.columns]
            return joined.select(left_cols)
        elif polars_how == "cross":
            return df1.join(df2, how="cross")
        else:
            # Handle different column names with left_on/right_on
            if left_on is not None and right_on is not None:
                # Validate right_on is not empty
                if not right_on:
                    raise ValueError(
                        f"Join right_on is empty. left_on={left_on}, right_on={right_on}, "
                        f"df1.columns={df1.columns}, df2.columns={df2.columns}"
                    )

                # Verify columns exist using ColumnResolver
                resolved_left_on = []
                for col in left_on:
                    actual_col = self._find_column(df1, col, case_sensitive)
                    if actual_col is None:
                        raise ValueError(
                            f"Join column '{col}' not found in left DataFrame. Available columns: {df1.columns}"
                        )
                    resolved_left_on.append(actual_col)
                resolved_right_on = []
                for col in right_on:
                    actual_col = self._find_column(df2, col, case_sensitive)
                    if actual_col is None:
                        raise ValueError(
                            f"Join column '{col}' not found in right DataFrame. Available columns: {df2.columns}"
                        )
                    resolved_right_on.append(actual_col)

                # Coerce join key types if needed (for left_on/right_on case)
                df1, df2, _, coerced_left_on, coerced_right_on = (
                    self._coerce_join_key_types(
                        df1, df2, None, resolved_left_on, resolved_right_on
                    )
                )
                if coerced_left_on is not None:
                    resolved_left_on = coerced_left_on
                if coerced_right_on is not None:
                    resolved_right_on = coerced_right_on

                # Polars join with left_on/right_on doesn't include right_on column
                # But PySpark includes both columns, so we need to add it back
                joined = df1.join(
                    df2,
                    left_on=resolved_left_on,
                    right_on=resolved_right_on,
                    how=polars_how,
                )
                # Add the right_on column back if it's not already present (PySpark includes both)
                for right_col in resolved_right_on:
                    if right_col not in joined.columns:
                        # Get the corresponding left column value (they should be equal after join)
                        left_col = resolved_left_on[resolved_right_on.index(right_col)]
                        joined = joined.with_columns(pl.col(left_col).alias(right_col))
                return joined
            else:
                # Verify columns exist in both DataFrames using ColumnResolver
                # Check if we already resolved with left_on/right_on in the earlier block
                if use_left_right_on and left_on_keys and right_on_keys:
                    # Use left_on/right_on when column names differ (from earlier resolution)
                    joined = df1.join(
                        df2,
                        left_on=left_on_keys,
                        right_on=right_on_keys,
                        how=polars_how,
                    )
                    # Add the right_on columns back if needed (PySpark includes both)
                    for right_col in right_on_keys:
                        if right_col not in joined.columns:
                            left_col = left_on_keys[right_on_keys.index(right_col)]
                            joined = joined.with_columns(
                                pl.col(left_col).alias(right_col)
                            )
                    return joined
                elif resolved_join_keys and len(resolved_join_keys) > 0:
                    # Use resolved_join_keys from earlier resolution
                    return df1.join(df2, on=resolved_join_keys, how=polars_how)
                elif resolved_join_keys is None:
                    # Need to resolve join_keys
                    # If resolved_join_keys is None, join_keys must have been None
                    # (since we set resolved_join_keys = [] when join_keys is not None)
                    raise ValueError("Join keys must be specified")
                else:
                    # resolved_join_keys is empty list, fallback to original join_keys
                    return df1.join(df2, on=join_keys, how=polars_how)

    def _apply_expression_join(
        self,
        df1: pl.DataFrame,
        df2: pl.DataFrame,
        condition: ColumnOperation,
        polars_how: str,
        how: str,
    ) -> pl.DataFrame:
        """Apply join with expression-based condition (e.g., array_contains).

        For expression-based joins, we:
        1. Do a cross join to get all row combinations
        2. Evaluate the expression condition for each row pair
        3. Filter based on the expression result
        4. Handle join types appropriately

        Args:
            df1: Left DataFrame
            df2: Right DataFrame
            condition: ColumnOperation expression to evaluate (e.g., array_contains)
            polars_how: Polars join type string
            how: Original join type string

        Returns:
            Joined DataFrame
        """
        # Step 1: Do cross join to get all row combinations
        # Check for column name conflicts and prefix df2 columns if needed
        df1_cols = set(df1.columns)
        df2_cols = set(df2.columns)
        has_conflicts = bool(df1_cols & df2_cols)

        if has_conflicts:
            # Prefix right DataFrame columns to avoid conflicts
            df2_prefixed = df2.select(
                [pl.col(col).alias(f"_right_{col}") for col in df2.columns]
            )
            cross_joined = df1.join(df2_prefixed, how="cross")
            available_columns = list(df1.columns) + list(df2_prefixed.columns)
        else:
            # No conflicts - can use original column names
            cross_joined = df1.join(df2, how="cross")
            available_columns = list(df1.columns) + list(df2.columns)

        # Step 2: Translate and evaluate the condition expression
        case_sensitive = self._get_case_sensitive()

        # If there are column name conflicts, we need to map df2 column references
        # to prefixed names. Create a modified condition that uses prefixed column names.
        condition_to_translate = condition
        if has_conflicts and isinstance(condition, ColumnOperation):
            # For array_contains(df1.IDs, df2.ID), we need to replace df2.ID with _right_ID
            # Create a new condition with prefixed df2 column references
            from sparkless.functions.core.column import Column

            if condition.operation == "array_contains":
                # array_contains(column, value)
                # column is from df1 (keep as is), value might be from df2 (needs prefix)
                original_column = condition.column
                original_value = condition.value

                # Check if value is a Column from df2 that needs prefixing
                modified_value = original_value
                if isinstance(original_value, Column):
                    col_name = original_value.name
                    if col_name in df2.columns:
                        # This is a df2 column - use prefixed name
                        prefixed_name = f"_right_{col_name}"
                        modified_value = Column(prefixed_name)
                elif (
                    isinstance(original_value, ColumnOperation)
                    and hasattr(original_value, "column")
                    and isinstance(original_value.column, Column)
                ):
                    # Nested ColumnOperation - check if column needs prefixing
                    # operation is always a string for ColumnOperation instances
                    op = getattr(original_value, "operation", None)
                    if op is not None:
                        col_name = original_value.column.name
                        if col_name in df2.columns:
                            prefixed_name = f"_right_{col_name}"
                            # Create new ColumnOperation with prefixed column
                            modified_value = ColumnOperation(
                                Column(prefixed_name),
                                op,
                                original_value.value,
                            )

                if modified_value != original_value:
                    # Create new condition with modified value
                    # condition.operation is always a string for ColumnOperation instances
                    op = getattr(condition, "operation", None)
                    if op is not None:
                        condition_to_translate = ColumnOperation(
                            original_column,
                            op,
                            modified_value,
                        )

        try:
            # Try to translate the condition
            filter_expr = self.translator.translate(
                condition_to_translate,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )
            # Filter based on expression result
            filtered = cross_joined.filter(filter_expr)

            # Handle join types
            if how in ("left", "outer", "full", "full_outer"):
                # For left/outer joins, include unmatched rows from left DataFrame
                if filtered.height == 0:
                    # No matches - return all left rows with null right columns
                    result_df = df1
                    for col in df2.columns:
                        prefixed_col = f"_right_{col}" if has_conflicts else col
                        result_df = result_df.with_columns(
                            pl.lit(None).alias(prefixed_col)
                        )
                    # Remove prefix if needed
                    if has_conflicts:
                        rename_dict = {f"_right_{col}": col for col in df2.columns}
                        result_df = result_df.rename(rename_dict)
                    return result_df
                elif len(df1.columns) > 0:
                    # Get matched left rows using all columns as composite key
                    # Create a hash or use all columns to identify matched rows
                    matched_left_cols = df1.columns
                    matched_left = filtered.select(matched_left_cols).unique()
                    # Get all left rows
                    all_left = df1.select(matched_left_cols)
                    # Find unmatched left rows using anti join on all columns
                    unmatched_left = all_left.join(
                        matched_left, on=matched_left_cols, how="anti"
                    )
                    if unmatched_left.height > 0:
                        # Add unmatched left rows with null right columns
                        unmatched_with_nulls = unmatched_left
                        for col in df2.columns:
                            prefixed_col = f"_right_{col}" if has_conflicts else col
                            unmatched_with_nulls = unmatched_with_nulls.with_columns(
                                pl.lit(None).alias(prefixed_col)
                            )
                        # Combine matched and unmatched rows
                        filtered = pl.concat([filtered, unmatched_with_nulls])
                if how in ("outer", "full", "full_outer"):
                    # For outer joins, also include unmatched right rows
                    # This is more complex and may not be needed for basic cases
                    pass
        except (ValueError, AttributeError, TypeError):
            # If direct translation fails, use Python fallback
            from sparkless.dataframe.evaluation.expression_evaluator import (
                ExpressionEvaluator,
            )

            evaluator = ExpressionEvaluator()
            rows = cross_joined.to_dicts()

            # Evaluate condition for each row
            filtered_rows = []
            for row in rows:
                # Create evaluation row - map prefixed columns back if needed
                eval_row = row.copy()
                if has_conflicts:
                    # Map prefixed columns back to original names for evaluation
                    for col in df2.columns:
                        prefixed_col = f"_right_{col}"
                        if prefixed_col in eval_row:
                            eval_row[col] = eval_row[prefixed_col]

                try:
                    result = evaluator.evaluate_expression(eval_row, condition)
                    if result is True:
                        filtered_rows.append(row)
                except Exception:
                    # If evaluation fails, skip this row
                    pass

            if not filtered_rows:
                # No matches
                if how in ("left", "outer", "full", "full_outer"):
                    # For left/outer joins, return all left rows with null right columns
                    result_df = df1
                    for col in df2.columns:
                        result_df = result_df.with_columns(pl.lit(None).alias(col))
                    return result_df
                else:
                    # For inner/right joins with no matches, return empty with correct schema
                    empty_schema = {col: df1[col].dtype for col in df1.columns}
                    for col in df2.columns:
                        empty_schema[col] = df2[col].dtype
                    return pl.DataFrame(schema=empty_schema)

            # Convert back to Polars DataFrame
            filtered = pl.DataFrame(filtered_rows)

        # Step 3: Handle join types
        # For now, we return the filtered result (inner join behavior)
        # Full left/outer join support would require tracking unmatched rows
        # This is a simplified implementation that works for inner joins

        # Step 4: Remove prefix from column names if we prefixed df2 columns
        # Only rename if the original column name doesn't already exist (from df1)
        if has_conflicts:
            rename_dict = {}
            for col in df2.columns:
                prefixed_col = f"_right_{col}"
                # Only rename if the original name doesn't exist (to avoid duplicates)
                # If it exists, keep the prefixed name (PySpark keeps both with same name)
                if prefixed_col in filtered.columns and col not in filtered.columns:
                    rename_dict[prefixed_col] = col
            if rename_dict:
                filtered = filtered.rename(rename_dict)

        return filtered

    def _coerce_union_types(
        self, df1: pl.DataFrame, df2: pl.DataFrame
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """Coerce union column types to match if needed (numeric vs string).

        PySpark allows unioning DataFrames with different types (e.g., i64 vs str),
        automatically normalizing to string when mixing numeric and string types.

        Args:
            df1: First DataFrame
            df2: Second DataFrame

        Returns:
            Tuple of (coerced_df1, coerced_df2)
        """
        # Define numeric types
        numeric_types = (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Float32,
            pl.Float64,
        )
        string_type = pl.Utf8

        cast_exprs_df1 = []
        cast_exprs_df2 = []
        result_df1 = df1
        result_df2 = df2

        # Ensure both DataFrames have the same columns
        df1_cols = set(df1.columns)
        df2_cols = set(df2.columns)
        all_cols = sorted(df1_cols | df2_cols)

        # Process each column that exists in both DataFrames
        for col in all_cols:
            if col not in df1.columns or col not in df2.columns:
                continue

            dtype1 = df1[col].dtype
            dtype2 = df2[col].dtype

            # If types match, no coercion needed
            if dtype1 == dtype2:
                continue

            # Check if one is numeric and one is string
            is_numeric1 = dtype1 in numeric_types
            is_numeric2 = dtype2 in numeric_types
            is_string1 = dtype1 == string_type
            is_string2 = dtype2 == string_type

            if (is_numeric1 and is_string2) or (is_string1 and is_numeric2):
                # One is numeric, one is string - normalize to string (PySpark behavior)
                # Issue #242: LongType + StringType -> StringType
                if is_numeric1 and is_string2:
                    # df1[col] is numeric, cast to string
                    cast_exprs_df1.append(pl.col(col).cast(string_type, strict=False))
                else:
                    # df2[col] is numeric, cast to string
                    cast_exprs_df2.append(pl.col(col).cast(string_type, strict=False))
            elif is_numeric1 and is_numeric2:
                # Both numeric but different types - promote to wider type
                if isinstance(dtype1, (pl.Float32, pl.Float64)) or isinstance(
                    dtype2, (pl.Float32, pl.Float64)
                ):
                    target_dtype = pl.Float64
                else:
                    target_dtype = pl.Int64

                if dtype1 != target_dtype:
                    cast_exprs_df1.append(pl.col(col).cast(target_dtype, strict=False))
                if dtype2 != target_dtype:
                    cast_exprs_df2.append(pl.col(col).cast(target_dtype, strict=False))
            # For other type combinations, don't coerce (will be handled by validation)

        # Apply casts if any
        if cast_exprs_df1:
            result_df1 = df1.with_columns(cast_exprs_df1)
        if cast_exprs_df2:
            result_df2 = df2.with_columns(cast_exprs_df2)

        return result_df1, result_df2

    def apply_union(self, df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
        """Apply a union operation.

        Args:
            df1: First DataFrame
            df2: Second DataFrame

        Returns:
            Unioned DataFrame
        """
        # Coerce types before union (PySpark behavior: normalize numeric+string to string)
        df1, df2 = self._coerce_union_types(df1, df2)

        # Ensure schemas match
        df1_cols = set(df1.columns)
        df2_cols = set(df2.columns)

        # Add missing columns with correct types
        for col in df1_cols - df2_cols:
            # Use the type from df1's column
            col_type = df1[col].dtype
            df2 = df2.with_columns(pl.lit(None, dtype=col_type).alias(col))

        for col in df2_cols - df1_cols:
            # Use the type from df2's column
            col_type = df2[col].dtype
            df1 = df1.with_columns(pl.lit(None, dtype=col_type).alias(col))

        # Ensure column order matches
        column_order = df1.columns
        df2 = df2.select(column_order)

        return pl.concat([df1, df2])

    def apply_order_by(
        self, df: pl.DataFrame, columns: List[Any], ascending: bool = True
    ) -> pl.DataFrame:
        """Apply an orderBy operation.

        Args:
            df: Source Polars DataFrame
            columns: Columns to sort by
            ascending: Sort direction

        Returns:
            Sorted DataFrame
        """
        sort_by = []
        descending_flags = []
        nulls_last_flags = []
        for col in columns:
            is_desc = False
            nulls_last = None  # None means default behavior
            col_name = None
            if isinstance(col, str):
                col_name = col
                is_desc = not ascending
                nulls_last = True  # PySpark default: nulls last
            elif hasattr(col, "operation"):
                operation = col.operation
                col_name = col.column.name if hasattr(col, "column") else col.name
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
                    nulls_last = True  # PySpark default: nulls last for desc()
                elif operation == "asc":
                    is_desc = False
                    nulls_last = True  # PySpark default: nulls last for asc()
                else:
                    # Fallback for other operations
                    is_desc = not ascending
                    nulls_last = True  # PySpark default: nulls last
            else:
                col_name = col.name if hasattr(col, "name") else str(col)
                is_desc = not ascending
                nulls_last = True  # PySpark default: nulls last

            if col_name:
                sort_by.append(col_name)
                descending_flags.append(is_desc)
                nulls_last_flags.append(nulls_last)

        if not sort_by:
            return df

        # Use sort() with by, descending, and nulls_last parameters
        has_nulls_specification = any(n is not None for n in nulls_last_flags)
        if has_nulls_specification:
            return df.sort(
                sort_by, descending=descending_flags, nulls_last=nulls_last_flags
            )
        else:
            # No nulls specification, use default
            return df.sort(sort_by, descending=descending_flags)

    def apply_limit(self, df: pl.DataFrame, n: int) -> pl.DataFrame:
        """Apply a limit operation.

        Args:
            df: Source Polars DataFrame
            n: Number of rows to return

        Returns:
            Limited DataFrame
        """
        return df.head(n)

    def apply_offset(self, df: pl.DataFrame, n: int) -> pl.DataFrame:
        """Apply an offset operation (skip first n rows).

        Args:
            df: Source Polars DataFrame
            n: Number of rows to skip

        Returns:
            DataFrame with first n rows skipped
        """
        return df.slice(n)

    @profiled("polars.apply_group_by_agg", category="polars")
    def apply_group_by_agg(
        self, df: pl.DataFrame, group_by: List[Any], aggs: List[Any]
    ) -> pl.DataFrame:
        """Apply a groupBy().agg() operation.

        Args:
            df: Source Polars DataFrame
            group_by: Columns to group by
            aggs: Aggregation expressions

        Returns:
            Aggregated DataFrame
        """
        # Translate group by columns
        group_by_cols = []
        for col in group_by:
            if isinstance(col, str):
                group_by_cols.append(col)
            elif hasattr(col, "name"):
                group_by_cols.append(col.name)
            else:
                raise ValueError(f"Cannot determine column name for group by: {col}")

        # Translate aggregation expressions
        agg_exprs = []
        for agg in aggs:
            expr = self.translator.translate(agg)
            # Get alias if available
            alias_name = getattr(agg, "name", None) or getattr(agg, "_alias_name", None)
            if alias_name:
                expr = expr.alias(alias_name)
            agg_exprs.append(expr)

        if not group_by_cols:
            # Global aggregation
            return df.select(agg_exprs)
        else:
            return df.group_by(group_by_cols).agg(agg_exprs)

    def apply_distinct(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply a distinct operation.

        Args:
            df: Source Polars DataFrame

        Returns:
            DataFrame with distinct rows
        """
        return df.unique()

    def apply_drop(self, df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """Apply a drop operation.

        Args:
            df: Source Polars DataFrame
            columns: Columns to drop

        Returns:
            DataFrame with columns dropped
        """
        return df.drop(columns)

    def apply_with_column_renamed(
        self, df: pl.DataFrame, old_name: str, new_name: str
    ) -> pl.DataFrame:
        """Apply a withColumnRenamed operation.

        Args:
            df: Source Polars DataFrame
            old_name: Old column name
            new_name: New column name

        Returns:
            DataFrame with renamed column
        """
        return df.rename({old_name: new_name})
