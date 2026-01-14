from __future__ import annotations

# DataFrame operation executor for Polars.
# Provides execution of DataFrame operations (filter, select, join, etc.)
# using the Polars DataFrame API.

import json
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Tuple, Union
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

    def _find_column_case_insensitive(
        self, df: pl.DataFrame, column_name: str
    ) -> Optional[str]:
        """Find column name in Polars DataFrame case-insensitively."""
        for col in df.columns:
            if col.lower() == column_name.lower():
                return str(col)  # Ensure we return str, not Any
        return None

    @profiled("polars.apply_filter", category="polars")
    def apply_filter(self, df: pl.DataFrame, condition: Any) -> pl.DataFrame:
        """Apply a filter operation.

        Args:
            df: Source Polars DataFrame
            condition: Filter condition (ColumnOperation or expression)

        Returns:
            Filtered Polars DataFrame
        """
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
                # Find column case-insensitively
                actual_col_name = self._find_column_case_insensitive(df, col_name)
                if actual_col_name and actual_col_name in df.columns:
                    # Get the column's dtype
                    input_col_dtype = df[actual_col_name].dtype

        filter_expr = self.translator.translate(
            condition,
            input_col_dtype=input_col_dtype,
            available_columns=list(df.columns),
        )
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
                else:
                    # Use column name as-is (case-insensitive matching handled earlier)
                    select_exprs.append(pl.col(col))
                    select_names.append(col)
            elif isinstance(col, WindowFunction):
                # Handle window functions
                try:
                    window_expr = self.window_handler.translate_window_function(col, df)
                    alias_name = (
                        getattr(col, "name", None)
                        or f"{col.function_name.lower()}_window"
                    )
                    select_exprs.append(window_expr.alias(alias_name))
                    select_names.append(alias_name)
                except ValueError:
                    # Fallback to Python evaluation for unsupported window functions
                    # (e.g., rowsBetween frames that require reverse cumulative operations)
                    # Window functions need to be evaluated across all rows, not row-by-row
                    # So we'll collect them and evaluate them together later
                    if rows_cache is None:
                        rows_cache = df.to_dicts()
                    # Store window function for batch evaluation
                    if not hasattr(self, "_python_window_functions"):
                        self._python_window_functions = []
                    alias_name = (
                        getattr(col, "name", None)
                        or f"{col.function_name.lower()}_window"
                    )
                    self._python_window_functions.append((alias_name, col, rows_cache))
                    select_names.append(alias_name)
                    continue
            else:
                alias_name = getattr(col, "name", None) or getattr(
                    col, "_alias_name", None
                )
                try:
                    expr = self.translator.translate(col)
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
        if hasattr(self, "_python_window_functions") and self._python_window_functions:
            from sparkless.dataframe.window_handler import WindowFunctionHandler
            from sparkless.dataframe import DataFrame

            # Use the cached rows for window function evaluation
            data_rows = rows_cache if rows_cache else result.to_dicts()

            # Create a temporary DataFrame for window function evaluation
            # We need the original data to evaluate window functions correctly
            # Create an empty schema since we're only using this for window function evaluation
            from sparkless.spark_types import StructType

            temp_df = DataFrame(data_rows, StructType([]), None)
            window_handler = WindowFunctionHandler(temp_df)

            # Evaluate all window functions
            for alias_name, window_func, _ in self._python_window_functions:
                # Evaluate window function across all rows
                # The window handler modifies data_rows in place
                window_handler.evaluate_window_functions(data_rows, [(0, window_func)])
                # Extract values from evaluated data
                values = [row.get(alias_name) for row in data_rows]
                result = result.with_columns(pl.Series(alias_name, values))

            # Clean up
            delattr(self, "_python_window_functions")

        # Only reorder if we have python_columns AND the order doesn't match
        # This ensures we preserve all columns while matching the requested order
        if select_names and (
            python_columns
            or (
                hasattr(self, "_python_window_functions")
                and getattr(self, "_python_window_functions", None)
            )
        ):
            existing_cols = list(result.columns)
            # Check if reordering is needed and safe
            if existing_cols != select_names and all(
                name in existing_cols for name in select_names
            ):
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

    def _extract_column_name(self, expr: Any) -> Union[str, None]:
        if isinstance(expr, Column):
            return expr.name
        if isinstance(expr, ColumnOperation) and hasattr(expr, "name"):
            return expr.name
        if isinstance(expr, str):
            return expr
        return getattr(expr, "name", None)

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
        if isinstance(expression, WindowFunction):
            # Window functions need special handling
            # For window functions with order_by, we need to sort the DataFrame first
            # to ensure correct window function evaluation
            window_spec = expression.window_spec
            function_name = getattr(expression, "function_name", "").upper()

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
                            sort_cols.append(col)
                        elif hasattr(col, "name"):
                            sort_cols.append(col.name)
                # Add order_by columns
                for col in window_spec._order_by:
                    col_name = None
                    is_desc = False
                    if isinstance(col, str):
                        col_name = col
                    elif hasattr(col, "name"):
                        col_name = col.name
                    elif hasattr(col, "operation") and col.operation == "desc":
                        is_desc = True
                        if hasattr(col, "column") and hasattr(col.column, "name"):
                            col_name = col.column.name
                        elif hasattr(col, "name"):
                            col_name = col.name

                    if col_name:
                        if is_desc:
                            # For descending, convert to Polars expression
                            if all(isinstance(c, str) for c in sort_cols):
                                # Convert all to expressions
                                expr_cols = [pl.col(c) for c in sort_cols]
                                expr_cols.append(pl.col(col_name).desc())
                                df = df.sort(expr_cols)
                                sort_cols = []  # Mark as sorted
                            else:
                                sort_cols.append(pl.col(col_name).desc())
                        else:
                            sort_cols.append(col_name)

            # Sort if we have string column names (and haven't already sorted with expressions)
            # CRITICAL: For lag/lead functions, we MUST sort before applying the window function
            if function_name in ("LAG", "LEAD") and has_order_by:
                # Rebuild sort_cols if needed to ensure we sort
                if not sort_cols or not all(isinstance(c, str) for c in sort_cols):
                    sort_cols = []
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
                if sort_cols and all(isinstance(c, str) for c in sort_cols):
                    df = df.sort(sort_cols)
            elif sort_cols and all(isinstance(c, str) for c in sort_cols):
                df = df.sort(sort_cols)

            try:
                window_expr = self.window_handler.translate_window_function(
                    expression, df
                )
                result = df.with_columns(window_expr.alias(column_name))
            except ValueError:
                # Fallback to Python evaluation for unsupported window functions
                # Convert Polars DataFrame to list of dicts for Python evaluation
                data = df.to_dicts()
                # Evaluate window function using Python implementation
                results = expression.evaluate(data)
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
            # Check if this is a to_timestamp operation and if the input column is a string
            # This helps us choose the right method (str.strptime vs map_elements)
            input_col_dtype = None
            from sparkless.functions.core.column import ColumnOperation, Column

            if (
                isinstance(expression, ColumnOperation)
                and expression.operation == "to_timestamp"
            ):
                # Check if the input column is a simple Column (direct column reference)
                if isinstance(expression.column, Column) and not isinstance(
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

            expr = self.translator.translate(
                expression,
                input_col_dtype=input_col_dtype,
                available_columns=list(df.columns),
            )

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

            result = df.with_columns(expr.alias(column_name))

            return result

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

        if isinstance(on, ColumnOperation) and getattr(on, "operation", None) == "==":
            if not hasattr(on, "column") or not hasattr(on, "value"):
                raise ValueError("Join condition must have column and value attributes")
            left_col = on.column.name if hasattr(on.column, "name") else str(on.column)
            right_col = on.value.name if hasattr(on.value, "name") else str(on.value)
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

        # Resolve join_keys case-insensitively if they are strings
        resolved_join_keys = None
        left_on_keys = []
        right_on_keys = []
        use_left_right_on = False
        if join_keys is not None:
            resolved_join_keys = []
            for col in join_keys:
                if isinstance(col, str):
                    # Resolve column name case-insensitively in both DataFrames
                    actual_col_df1 = self._find_column_case_insensitive(df1, col)
                    actual_col_df2 = self._find_column_case_insensitive(df2, col)
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
                else:
                    resolved_join_keys.append(col)

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

                # Verify columns exist (case-insensitive)
                resolved_left_on = []
                for col in left_on:
                    actual_col = self._find_column_case_insensitive(df1, col)
                    if actual_col is None:
                        raise ValueError(
                            f"Join column '{col}' not found in left DataFrame. Available columns: {df1.columns}"
                        )
                    resolved_left_on.append(actual_col)
                resolved_right_on = []
                for col in right_on:
                    actual_col = self._find_column_case_insensitive(df2, col)
                    if actual_col is None:
                        raise ValueError(
                            f"Join column '{col}' not found in right DataFrame. Available columns: {df2.columns}"
                        )
                    resolved_right_on.append(actual_col)

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
                # Verify columns exist in both DataFrames (case-insensitive)
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
                    if join_keys is None:
                        raise ValueError("Join keys must be specified")
                    # Fallback: resolve join_keys if not already resolved
                    resolved_join_keys = []
                    fallback_left_on_keys = []
                    fallback_right_on_keys = []
                    fallback_use_left_right_on = False
                    for col in join_keys:
                        if isinstance(col, str):
                            # Resolve column name case-insensitively in both DataFrames
                            actual_col_df1 = self._find_column_case_insensitive(
                                df1, col
                            )
                            actual_col_df2 = self._find_column_case_insensitive(
                                df2, col
                            )
                            if actual_col_df1 is None:
                                raise ValueError(
                                    f"Join column '{col}' not found in left DataFrame. Available columns: {df1.columns}"
                                )
                            if actual_col_df2 is None:
                                raise ValueError(
                                    f"Join column '{col}' not found in right DataFrame. Available columns: {df2.columns}"
                                )
                            # If column names differ, use left_on/right_on
                            if actual_col_df1 != actual_col_df2:
                                fallback_use_left_right_on = True
                                fallback_left_on_keys.append(actual_col_df1)
                                fallback_right_on_keys.append(actual_col_df2)
                                # Don't add to resolved_join_keys when using left_on/right_on
                            else:
                                # Same column name, can use on=
                                resolved_join_keys.append(actual_col_df1)
                        else:
                            resolved_join_keys.append(col)

                    if (
                        fallback_use_left_right_on
                        and fallback_left_on_keys
                        and fallback_right_on_keys
                    ):
                        # Use left_on/right_on when column names differ
                        joined = df1.join(
                            df2,
                            left_on=fallback_left_on_keys,
                            right_on=fallback_right_on_keys,
                            how=polars_how,
                        )
                        # Add the right_on columns back if needed (PySpark includes both)
                        for right_col in fallback_right_on_keys:
                            if right_col not in joined.columns:
                                left_col = fallback_left_on_keys[
                                    fallback_right_on_keys.index(right_col)
                                ]
                                joined = joined.with_columns(
                                    pl.col(left_col).alias(right_col)
                                )
                        return joined
                    elif resolved_join_keys and len(resolved_join_keys) > 0:
                        return df1.join(df2, on=resolved_join_keys, how=polars_how)
                    else:
                        # Fallback to original join_keys if resolution failed
                        return df1.join(df2, on=join_keys, how=polars_how)
                else:
                    # resolved_join_keys is empty list, fallback to original join_keys
                    return df1.join(df2, on=join_keys, how=polars_how)

    def apply_union(self, df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
        """Apply a union operation.

        Args:
            df1: First DataFrame
            df2: Second DataFrame

        Returns:
            Unioned DataFrame
        """
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
        for col in columns:
            if isinstance(col, str):
                if ascending:
                    sort_by.append(pl.col(col))
                else:
                    sort_by.append(pl.col(col).desc())
            elif hasattr(col, "operation") and col.operation == "desc":
                col_name = col.column.name if hasattr(col, "column") else col.name
                sort_by.append(pl.col(col_name).desc())
            else:
                col_name = col.name if hasattr(col, "name") else str(col)
                if ascending:
                    sort_by.append(pl.col(col_name))
                else:
                    sort_by.append(pl.col(col_name).desc())

        if not sort_by:
            return df

        return df.sort(sort_by)

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
