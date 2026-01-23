"""
Polars materializer for lazy DataFrame operations.

This module provides materialization of lazy DataFrame operations using Polars,
replacing SQL-based materialization with Polars DataFrame operations.
"""

from typing import Any, Dict, List, Optional, Set, Tuple, cast
import polars as pl
from sparkless.spark_types import StructType, Row
from sparkless.functions import ColumnOperation
from sparkless.functions.core.column import Column
from .expression_translator import PolarsExpressionTranslator
from .operation_executor import PolarsOperationExecutor


class PolarsMaterializer:
    """Materializes lazy operations using Polars."""

    # Explicit capability declarations
    SUPPORTED_OPERATIONS = {
        "select",
        "filter",
        "withColumn",
        "drop",
        "join",
        "union",
        "orderBy",
        "limit",
        "offset",
        "groupBy",
        "distinct",
        "withColumnRenamed",
    }

    # Operations that are explicitly unsupported (require manual materialization)
    UNSUPPORTED_OPERATIONS = {
        "months_between",
        "pi",
        "e",
    }

    # Optional: Operation-specific metadata (for future extensibility)
    OPERATION_METADATA: Dict[str, Dict[str, Any]] = {}

    def __init__(self) -> None:
        """Initialize Polars materializer."""
        self.translator = PolarsExpressionTranslator()
        self.operation_executor = PolarsOperationExecutor(self.translator)

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

    def materialize(
        self,
        data: List[Dict[str, Any]],
        schema: StructType,
        operations: List[Tuple[str, Any]],
    ) -> List[Row]:
        """Materialize lazy operations into actual data.

        Args:
            data: Initial data
            schema: DataFrame schema
            operations: List of queued operations (operation_name, payload)

        Returns:
            List of result rows
        """
        # Check if we have operations that require processing even with empty data
        # (e.g., union with non-empty DataFrame)
        has_union_operation = any(op_name == "union" for op_name, _ in operations)

        if not data and not has_union_operation:
            # Empty DataFrame with no operations that need processing
            return []

        # Convert data to Polars DataFrame
        # For empty DataFrames, create from schema if available
        if not data and schema.fields:
            from .type_mapper import mock_type_to_polars_dtype

            schema_dict = {}
            for field in schema.fields:
                polars_dtype = mock_type_to_polars_dtype(field.dataType)
                schema_dict[field.name] = pl.Series(field.name, [], dtype=polars_dtype)
            df = pl.DataFrame(schema_dict)
        elif not data:
            # Empty DataFrame with no schema
            df = pl.DataFrame()
        else:
            # Create DataFrame from data
            # Handle tuple/list format by converting to dicts using schema field names
            # Note: Type signature says List[dict], but we defensively handle tuples at runtime
            if data:
                first_row: Any = data[0]  # Allow runtime check for tuples/lists
                if isinstance(first_row, (list, tuple)):
                    # Convert tuples to dicts using schema field names
                    converted_data = []
                    field_names = [f.name for f in schema.fields]
                    for row in data:
                        row_any: Any = row  # Allow runtime check for tuples/lists
                        if isinstance(row_any, (list, tuple)):
                            # Only iterate up to the minimum of row length and field_names length
                            # This handles cases where select operations reduce the number of columns
                            converted_data.append(
                                {
                                    field_names[i]: row_any[i]
                                    for i in range(min(len(row_any), len(field_names)))
                                }
                            )
                        else:
                            converted_data.append(row_any)
                    df = pl.DataFrame(converted_data)
                else:
                    df = pl.DataFrame(data)
            else:
                df = pl.DataFrame(data)

            # Only enforce schema types if we have a union operation (to prevent type mismatches)
            # For other operations, let Polars infer types naturally
            if has_union_operation and schema.fields:
                from .type_mapper import mock_type_to_polars_dtype

                cast_exprs = []
                for field in schema.fields:
                    polars_dtype = mock_type_to_polars_dtype(field.dataType)
                    # Only cast if column exists and type doesn't match
                    # Only cast numeric types to prevent Int32/Int64 mismatches
                    if (
                        field.name in df.columns
                        and df[field.name].dtype != polars_dtype
                        and polars_dtype in (pl.Int32, pl.Int64, pl.Float32, pl.Float64)
                    ):
                        # Only cast numeric types (Int32/Int64) to prevent union issues
                        # Don't cast string/datetime types as they can cause schema errors
                        cast_exprs.append(pl.col(field.name).cast(polars_dtype))

                if cast_exprs:
                    df = df.with_columns(cast_exprs)

        # Use lazy evaluation for better performance
        lazy_df = df.lazy()

        # Track original schema BEFORE any operations
        # The schema parameter should be the original schema before any operations
        # If we have data, we can verify by checking the data keys match schema fields
        original_schema = schema
        # Verify schema matches data (if data exists)
        if data and len(data) > 0:
            first_row = data[0]
            # Handle both dict and tuple formats
            is_dict_format = isinstance(first_row, dict)
            is_tuple_format = isinstance(first_row, (list, tuple))

            if is_dict_format:
                data_keys = set(first_row.keys())
            elif is_tuple_format:
                # Tuple/list format - use schema field names
                data_keys = {f.name for f in schema.fields}
            else:
                # Fallback: try to get keys if possible
                data_keys = set(getattr(first_row, "keys", lambda: [])())

            schema_keys = {field.name for field in schema.fields}
            # If we have operations, use base schema from data for the op loop
            # (e.g. union compares current_schema vs other; current_schema must be base, not projected).
            if data and operations and is_dict_format:
                from ...core.schema_inference import SchemaInferenceEngine

                try:
                    inferred_schema, _ = SchemaInferenceEngine.infer_from_data(data)
                    original_schema = inferred_schema
                except (ValueError, Exception):
                    pass
            elif is_dict_format and data_keys != schema_keys and not operations:
                # No ops: infer when schema doesn't match data
                from ...core.schema_inference import SchemaInferenceEngine

                inferred_schema, _ = SchemaInferenceEngine.infer_from_data(data)
                original_schema = inferred_schema

        original_columns = {field.name for field in original_schema.fields}

        # Track current schema as operations are applied
        current_schema = original_schema

        # Build mapping of computed expressions to column names for optimization
        from ...dataframe.lazy import LazyEvaluationEngine

        computed_expressions = LazyEvaluationEngine._build_computed_expressions_map(
            operations
        )

        # Build column dependency graph for drop operation optimization
        # This tracks which columns depend on which other columns
        # IMPORTANT: Use the projected schema (which includes computed columns) to determine
        # available columns, not just original_columns. This ensures that intermediate
        # columns created by earlier operations are available for dependency tracking.
        # The schema parameter already reflects the projected schema after all operations.
        projected_columns = {field.name for field in schema.fields}
        column_dependencies = LazyEvaluationEngine._build_column_dependency_graph(
            operations, projected_columns
        )

        # Track current columns as we process operations (for dependency tracking)
        # Start with projected columns (which includes computed columns from earlier operations)
        current_available_columns = projected_columns.copy()

        # Track if we have a materialized DataFrame available (e.g., after drop operation)
        # This avoids schema issues when converting back to lazy and then collecting again
        df_materialized: Optional[pl.DataFrame] = None

        # Group operations to handle filter-before-select optimization
        # When filter references original columns not in current schema, push it before select
        optimized_operations = []
        i = 0
        while i < len(operations):
            op_name, payload = operations[i]
            if op_name == "select" and i + 1 < len(operations):
                # Check if next operation is a filter that references original columns
                next_op_name, next_payload = operations[i + 1]
                if next_op_name == "filter":
                    # Check if filter references columns not in select result
                    # Extract column names that will be in the result of select
                    # (using schema inference, not just looking at the expressions)
                    from ...dataframe.schema.schema_manager import SchemaManager

                    select_schema = SchemaManager.project_schema_with_operations(
                        current_schema, [(op_name, payload)]
                    )
                    current_columns = {field.name for field in select_schema.fields}
                    # Check if filter references original columns not in current columns
                    missing_cols = LazyEvaluationEngine._extract_column_names(
                        next_payload, current_columns
                    )
                    # If filter references original columns, push it before select
                    if missing_cols and missing_cols.issubset(original_columns):
                        # Filter references original columns - apply it before select
                        # Add filter first, then select
                        optimized_operations.append((next_op_name, next_payload))
                        optimized_operations.append(
                            (op_name, payload)
                        )  # Add select after filter
                        i += 2  # Skip both select and filter
                        continue
            optimized_operations.append((op_name, payload))
            i += 1

        # Apply optimized operations in sequence
        # If no optimization happened, optimized_operations will be the same as operations
        for current_op_index, (op_name, payload) in enumerate(optimized_operations):
            if op_name == "filter":
                # Filter operation - optimize to use computed columns if available
                # Note: When filter is pushed before select, computed_expressions will be empty
                # because select hasn't been applied yet. This is fine - the filter references
                # original columns, not computed ones.
                # If we have a materialized DataFrame, convert it back to lazy first
                if df_materialized is not None:
                    lazy_df = df_materialized.lazy()
                    df_materialized = None
                optimized_condition = (
                    LazyEvaluationEngine._replace_with_computed_column(
                        payload, computed_expressions
                    )
                )
                # Get available columns from lazy DataFrame schema for case-insensitive matching
                available_columns = (
                    list(lazy_df.schema.keys()) if hasattr(lazy_df, "schema") else []
                )

                # Check if condition contains struct field paths that need special handling
                # (e.g., F.col("StructVal")["E1"] > 2 creates ColumnOperation with Column("StructVal.E1"))
                # This must be checked BEFORE translation to avoid ColumnNotFoundError
                is_struct_field_path = False
                if isinstance(optimized_condition, ColumnOperation):
                    if (
                        isinstance(optimized_condition.column, Column)
                        and "." in optimized_condition.column.name
                    ):
                        is_struct_field_path = True
                elif (
                    isinstance(optimized_condition, Column)
                    and "." in optimized_condition.name
                ):
                    is_struct_field_path = True

                if is_struct_field_path:
                    # Handle struct field path in filter
                    # Use apply_filter which has the struct field path handling
                    df_collected = lazy_df.collect()
                    filtered_df = self.operation_executor.apply_filter(
                        df_collected, optimized_condition
                    )
                    lazy_df = filtered_df.lazy()
                    continue

                # Extract column dtype for isin operations to enable type coercion
                input_col_dtype = None
                if (
                    isinstance(optimized_condition, ColumnOperation)
                    and optimized_condition.operation == "isin"
                    and hasattr(optimized_condition, "column")
                    and hasattr(optimized_condition.column, "name")
                ):
                    # Get the column name from the condition
                    col_name = optimized_condition.column.name
                    # Get column dtype from schema if available
                    if hasattr(lazy_df, "schema") and col_name in lazy_df.schema:
                        input_col_dtype = lazy_df.schema[col_name]

                try:
                    filter_expr = self.translator.translate(
                        optimized_condition,
                        input_col_dtype=input_col_dtype,
                        available_columns=available_columns,
                    )
                except ValueError as e:
                    # Check if this is a WindowFunction comparison that should be handled
                    error_msg = str(e)
                    from sparkless.functions.window_execution import WindowFunction
                    # Note: Column is already imported at the top of the file

                    is_window_function_comparison = (
                        "WindowFunction comparison" in error_msg
                        and isinstance(optimized_condition, ColumnOperation)
                        and isinstance(optimized_condition.column, WindowFunction)
                    )

                    if is_window_function_comparison:
                        # Handle WindowFunction comparison in filter
                        # Use apply_filter which has the WindowFunction comparison handling
                        df_collected = lazy_df.collect()
                        filtered_df = self.operation_executor.apply_filter(
                            df_collected, optimized_condition
                        )
                        lazy_df = filtered_df.lazy()
                        continue

                    # Check if this is a struct field path comparison that should be handled
                    # (e.g., F.col("StructVal")["E1"] > 2 creates ColumnOperation with Column("StructVal.E1"))
                    # Note: Column is already imported at the top of the file
                    is_struct_field_path = False
                    if isinstance(optimized_condition, ColumnOperation):
                        if (
                            isinstance(optimized_condition.column, Column)
                            and "." in optimized_condition.column.name
                        ):
                            is_struct_field_path = True
                    elif (
                        isinstance(optimized_condition, Column)
                        and "." in optimized_condition.name
                    ):
                        is_struct_field_path = True

                    if is_struct_field_path:
                        # Handle struct field path in filter
                        # Use apply_filter which has the struct field path handling
                        df_collected = lazy_df.collect()
                        filtered_df = self.operation_executor.apply_filter(
                            df_collected, optimized_condition
                        )
                        lazy_df = filtered_df.lazy()
                        continue

                    # Fallback to Python evaluation for unsupported operations (e.g., + with strings)
                    if "+ operation requires Python evaluation" in error_msg:
                        # Convert to eager DataFrame for Python evaluation
                        df_collected = lazy_df.collect()
                        data = df_collected.to_dicts()
                        from sparkless.dataframe.evaluation.expression_evaluator import (
                            ExpressionEvaluator,
                        )

                        evaluator = ExpressionEvaluator()
                        # Evaluate filter condition for each row
                        filtered_data = [
                            row
                            for row in data
                            if evaluator.evaluate_expression(row, optimized_condition)
                        ]
                        # Recreate DataFrame from filtered data
                        if filtered_data:
                            lazy_df = pl.DataFrame(filtered_data).lazy()
                        else:
                            # Empty result - create empty DataFrame with same schema
                            if hasattr(lazy_df, "schema"):
                                schema_dict = {
                                    col: pl.Series(col, [], dtype=lazy_df.schema[col])
                                    for col in lazy_df.schema
                                }
                                lazy_df = pl.DataFrame(schema_dict).lazy()
                            else:
                                lazy_df = pl.DataFrame().lazy()
                        continue
                    else:
                        raise

                # Apply filter to lazy DataFrame
                # Catch Polars ColumnNotFoundError and convert to SparkColumnNotFoundError
                try:
                    lazy_df = lazy_df.filter(filter_expr)

                except pl.exceptions.ColumnNotFoundError as e:
                    # Check if this is a struct field path error (e.g., "StructVal.E1" not found)
                    # If so, handle it by collecting and using apply_filter
                    error_msg = str(e)
                    import re
                    # Note: Column is already imported at the top of the file

                    # Check if the error is about a struct field path
                    col_match = re.search(
                        r'unable to find column\s+"([^"]+)"', error_msg
                    )
                    if col_match:
                        col_name = col_match.group(1)
                        # Check if this is a struct field path (contains a dot)
                        if "." in col_name:
                            # Check if the condition contains this struct field path
                            is_struct_field_path = False
                            if isinstance(optimized_condition, ColumnOperation):
                                if (
                                    isinstance(optimized_condition.column, Column)
                                    and "." in optimized_condition.column.name
                                ):
                                    is_struct_field_path = True
                            elif (
                                isinstance(optimized_condition, Column)
                                and "." in optimized_condition.name
                            ):
                                is_struct_field_path = True

                            if is_struct_field_path:
                                # Handle struct field path in filter
                                # Use apply_filter which has the struct field path handling
                                df_collected = lazy_df.collect()
                                filtered_df = self.operation_executor.apply_filter(
                                    df_collected, optimized_condition
                                )
                                lazy_df = filtered_df.lazy()
                                continue

                    # Convert Polars error to our consistent error format
                    from ...core.exceptions.operation import SparkColumnNotFoundError

                    # Extract column name from error message
                    # Polars error format: "unable to find column "col_name"; valid columns: [...]"
                    # Extract column name and available columns
                    valid_match = re.search(r"valid columns:\s*\[([^\]]+)\]", error_msg)

                    if col_match and valid_match:
                        col_name = col_match.group(1)
                        valid_cols_str = valid_match.group(1)
                        # Parse valid columns (remove quotes and split)
                        available_columns = [
                            col.strip().strip('"').strip("'")
                            for col in valid_cols_str.split(",")
                        ]
                        raise SparkColumnNotFoundError(col_name, available_columns)
                    else:
                        # Fallback: try to extract column name from error message
                        # or use the original error message
                        raise SparkColumnNotFoundError(
                            "unknown_column",
                            list(lazy_df.collect().columns)
                            if lazy_df is not None
                            else [],
                            f"Column not found during filter operation: {error_msg}",
                        )
                # Verify filter worked by checking row count (for debugging)
                # Note: We don't update current_schema for filter as it doesn't change columns
            elif op_name == "select":
                # Select operation - need to collect first for window functions
                # Use materialized DataFrame if available, otherwise collect from lazy
                # Preserve materialized state if it contains computed values (e.g., from to_timestamp)
                if df_materialized is not None:
                    df_collected = df_materialized

                    # Don't clear df_materialized yet - we'll clear it after select if needed
                    # This ensures computed values are preserved through select
                else:
                    df_collected = lazy_df.collect()

                # Check if columns are being dropped (columns before select vs after)
                columns_before = set(df_collected.columns)

                # Validate column names exist, but preserve requested column names for output
                # apply_select will handle resolution and aliasing to preserve requested names
                # This ensures PySpark behavior: requested column name is used as output name
                available_cols = list(df_collected.columns)
                case_sensitive = self._get_case_sensitive()
                for col in payload:
                    if isinstance(col, str) and col != "*":
                        from sparkless.core.column_resolver import ColumnResolver

                        # Validate column exists (for error reporting), but keep original name
                        # Handle nested struct field access (e.g., "Person.name")
                        if "." in col:
                            # Split into struct column and field name
                            parts = col.split(".", 1)
                            struct_col = parts[0]
                            # Validate the struct column exists
                            resolved_struct_col = ColumnResolver.resolve_column_name(
                                struct_col, available_cols, case_sensitive
                            )
                            if resolved_struct_col is None:
                                from ...core.exceptions.operation import (
                                    SparkColumnNotFoundError,
                                )

                                raise SparkColumnNotFoundError(
                                    struct_col, available_cols
                                )
                            # Keep original nested column name for output (apply_select will handle resolution)
                            # Validation is done above, now preserve the original requested name for output
                            # apply_select will resolve and alias correctly
                        else:
                            # Validate column exists, but preserve requested name
                            resolved_col: Optional[str] = (
                                ColumnResolver.resolve_column_name(
                                    col, available_cols, case_sensitive
                                )
                            )
                            if resolved_col is None:
                                from ...core.exceptions.operation import (
                                    SparkColumnNotFoundError,
                                )

                                raise SparkColumnNotFoundError(col, available_cols)
                            # Don't replace col with resolved_col - preserve requested name
                            # apply_select will resolve and apply alias to preserve requested name

                # Apply select with original column names (not resolved)
                # apply_select will handle resolution and aliasing to preserve requested names
                # This ensures the output column names match the requested names (PySpark behavior)
                result_df = self.operation_executor.apply_select(df_collected, payload)

                # If we started with df_materialized, keep the result materialized
                # to preserve computed values (e.g., from to_timestamp operations)
                had_materialized_before_select = df_materialized is not None

                # Get columns after select
                columns_after = set(result_df.columns)

                # Check if select includes computed columns (columns created by withColumn)
                # Computed columns are those in the current schema but not in the original schema
                # or columns that were created by previous withColumn operations
                original_column_names = {field.name for field in original_schema.fields}
                selected_columns = set()
                for col in payload:
                    if isinstance(col, str) and col != "*":
                        selected_columns.add(col)
                    elif hasattr(col, "name"):
                        # Column object - get the name
                        selected_columns.add(col.name)
                    elif hasattr(col, "_alias") and col._alias:
                        # Column with alias - use alias name
                        selected_columns.add(col._alias)

                # Check if any selected column is a computed column (not in original schema)
                has_computed_columns = bool(selected_columns - original_column_names)

                # If we had materialized data before OR if we're selecting computed columns,
                # keep it materialized after select to preserve computed values
                # This is especially important when distinct follows select, as distinct
                # needs the materialized DataFrame to preserve computed column values
                if had_materialized_before_select or has_computed_columns:
                    df_materialized = result_df
                    lazy_df = None
                else:
                    lazy_df = result_df.lazy()
                    df_materialized = None

                # If columns were dropped, clear the expression cache to invalidate
                # cached expressions that reference the dropped columns
                # This fixes issue #160 where cached expressions reference dropped columns
                if columns_before - columns_after:
                    self.translator.clear_cache()

                # Update schema after select
                from ...dataframe.schema.schema_manager import SchemaManager

                # Get case sensitivity from session for schema projection
                case_sensitive = self._get_case_sensitive()
                current_schema = SchemaManager.project_schema_with_operations(
                    current_schema, [(op_name, payload)], case_sensitive
                )
            elif op_name == "withColumn":
                # WithColumn operation - need to collect first for window functions
                # Use materialized DataFrame if available, otherwise collect from lazy
                # This avoids schema mismatch issues when converting back to lazy after drop
                # Track if we had materialized data before clearing it (to preserve computed values)
                had_materialized_before = df_materialized is not None

                if df_materialized is not None:
                    df_collected = df_materialized

                    df_materialized = None
                elif lazy_df is not None:
                    df_collected = lazy_df.collect()
                else:
                    # Should not happen, but handle gracefully
                    raise ValueError("No DataFrame available for withColumn operation")

                column_name, expression = payload

                # Get the expected schema after this operation BEFORE applying withColumn
                # This allows us to pass the expected type to enforce correct casting
                from ...dataframe.schema.schema_manager import SchemaManager

                updated_schema = SchemaManager.project_schema_with_operations(
                    current_schema, [(op_name, payload)]
                )

                # Find the expected type for this column from the updated schema
                expected_field = None
                for field in updated_schema.fields:
                    if field.name == column_name:
                        expected_field = field
                        break

                # Apply withColumn with expected schema type enforcement
                result_df = self.operation_executor.apply_with_column(
                    df_collected, column_name, expression, expected_field
                )

                # Don't align after withColumn - the result should already match the schema
                # Alignment can cause issues when the expression produces the correct type
                # but alignment tries to enforce a different type

                # Keep materialized DataFrame only if next operation is withColumnRenamed
                # This ensures columns added by withColumn are preserved through rename operations
                # For to_timestamp operations, keep materialized to avoid schema validation issues
                # when converting back to lazy (Polars validates against expression input types)
                # Also keep materialized if we had materialized data before (to preserve computed values)
                from sparkless.spark_types import TimestampType

                is_timestamp_column = expected_field is not None and isinstance(
                    expected_field.dataType, TimestampType
                )

                next_op_index = current_op_index + 1
                if next_op_index < len(optimized_operations):
                    next_op_name, _ = optimized_operations[next_op_index]
                    # Keep materialized if:
                    # 1. Next operation is withColumnRenamed
                    # 2. This is a timestamp column (to avoid validation issues)
                    # 3. We had materialized data before (to preserve computed values through multiple withColumn ops)
                    # 4. Next operation is select (to preserve computed column values for select->distinct sequences)
                    if (
                        next_op_name == "withColumnRenamed"
                        or is_timestamp_column
                        or had_materialized_before
                        or next_op_name == "select"
                    ):
                        # Keep materialized for next withColumnRenamed operation, for to_timestamp,
                        # or to preserve computed values from previous withColumn operations
                        # This avoids Polars schema validation issues when converting to lazy
                        # and ensures computed values are preserved
                        df_materialized = result_df
                        # For to_timestamp, when preserving materialized state, or when next op is select,
                        # don't create lazy_df. The materialized DataFrame will be used directly.
                        lazy_df = (
                            None
                            if (
                                is_timestamp_column
                                or had_materialized_before
                                or next_op_name == "select"
                            )
                            else result_df.lazy()
                        )  # Don't create lazy frame for to_timestamp, when preserving materialized, or when next is select
                    else:
                        # Convert result back to lazy for other operations
                        lazy_df = result_df.lazy()
                        df_materialized = None
                else:
                    # No more operations
                    # Keep materialized for to_timestamp, when we had materialized data,
                    # or when this is a computed column (to preserve values for final collection)
                    # This avoids validation on final collection and preserves computed values
                    if is_timestamp_column or had_materialized_before:
                        df_materialized = result_df
                        lazy_df = None  # Don't create lazy frame for to_timestamp or when preserving materialized
                    else:
                        # For computed columns (like struct field paths), keep materialized to preserve values
                        # Check if this column was created from a struct field path by checking the expression
                        # Column is already imported at module level
                        is_computed_column = (
                            isinstance(expression, Column) and "." in expression.name
                        ) or (
                            hasattr(expression, "column")
                            and isinstance(expression.column, Column)
                            and "." in expression.column.name
                        )

                        if is_computed_column:
                            df_materialized = result_df
                            lazy_df = None
                        else:
                            # Convert result back to lazy for final collection
                            lazy_df = result_df.lazy()
                            df_materialized = None
                # Update schema and available columns after withColumn
                from ...dataframe.schema.schema_manager import SchemaManager

                # Get case sensitivity from session for schema projection
                case_sensitive = self._get_case_sensitive()
                current_schema = SchemaManager.project_schema_with_operations(
                    current_schema, [(op_name, payload)], case_sensitive
                )
                current_available_columns.add(column_name)
            elif op_name == "join":
                # Join operation - need to handle separately
                other_df, on, how = payload
                # Materialize other_df if it has lazy operations before converting to Polars
                # This ensures renamed columns and other operations are applied
                if not isinstance(other_df, pl.DataFrame):
                    # Materialize if it has lazy operations
                    if (
                        hasattr(other_df, "_operations_queue")
                        and other_df._operations_queue
                    ):
                        # Materialize the other DataFrame first
                        materialized_other = other_df._materialize_if_lazy()
                        # Convert materialized DataFrame to Polars
                        if hasattr(materialized_other, "collect"):
                            # It's still a Sparkless DataFrame, get the data
                            other_rows = materialized_other.collect()
                            other_schema = getattr(materialized_other, "schema", None)

                            def _row_to_dict(row: Any) -> Dict[str, Any]:
                                # Sparkless Row
                                if hasattr(row, "asDict") and callable(row.asDict):
                                    return cast("Dict[str, Any]", row.asDict())
                                # Already a mapping
                                if isinstance(row, dict):
                                    return cast("Dict[str, Any]", row)
                                # Sequence-like rows (e.g. tuple/list) with a known schema
                                if other_schema is not None and hasattr(
                                    other_schema, "fields"
                                ):
                                    try:
                                        values = list(row)
                                        return {
                                            field.name: values[i]
                                            for i, field in enumerate(
                                                other_schema.fields
                                            )
                                            if i < len(values)
                                        }
                                    except Exception:
                                        pass
                                # Fallback: iterables of (k, v) pairs
                                return cast("Dict[str, Any]", dict(row))

                            other_data = (
                                [_row_to_dict(row) for row in other_rows]
                                if other_rows
                                else []
                            )

                            if not other_data:
                                # Empty DataFrame - create from schema if available
                                if hasattr(materialized_other, "schema"):
                                    from .type_mapper import mock_type_to_polars_dtype

                                    schema_dict = {}
                                    for field in materialized_other.schema.fields:
                                        polars_dtype = mock_type_to_polars_dtype(
                                            field.dataType
                                        )
                                        schema_dict[field.name] = pl.Series(
                                            field.name, [], dtype=polars_dtype
                                        )
                                    other_df = pl.DataFrame(schema_dict)
                                else:
                                    other_df = pl.DataFrame()
                            else:
                                # Use schema to ensure column names are preserved correctly
                                if hasattr(materialized_other, "schema"):
                                    # Verify column names match schema (safety check)
                                    schema_cols = set(
                                        materialized_other.schema.fieldNames()
                                    )
                                    data_cols = (
                                        set(other_data[0].keys())
                                        if other_data
                                        else set()
                                    )
                                    if schema_cols != data_cols:
                                        # Column mismatch - use schema to create DataFrame with correct column order
                                        # Create DataFrame ensuring column order matches schema
                                        ordered_data = []
                                        for row in other_data:
                                            ordered_row = {
                                                field.name: row.get(field.name)
                                                for field in materialized_other.schema.fields
                                            }
                                            ordered_data.append(ordered_row)
                                        other_df = pl.DataFrame(ordered_data)
                                    else:
                                        other_df = pl.DataFrame(other_data)
                                else:
                                    other_df = pl.DataFrame(other_data)
                        else:
                            other_data = getattr(materialized_other, "data", [])
                            if not other_data:
                                # Empty DataFrame - create from schema if available
                                if hasattr(materialized_other, "schema"):
                                    from .type_mapper import mock_type_to_polars_dtype

                                    schema_dict = {}
                                    for field in materialized_other.schema.fields:
                                        polars_dtype = mock_type_to_polars_dtype(
                                            field.dataType
                                        )
                                        schema_dict[field.name] = pl.Series(
                                            field.name, [], dtype=polars_dtype
                                        )
                                    other_df = pl.DataFrame(schema_dict)
                                else:
                                    other_df = pl.DataFrame()
                            else:
                                other_df = pl.DataFrame(other_data)
                    else:
                        other_data = getattr(other_df, "data", [])
                        if not other_data:
                            # Empty DataFrame - create from schema if available
                            if hasattr(other_df, "schema"):
                                from .type_mapper import mock_type_to_polars_dtype

                                schema_dict = {}
                                for field in other_df.schema.fields:
                                    polars_dtype = mock_type_to_polars_dtype(
                                        field.dataType
                                    )
                                    schema_dict[field.name] = pl.Series(
                                        field.name, [], dtype=polars_dtype
                                    )
                                other_df = pl.DataFrame(schema_dict)
                            else:
                                other_df = pl.DataFrame()
                        else:
                            other_df = pl.DataFrame(other_data)
                # Collect before joining - use df_materialized when available
                # (e.g. after select with computed columns like struct field + alias)
                if df_materialized is not None:
                    df_collected = df_materialized
                    df_materialized = None
                else:
                    df_collected = lazy_df.collect()
                result_df = self.operation_executor.apply_join(
                    df_collected, other_df, on=on, how=how
                )
                lazy_df = result_df.lazy()
            elif op_name == "union":
                # Union operation - need to collect first
                # Use df_materialized when available (e.g. after select with computed columns)
                if df_materialized is not None:
                    df_collected = df_materialized
                    df_materialized = None
                else:
                    df_collected = lazy_df.collect()
                other_df_payload = payload

                # Validate schema compatibility before union (PySpark compatibility)
                # current_schema is the schema after all previous operations
                if hasattr(other_df_payload, "schema"):
                    other_schema = other_df_payload.schema
                else:
                    # If other_df doesn't have schema, we can't validate - skip validation
                    # This shouldn't happen in normal usage
                    other_schema = None

                if other_schema is not None:
                    from ...dataframe.operations.set_operations import SetOperations
                    from ...core.exceptions.analysis import AnalysisException

                    # Check column count
                    if len(current_schema.fields) != len(other_schema.fields):
                        raise AnalysisException(
                            f"Union can only be performed on tables with the same number of columns, "
                            f"but the first table has {len(current_schema.fields)} columns and "
                            f"the second table has {len(other_schema.fields)} columns"
                        )

                    # Check column names and types
                    for i, (field1, field2) in enumerate(
                        zip(current_schema.fields, other_schema.fields)
                    ):
                        if field1.name != field2.name:
                            raise AnalysisException(
                                f"Union can only be performed on tables with compatible column names. "
                                f"Column {i} name mismatch: '{field1.name}' vs '{field2.name}'"
                            )

                        # Type compatibility check
                        if not SetOperations._are_types_compatible(
                            field1.dataType, field2.dataType
                        ):
                            raise AnalysisException(
                                f"Union can only be performed on tables with compatible column types. "
                                f"Column '{field1.name}' type mismatch: "
                                f"{field1.dataType} vs {field2.dataType}"
                            )

                # Convert other_df to Polars DataFrame if needed
                if not isinstance(other_df_payload, pl.DataFrame):
                    # Check if other_df_payload has lazy operations that need materialization
                    # (e.g., withColumn operations that create computed columns like struct field paths)
                    if (
                        hasattr(other_df_payload, "_operations_queue")
                        and other_df_payload._operations_queue
                    ):
                        # Materialize the other DataFrame to ensure computed columns are available
                        from ...dataframe.lazy import LazyEvaluationEngine

                        materialized_other = LazyEvaluationEngine.materialize(
                            other_df_payload
                        )
                        other_rows = materialized_other.collect()
                        other_schema = getattr(materialized_other, "schema", None)

                        def _row_to_dict(row: Any) -> Dict[str, Any]:
                            # Sparkless Row
                            if hasattr(row, "asDict") and callable(row.asDict):
                                return cast("Dict[str, Any]", row.asDict())
                            # Already a mapping
                            if isinstance(row, dict):
                                return cast("Dict[str, Any]", row)
                            # Sequence-like rows (e.g. tuple/list) with a known schema
                            if other_schema is not None and hasattr(
                                other_schema, "fields"
                            ):
                                try:
                                    values = list(row)
                                    return {
                                        field.name: values[i]
                                        for i, field in enumerate(other_schema.fields)
                                        if i < len(values)
                                    }
                                except Exception:
                                    pass
                            # Fallback: iterables of (k, v) pairs
                            return cast("Dict[str, Any]", dict(row))

                        other_data = (
                            [_row_to_dict(row) for row in other_rows]
                            if other_rows
                            else []
                        )
                    else:
                        other_data = getattr(other_df_payload, "data", [])

                    if not other_data:
                        # Empty DataFrame - create from schema if available
                        if hasattr(other_df_payload, "schema"):
                            from .type_mapper import mock_type_to_polars_dtype

                            schema_dict = {}
                            for field in other_df_payload.schema.fields:
                                polars_dtype = mock_type_to_polars_dtype(field.dataType)
                                schema_dict[field.name] = pl.Series(
                                    field.name, [], dtype=polars_dtype
                                )
                            other_df = pl.DataFrame(schema_dict)
                        else:
                            other_df = pl.DataFrame()
                    else:
                        # Create DataFrame from data
                        # Only enforce schema types for union operations to prevent type mismatches
                        other_df = pl.DataFrame(other_data)
                        if (
                            hasattr(other_df_payload, "schema")
                            and other_df_payload.schema.fields
                        ):
                            from .type_mapper import mock_type_to_polars_dtype

                            cast_exprs = []
                            for field in other_df_payload.schema.fields:
                                polars_dtype = mock_type_to_polars_dtype(field.dataType)
                                # Only cast if column exists and type doesn't match
                                # Only cast numeric types to prevent Int32/Int64 mismatches
                                # Only cast numeric types (Int32/Int64) to prevent union issues
                                # Don't cast string/datetime types as they can cause schema errors
                                if (
                                    field.name in other_df.columns
                                    and other_df[field.name].dtype != polars_dtype
                                    and polars_dtype
                                    in (pl.Int32, pl.Int64, pl.Float32, pl.Float64)
                                ):
                                    cast_exprs.append(
                                        pl.col(field.name).cast(polars_dtype)
                                    )

                            if cast_exprs:
                                other_df = other_df.with_columns(cast_exprs)
                else:
                    other_df = other_df_payload

                result_df = self.operation_executor.apply_union(df_collected, other_df)
                # Schema may change after union if type coercion occurred
                # PySpark normalizes types (e.g., LongType + StringType -> StringType)
                # Update current_schema to reflect coerced types from result_df
                from .type_mapper import polars_dtype_to_mock_type

                if result_df.schema:
                    # Extract schema from result DataFrame (reflects coerced types)
                    result_schema_fields = []
                    for col_name, dtype in result_df.schema.items():
                        mock_type = polars_dtype_to_mock_type(dtype)
                        from ...spark_types import StructField

                        result_schema_fields.append(
                            StructField(col_name, mock_type, nullable=True)
                        )
                    if result_schema_fields:
                        current_schema = StructType(result_schema_fields)

                lazy_df = result_df.lazy()
            elif op_name == "orderBy":
                # OrderBy operation - can be done lazily
                # Use df_materialized when available (e.g. after select with computed columns)
                if df_materialized is not None:
                    lazy_df = df_materialized.lazy()
                    df_materialized = None
                # Payload can be (columns, ascending) tuple or just columns (for backward compatibility)
                if isinstance(payload, tuple) and len(payload) == 2:
                    # New format: (columns, ascending)
                    columns_raw, ascending = payload
                    # Handle case where columns is a single list/tuple
                    if isinstance(columns_raw, (list, tuple)):
                        columns = tuple(columns_raw)
                    else:
                        columns = (columns_raw,)
                elif (
                    isinstance(payload, tuple)
                    and len(payload) == 1
                    and isinstance(payload[0], (list, tuple))
                ):
                    # Old format: tuple containing a single list/tuple
                    # (e.g., when df.sort(["col1", "col2"]) is called)
                    columns = tuple(payload[0])
                    ascending = True
                elif isinstance(payload, (tuple, list)):
                    # Old format: just columns tuple/list
                    columns = tuple(payload) if isinstance(payload, list) else payload
                    ascending = True
                else:
                    # Old format: single column
                    columns = (payload,)
                    ascending = True

                # Optimize orderBy columns to use computed columns if available
                optimized_columns = []
                for col in columns:
                    optimized_col = LazyEvaluationEngine._replace_with_computed_column(
                        col, computed_expressions
                    )
                    optimized_columns.append(optimized_col)

                # Build sort expressions with descending and nulls_last parameters
                # Polars sort() accepts:
                # - by: list of column names (str) or expressions
                # - descending: list of bools for direction
                # - nulls_last: list of bools for nulls handling
                # Get available columns from lazy DataFrame schema for case-insensitive matching
                available_columns = (
                    list(lazy_df.schema.keys()) if hasattr(lazy_df, "schema") else []
                )
                sort_by = []
                descending_flags = []
                nulls_last_flags = []
                for col in optimized_columns:
                    is_desc = False
                    nulls_last = None  # None means default behavior
                    col_name = None
                    if isinstance(col, str):
                        col_name = col
                        is_desc = not ascending
                        nulls_last = True  # PySpark default: nulls last
                    elif hasattr(col, "operation"):
                        operation = col.operation
                        col_name = (
                            col.column.name if hasattr(col, "column") else col.name
                        )
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
                        # For ColumnOperation with asc/desc, get the actual column name
                        if hasattr(col, "column") and hasattr(col.column, "name"):
                            col_name = col.column.name
                        elif hasattr(col, "name"):
                            col_name = col.name
                        else:
                            col_name = str(col)
                        # Remove any " ASC" or " DESC" suffix that might be in the name
                        col_name = (
                            col_name.replace(" ASC", "")
                            .replace(" DESC", "")
                            .replace(" NULLS FIRST", "")
                            .replace(" NULLS LAST", "")
                            .strip()
                        )
                        is_desc = not ascending
                        nulls_last = True  # PySpark default: nulls last

                    # Resolve column name using ColumnResolver if available columns are provided
                    if col_name is not None:
                        if available_columns:
                            from sparkless.core.column_resolver import ColumnResolver

                            # Get case sensitivity from session
                            case_sensitive = self._get_case_sensitive()
                            actual_col_name = ColumnResolver.resolve_column_name(
                                col_name, available_columns, case_sensitive
                            )
                            if actual_col_name:
                                col_name = actual_col_name

                        sort_by.append(col_name)
                        descending_flags.append(is_desc)
                        nulls_last_flags.append(nulls_last)

                if sort_by:
                    # Use sort() with by, descending, and nulls_last parameters
                    # nulls_last can be None (default), True, or False
                    # We only pass nulls_last if at least one value is not None
                    has_nulls_specification = any(
                        n is not None for n in nulls_last_flags
                    )
                    if has_nulls_specification:
                        lazy_df = lazy_df.sort(
                            sort_by,
                            descending=descending_flags,
                            nulls_last=nulls_last_flags,
                        )
                    else:
                        # No nulls specification, use default
                        lazy_df = lazy_df.sort(sort_by, descending=descending_flags)
            elif op_name == "limit":
                # Limit operation
                n = payload
                lazy_df = lazy_df.head(n)
            elif op_name == "offset":
                # Offset operation (skip first n rows)
                n = payload
                lazy_df = lazy_df.slice(n)
            elif op_name == "groupBy":
                # GroupBy operation - need to collect first
                # Use materialized DataFrame if available, otherwise collect from lazy
                if df_materialized is not None:
                    df_collected = df_materialized
                    df_materialized = None  # Clear after use
                elif lazy_df is not None:
                    df_collected = lazy_df.collect()
                else:
                    raise ValueError("No DataFrame available for groupBy operation")
                group_by, aggs = payload
                result_df = self.operation_executor.apply_group_by_agg(
                    df_collected, group_by, aggs
                )
                lazy_df = result_df.lazy()
            elif op_name == "distinct":
                # Distinct operation
                # For distinct to work correctly with computed columns (like struct field paths),
                # we need to ensure the DataFrame is materialized first
                # This is especially important when distinct follows a select operation
                # Use materialized DataFrame if available, otherwise collect from lazy
                if df_materialized is not None:
                    df_collected = df_materialized
                else:
                    df_collected = lazy_df.collect()
                result_df = df_collected.unique()
                lazy_df = result_df.lazy()
                df_materialized = result_df
            elif op_name == "drop":
                # Drop operation - need to handle Polars lazy evaluation limitation
                # Polars drops columns that depend on dropped columns during lazy evaluation
                # Solution: materialize ALL columns before dropping, then re-select what we need
                # This breaks the lazy dependency chain
                columns_to_drop = (
                    payload if isinstance(payload, (list, tuple)) else [payload]
                )

                # Filter out non-existent columns (PySpark allows dropping non-existent columns silently)
                # Resolve column names using ColumnResolver
                from sparkless.core.column_resolver import ColumnResolver

                available_cols_list = list(current_available_columns)
                # Get case sensitivity from session
                case_sensitive = self._get_case_sensitive()
                existing_columns_to_drop = []
                for col in columns_to_drop:
                    # Find actual column name using ColumnResolver
                    actual_col = ColumnResolver.resolve_column_name(
                        col, available_cols_list, case_sensitive
                    )
                    if actual_col:
                        existing_columns_to_drop.append(actual_col)

                # If no columns to actually drop, skip this operation
                if not existing_columns_to_drop:
                    continue

                columns_to_drop = existing_columns_to_drop

                # Find all columns that depend on the columns being dropped
                columns_to_preserve: Set[str] = set()
                for col_name, deps in column_dependencies.items():
                    # If this column depends on any column being dropped, we need to preserve it
                    if deps.intersection(columns_to_drop):
                        columns_to_preserve.add(col_name)

                # Also check subsequent operations for columns they need
                current_op_index = optimized_operations.index((op_name, payload))
                for future_op_name, future_payload in optimized_operations[
                    current_op_index + 1 :
                ]:
                    if future_op_name == "withColumn":
                        col_name, expr = future_payload
                        # Extract all columns this expression depends on
                        expr_deps = (
                            LazyEvaluationEngine._extract_all_column_dependencies(expr)
                        )
                        # If expr depends on a column that depends on dropped columns, preserve it
                        for dep_col in expr_deps:
                            if dep_col in column_dependencies:
                                dep_deps = column_dependencies[dep_col]
                                if dep_deps.intersection(columns_to_drop):
                                    columns_to_preserve.add(dep_col)
                            # Also check if dep_col is in current available columns
                            elif (
                                dep_col in current_available_columns
                                and dep_col in column_dependencies
                                and column_dependencies[dep_col].intersection(
                                    columns_to_drop
                                )
                            ):
                                # This column exists and is needed - preserve it if it depends on dropped columns
                                columns_to_preserve.add(dep_col)

                # Always materialize before dropping if there are subsequent operations
                # This ensures dependent columns are preserved even if dependency graph is incomplete
                if current_op_index + 1 < len(optimized_operations):
                    # Collect current state to materialize all columns
                    # Use materialized DataFrame if available, otherwise collect from lazy
                    if df_materialized is not None:
                        df_collected = df_materialized
                        df_materialized = None  # Clear after use
                    elif lazy_df is not None:
                        df_collected = lazy_df.collect()
                    else:
                        raise ValueError("No DataFrame available for drop operation")

                    # Drop columns using select to preserve schema correctly
                    # Using select instead of drop ensures schema is properly maintained
                    cols_to_keep = [
                        col
                        for col in df_collected.columns
                        if col not in columns_to_drop
                    ]

                    # Handle edge case: dropping all columns
                    # PySpark preserves row count even when all columns are dropped
                    if not cols_to_keep:
                        # Create empty DataFrame with same number of rows
                        # Use select with empty column list to preserve row structure
                        row_count = df_collected.height
                        # Create a dummy column, select it, then drop it to preserve row count
                        # Actually, Polars can't represent a DataFrame with rows but no columns
                        # So we create a single dummy column with null values
                        df_collected = pl.DataFrame({"_dummy": [None] * row_count})
                        # But wait, PySpark behavior is to return empty schema but preserve rows
                        # For now, we'll use the dummy column approach and handle it in schema projection
                        # The schema will show no columns, but the row count will be preserved
                    else:
                        df_collected = df_collected.select(cols_to_keep)

                    # Store the materialized DataFrame instead of converting back to lazy
                    # This avoids schema mismatch issues when subsequent operations (like withColumn)
                    # need to collect again. They can use the materialized DataFrame directly.
                    # Only convert to lazy if the next operation requires it (like filter)
                    next_op_index = current_op_index + 1
                    if next_op_index < len(optimized_operations):
                        next_op_name, _ = optimized_operations[next_op_index]
                        # Operations that can work with lazy frames (don't collect)
                        lazy_ops = {
                            "filter",
                            "distinct",
                            "limit",
                            "offset",
                            "orderBy",
                            "sort",
                        }
                        if next_op_name in lazy_ops:
                            # Next operation works with lazy, so convert back to lazy
                            # Create a fresh lazy frame by converting to dicts and back to avoid schema issues
                            lazy_df = df_collected.lazy()
                            df_materialized = None
                        else:
                            # Next operation will collect anyway (withColumn, select, etc.)
                            # Keep it materialized to avoid schema issues when converting back to lazy
                            # Store the materialized DataFrame directly - no conversion to dicts needed
                            # as we'll use it directly in the next operation
                            df_materialized = df_collected
                            # Don't create lazy_df here - we'll use df_materialized directly
                            # Set lazy_df to None or keep it as is, but operations will check df_materialized first
                            lazy_df = (
                                df_collected.lazy()
                            )  # Still need for final collection if no more ops
                    else:
                        # No more operations, convert back to lazy for final collection
                        lazy_df = df_collected.lazy()
                        df_materialized = None
                elif columns_to_preserve:
                    # No future operations but we have columns to preserve
                    # Use materialized DataFrame if available, otherwise collect from lazy
                    if df_materialized is not None:
                        df_collected = df_materialized
                        df_materialized = None  # Clear after use
                    elif lazy_df is not None:
                        df_collected = lazy_df.collect()
                    else:
                        raise ValueError("No DataFrame available for drop operation")
                    current_cols = set(df_collected.columns)
                    cols_to_keep = list(current_cols - set(columns_to_drop))
                    if not cols_to_keep:
                        # All columns dropped - preserve row count
                        row_count = df_collected.height
                        df_collected = pl.DataFrame({"_dummy": [None] * row_count})
                    else:
                        df_collected = df_collected.select(cols_to_keep)
                    lazy_df = df_collected.lazy()
                else:
                    # No dependencies and no future operations - can drop directly
                    # But we still need to handle non-existent columns and all-columns case
                    # Use materialized DataFrame if available, otherwise collect from lazy
                    if df_materialized is not None:
                        df_collected = df_materialized
                        df_materialized = None  # Clear after use
                    elif lazy_df is not None:
                        df_collected = lazy_df.collect()
                    else:
                        raise ValueError("No DataFrame available for drop operation")
                    cols_to_keep = [
                        col
                        for col in df_collected.columns
                        if col not in columns_to_drop
                    ]
                    if not cols_to_keep:
                        # All columns dropped - preserve row count
                        row_count = df_collected.height
                        df_collected = pl.DataFrame({"_dummy": [None] * row_count})
                    else:
                        df_collected = df_collected.select(cols_to_keep)
                    # Store materialized for final collection
                    df_materialized = df_collected
                    lazy_df = None

                # Update available columns after drop
                for col in columns_to_drop:
                    current_available_columns.discard(col)

                # Update schema after drop
                from ...dataframe.schema.schema_manager import SchemaManager

                # Get case sensitivity from session for schema projection
                case_sensitive = self._get_case_sensitive()
                current_schema = SchemaManager.project_schema_with_operations(
                    current_schema, [(op_name, payload)], case_sensitive
                )
            elif op_name == "withColumnRenamed":
                # WithColumnRenamed operation
                # If we have a materialized DataFrame, rename directly in it to preserve columns
                # Otherwise, rename in the lazy DataFrame
                old_name, new_name = payload
                if df_materialized is not None:
                    # Rename directly in the materialized DataFrame to ensure all columns are preserved
                    df_materialized = df_materialized.rename({old_name: new_name})
                    # Update lazy_df to match (for consistency)
                    lazy_df = df_materialized.lazy()
                    # Keep df_materialized set if there are more operations
                    # This ensures columns are preserved for subsequent operations
                    next_op_index = current_op_index + 1
                    if next_op_index < len(optimized_operations):
                        next_op_name, _ = optimized_operations[next_op_index]
                        # Only convert to lazy for operations that explicitly need it
                        lazy_ops = {
                            "filter",
                            "distinct",
                            "limit",
                            "offset",
                            "orderBy",
                            "sort",
                        }
                        if next_op_name in lazy_ops:
                            # Next operation needs lazy - convert
                            lazy_df = df_materialized.lazy()
                            df_materialized = None
                        # Otherwise, keep df_materialized set for operations like select, withColumn, withColumnRenamed
                    # If no more operations, keep df_materialized for final collection
                else:
                    # Rename in the lazy DataFrame
                    lazy_df = lazy_df.rename({old_name: new_name})
                # Update schema and available columns after rename
                from ...dataframe.schema.schema_manager import SchemaManager

                # Get case sensitivity from session for schema projection
                case_sensitive = self._get_case_sensitive()
                current_schema = SchemaManager.project_schema_with_operations(
                    current_schema, [(op_name, payload)], case_sensitive
                )
                # Update available columns set
                if old_name in current_available_columns:
                    current_available_columns.remove(old_name)
                    current_available_columns.add(new_name)
            else:
                raise ValueError(f"Unsupported operation: {op_name}")

        # Materialize (collect) the lazy DataFrame
        # Use materialized DataFrame if available, otherwise collect from lazy
        if df_materialized is not None:
            result_df = df_materialized
        elif lazy_df is not None:
            result_df = lazy_df.collect()
        else:
            # Should not happen, but handle gracefully
            raise ValueError("No DataFrame to materialize")

        # Convert to List[Row]
        # Handle special case: if all columns were dropped, result_df may have a _dummy column
        # We need to convert this to empty dicts to match PySpark's behavior
        if "_dummy" in result_df.columns and len(result_df.columns) == 1:
            # All columns were dropped - create empty rows preserving row count
            rows = []
            row_count = result_df.height
            for _ in range(row_count):
                rows.append(Row({}, schema=None))
            return rows

        # For joins with duplicate columns, Polars uses _right suffix
        # We need to convert these to match PySpark's duplicate column handling
        rows = []

        # Convert Polars DataFrame to dicts and preserve date/timestamp types
        # Polars to_dicts() converts dates to strings, we need to convert them back
        import datetime as dt_module
        from .type_mapper import polars_dtype_to_mock_type
        from sparkless.spark_types import DateType, TimestampType

        # Get column types from Polars DataFrame schema
        polars_schema = result_df.schema
        column_types = {
            col: polars_dtype_to_mock_type(dtype)
            for col, dtype in polars_schema.items()
        }

        for row_dict in result_df.to_dicts():
            # Convert date/timestamp strings back to date/datetime objects
            # Polars to_dicts() converts dates to ISO format strings
            converted_row_dict: Dict[str, Any] = {}
            for col, value in row_dict.items():
                # Always preserve dict values - they represent structs (from Object dtype or StructType)
                if isinstance(value, dict):
                    converted_row_dict[col] = value
                    continue

                col_type = column_types.get(col)
                # Handle Object dtype - preserve Python dicts/objects as-is
                # Check if this column has Object dtype in the Polars DataFrame
                try:
                    if col in result_df.columns:
                        polars_dtype = result_df[col].dtype
                        if polars_dtype == pl.Object:
                            # This is an Object dtype column - preserve the value as-is
                            converted_row_dict[col] = value
                            continue
                except (KeyError, AttributeError, IndexError):
                    # Column might not exist or dtype check failed - continue with normal processing
                    pass
                # Also check for empty StructType (fallback for Object dtype mapping)
                if (
                    col_type is not None
                    and isinstance(col_type, StructType)
                    and len(col_type.fields) == 0
                ):
                    # This is an Object dtype that was converted to empty StructType
                    # Preserve the value as-is (should be a dict)
                    converted_row_dict[col] = value
                    continue
                elif isinstance(col_type, DateType) and isinstance(value, str):
                    # Convert ISO date string back to date object
                    try:
                        converted_row_dict[col] = dt_module.date.fromisoformat(value)
                    except (ValueError, AttributeError):
                        # If parsing fails, keep as string
                        converted_row_dict[col] = value
                elif isinstance(col_type, TimestampType) and isinstance(value, str):
                    # Convert ISO timestamp string back to datetime object
                    try:
                        # Handle various ISO formats
                        if "T" in value:
                            converted_row_dict[col] = dt_module.datetime.fromisoformat(
                                value.replace("Z", "+00:00")
                            )
                        else:
                            converted_row_dict[col] = dt_module.datetime.fromisoformat(
                                value
                            )
                    except (ValueError, AttributeError):
                        # If parsing fails, keep as string
                        converted_row_dict[col] = value
                else:
                    converted_row_dict[col] = value

            # Create Row from dict - Row will handle the conversion
            # The schema will be applied later in _convert_materialized_rows
            rows.append(Row(converted_row_dict, schema=None))

        return rows

    def _has_window_function(self, expr: Any) -> bool:
        """Check if expression contains WindowFunction objects that require manual materialization.

        Args:
            expr: Expression to check (Column, ColumnOperation, WindowFunction, or nested structure)

        Returns:
            True if expression contains WindowFunction objects
        """
        # Check if this is a WindowFunction directly
        if hasattr(expr, "__class__") and expr.__class__.__name__ == "WindowFunction":
            return True
        # Also try isinstance check as backup
        try:
            from sparkless.functions.window_execution import WindowFunction

            if isinstance(expr, WindowFunction):
                return True
        except (ImportError, AttributeError):
            pass
        # Recursively check nested expressions
        if hasattr(expr, "column") and self._has_window_function(expr.column):
            return True
        if hasattr(expr, "value") and self._has_window_function(expr.value):
            return True
        return bool(
            hasattr(expr, "function") and self._has_window_function(expr.function)
        )

    def _has_expr_expression(self, expr: Any) -> bool:
        """Check if expression contains F.expr() or complex operations that need manual materialization.

        Args:
            expr: Expression to check (Column, ColumnOperation, or nested structure)

        Returns:
            True if expression contains operations that require manual materialization
        """
        # Check if this expression was created by F.expr()
        if hasattr(expr, "_from_expr") and expr._from_expr:
            return True
        # Check if this is a ColumnOperation with expr operation
        if hasattr(expr, "operation"):
            # Check for direct expr operation (old F.expr() style)
            if expr.operation == "expr":
                return True
            # Check for function_name="expr" (another F.expr() marker)
            if hasattr(expr, "function_name") and expr.function_name == "expr":
                return True
            # Recursively check nested expressions
            if hasattr(expr, "column") and self._has_expr_expression(expr.column):
                return True
            if hasattr(expr, "value") and self._has_expr_expression(expr.value):
                return True
        # Check if this is a Column (simple reference, no issue)
        elif hasattr(expr, "name") and not hasattr(expr, "operation"):
            return False
        return False

    def _has_unsupported_operation(self, expr: Any) -> bool:
        """Check if expression contains unsupported operations.

        Args:
            expr: Expression to check (Column, ColumnOperation, or nested structure)

        Returns:
            True if expression contains unsupported operations
        """
        # Check if this is a ColumnOperation with an unsupported operation
        if hasattr(expr, "operation") and expr.operation in self.UNSUPPORTED_OPERATIONS:
            return True
        # Recursively check nested expressions
        if hasattr(expr, "column") and self._has_unsupported_operation(expr.column):
            return True
        if hasattr(expr, "value") and self._has_unsupported_operation(expr.value):
            return True
        return bool(
            hasattr(expr, "function") and self._has_unsupported_operation(expr.function)
        )

    def can_handle_operation(self, op_name: str, op_payload: Any) -> bool:
        """Check if this materializer can handle a specific operation.

        Args:
            op_name: Name of the operation (e.g., "to_timestamp", "filter")
            op_payload: Operation payload (operation-specific)

        Returns:
            True if the materializer can handle this operation, False otherwise
        """
        # Check unsupported operations first
        if op_name in self.UNSUPPORTED_OPERATIONS:
            return False

        # For complex operations, inspect payload for unsupported nested operations
        if op_name == "select":
            # Payload is a list of column expressions
            if isinstance(op_payload, (list, tuple)):
                for col in op_payload:
                    # Window functions (incl. arithmetic) handled by operation_executor.apply_select
                    # Check for unsupported operations only
                    if self._has_unsupported_operation(col):
                        return False
            return op_name in self.SUPPORTED_OPERATIONS

        elif op_name == "withColumn":
            # Payload is (col_name, expression)
            if isinstance(op_payload, (list, tuple)) and len(op_payload) == 2:
                _, expression = op_payload
                # Window functions handled by operation_executor.apply_withColumn
                if self._has_unsupported_operation(expression):
                    return False
            return op_name in self.SUPPORTED_OPERATIONS

        elif op_name == "filter":
            # Payload is filter expression
            # Check for F.expr() expressions
            if self._has_expr_expression(op_payload):
                return False
            # Check for unsupported operations
            if self._has_unsupported_operation(op_payload):
                return False
            return op_name in self.SUPPORTED_OPERATIONS

        # For simple operations, check supported operations set
        # Default: assume unsupported for safety
        return op_name in self.SUPPORTED_OPERATIONS

    def can_handle_operations(
        self, operations: List[Tuple[str, Any]]
    ) -> Tuple[bool, List[str]]:
        """Check if this materializer can handle a list of operations.

        Args:
            operations: List of (operation_name, payload) tuples

        Returns:
            Tuple of (can_handle_all, unsupported_operations)
            - can_handle_all: True if all operations are supported
            - unsupported_operations: List of operation names that are unsupported
        """
        unsupported_operations: List[str] = []
        for op_name, op_payload in operations:
            if not self.can_handle_operation(op_name, op_payload):
                unsupported_operations.append(op_name)

        can_handle_all = len(unsupported_operations) == 0
        return (can_handle_all, unsupported_operations)

    def close(self) -> None:
        """Close the materializer and clean up resources."""
        # Polars doesn't require explicit cleanup
        pass
