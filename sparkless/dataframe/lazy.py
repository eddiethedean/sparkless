"""
from __future__ import annotations
Lazy Evaluation Engine for DataFrames

This module handles lazy evaluation, operation queuing, and materialization
for DataFrame. Extracted from dataframe.py to improve organization.
"""

import contextlib
from typing import Sequence
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Tuple, cast
from ..spark_types import (
    StringType,
    StructField,
    DoubleType,
    LongType,
    IntegerType,
    BooleanType,
    DataType,
    StructType,
    ArrayType,
)
from ..optimizer.query_optimizer import OperationType

if TYPE_CHECKING:
    from sparkless.dataframe import DataFrame
    from .protocols import SupportsDataFrameOps


class LazyEvaluationEngine:
    """Handles lazy evaluation and materialization for DataFrames."""

    def __init__(self, enable_optimization: bool = True):
        """Initialize lazy evaluation engine.

        Args:
            enable_optimization: Whether to enable query optimization
        """
        self.enable_optimization = enable_optimization
        self._optimizer = None
        if enable_optimization:
            try:
                from ..optimizer import QueryOptimizer

                self._optimizer = QueryOptimizer()
            except ImportError:
                # Fallback if optimizer is not available
                self._optimizer = None

    @staticmethod
    def _normalize_expression(expr: Any) -> str:
        """Create a canonical representation of an expression for comparison.

        Args:
            expr: Expression to normalize (ColumnOperation, Column, etc.)

        Returns:
            Canonical string representation of the expression
        """
        from ..functions import Column, ColumnOperation, Literal

        if isinstance(expr, str):
            return f"col({expr})"
        elif isinstance(expr, Column):
            return f"col({expr.name})"
        elif isinstance(expr, Literal):
            value = expr.value
            if isinstance(value, str):
                return f"lit('{value}')"
            else:
                return f"lit({value})"
        elif isinstance(expr, ColumnOperation):
            operation = expr.operation
            # Handle desc/asc operations specially - they wrap the underlying expression
            if operation in ("desc", "asc"):
                # Normalize the underlying column, ignoring the sort direction
                return LazyEvaluationEngine._normalize_expression(expr.column)
            # Normalize column and value recursively
            col_part = LazyEvaluationEngine._normalize_expression(expr.column)
            if hasattr(expr, "value") and expr.value is not None:
                value_part = LazyEvaluationEngine._normalize_expression(expr.value)
                # For commutative operations (*, +), normalize order
                if operation in ("*", "+"):
                    # Create sorted tuple for normalization
                    parts = sorted([col_part, value_part])
                    return f"{operation}({parts[0]}, {parts[1]})"
                else:
                    return f"{operation}({col_part}, {value_part})"
            else:
                return f"{operation}({col_part})"
        else:
            # Fallback: string representation
            return str(expr)

    @staticmethod
    def _build_computed_expressions_map(
        operations: List[Tuple[str, Any]],
    ) -> Dict[str, str]:
        """Build a mapping from expression signatures to column names.

        Args:
            operations: List of (operation_name, payload) tuples

        Returns:
            Dictionary mapping expression signatures to column names
        """
        computed_expressions: Dict[str, str] = {}
        for op_name, payload in operations:
            if op_name == "withColumn":
                column_name, expression = payload
                expr_signature = LazyEvaluationEngine._normalize_expression(expression)
                computed_expressions[expr_signature] = column_name
        return computed_expressions

    @staticmethod
    def _replace_with_computed_column(
        expr: Any, computed_expressions: Dict[str, str]
    ) -> Any:
        """Replace expression with computed column reference if match found.

        Args:
            expr: Expression to potentially replace
            computed_expressions: Mapping from expression signatures to column names

        Returns:
            Original expression or Column reference to computed column
        """
        from ..functions import Column, ColumnOperation

        # Handle desc/asc operations specially - check underlying expression first
        if isinstance(expr, ColumnOperation) and expr.operation in ("desc", "asc"):
            # Replace the underlying expression, then reapply desc/asc if needed
            underlying_expr = LazyEvaluationEngine._replace_with_computed_column(
                expr.column, computed_expressions
            )
            # If underlying expression was replaced with a column, wrap it
            if (
                isinstance(underlying_expr, Column)
                and underlying_expr is not expr.column
            ):
                from ..functions.core.column import ColumnOperation

                # expr.operation is guaranteed to be a string in ColumnOperation
                op_str: str = cast("str", expr.operation)
                return ColumnOperation(underlying_expr, op_str, None)
            # Otherwise return original if no match found
            return expr

        expr_signature = LazyEvaluationEngine._normalize_expression(expr)
        if expr_signature in computed_expressions:
            # Replace with column reference
            column_name = computed_expressions[expr_signature]
            return Column(column_name)
        elif isinstance(expr, ColumnOperation):
            # Recursively check nested expressions
            if hasattr(expr, "column"):
                new_column = LazyEvaluationEngine._replace_with_computed_column(
                    expr.column, computed_expressions
                )
                if new_column is not expr.column:
                    # Create new ColumnOperation with replaced column
                    from ..functions.core.column import ColumnOperation
                    from typing import cast

                    # expr.operation is guaranteed to be a string in ColumnOperation
                    new_op_str: str = cast("str", expr.operation)
                    new_op = ColumnOperation(
                        new_column, new_op_str, getattr(expr, "value", None)
                    )
                    # Check if this new expression matches a computed column
                    new_expr_signature = LazyEvaluationEngine._normalize_expression(
                        new_op
                    )
                    if new_expr_signature in computed_expressions:
                        column_name = computed_expressions[new_expr_signature]
                        return Column(column_name)
                    return new_op
            if hasattr(expr, "value") and expr.value is not None:
                new_value = LazyEvaluationEngine._replace_with_computed_column(
                    expr.value, computed_expressions
                )
                if new_value is not expr.value:
                    # Create new ColumnOperation with replaced value
                    from ..functions.core.column import ColumnOperation
                    from typing import cast

                    # expr.operation is guaranteed to be a string in ColumnOperation
                    new_op_str2: str = cast("str", expr.operation)
                    new_op = ColumnOperation(expr.column, new_op_str2, new_value)
                    # Check if this new expression matches a computed column
                    new_expr_signature = LazyEvaluationEngine._normalize_expression(
                        new_op
                    )
                    if new_expr_signature in computed_expressions:
                        column_name = computed_expressions[new_expr_signature]
                        return Column(column_name)
                    return new_op
        return expr

    @staticmethod
    def _extract_column_names(expr: Any, available_columns: Set[str]) -> Set[str]:
        """Extract column names referenced in an expression that aren't in available_columns.

        Args:
            expr: Expression to analyze
            available_columns: Set of columns available in current schema

        Returns:
            Set of column names that don't exist in available_columns (from original DataFrame)
        """
        from ..functions import Column, ColumnOperation, Literal

        missing_columns: Set[str] = set()

        if isinstance(expr, str):
            # String column name
            if expr not in available_columns:
                missing_columns.add(expr)
        elif isinstance(expr, Column):
            # Column reference
            if expr.name not in available_columns:
                missing_columns.add(expr.name)
        elif isinstance(expr, Literal):
            # Literal - no column reference
            pass
        elif isinstance(expr, ColumnOperation):
            # Recursively check nested expressions
            if hasattr(expr, "column"):
                missing_columns.update(
                    LazyEvaluationEngine._extract_column_names(
                        expr.column, available_columns
                    )
                )
            if hasattr(expr, "value") and expr.value is not None:
                missing_columns.update(
                    LazyEvaluationEngine._extract_column_names(
                        expr.value, available_columns
                    )
                )

        return missing_columns

    @staticmethod
    def _extract_all_column_dependencies(expr: Any) -> Set[str]:
        """Extract all column names that an expression depends on.

        Args:
            expr: Expression to analyze

        Returns:
            Set of all column names referenced in the expression
        """
        from ..functions import Column, ColumnOperation, Literal

        dependencies: Set[str] = set()

        if isinstance(expr, str):
            # String column name
            dependencies.add(expr)
        elif isinstance(expr, Column):
            # Column reference
            dependencies.add(expr.name)
        elif isinstance(expr, Literal):
            # Literal - no column reference
            pass
        elif isinstance(expr, ColumnOperation):
            # Recursively check nested expressions
            if hasattr(expr, "column"):
                dependencies.update(
                    LazyEvaluationEngine._extract_all_column_dependencies(expr.column)
                )
            if hasattr(expr, "value") and expr.value is not None:
                dependencies.update(
                    LazyEvaluationEngine._extract_all_column_dependencies(expr.value)
                )

        return dependencies

    @staticmethod
    def _build_column_dependency_graph(
        operations: List[Tuple[str, Any]], available_columns: Set[str]
    ) -> Dict[str, Set[str]]:
        """Build a graph of column dependencies.

        Args:
            operations: List of (operation_name, payload) tuples
            available_columns: Set of available column names (from projected schema, not just original)

        Returns:
            Dictionary mapping column names to sets of columns they depend on
        """
        dependencies: Dict[str, Set[str]] = {}
        current_columns: Set[str] = available_columns.copy()

        for op_name, payload in operations:
            if op_name == "withColumn":
                column_name, expression = payload
                # Extract all columns this expression depends on
                # Only include columns that exist in current_columns (not computed ones from future)
                deps = LazyEvaluationEngine._extract_all_column_dependencies(expression)
                # Filter to only include columns that exist at this point
                deps = deps.intersection(current_columns)
                dependencies[column_name] = deps
                current_columns.add(column_name)
            elif op_name == "select":
                # Select creates new columns - extract dependencies for each
                for col in payload:
                    if hasattr(col, "name"):
                        col_name = col.name
                        deps = LazyEvaluationEngine._extract_all_column_dependencies(
                            col
                        )
                        # Filter to only include columns that exist at this point
                        deps = deps.intersection(current_columns)
                        dependencies[col_name] = deps
                        current_columns.add(col_name)
            elif op_name == "drop":
                # Drop removes columns
                columns_to_drop = (
                    payload if isinstance(payload, (list, tuple)) else [payload]
                )
                for col in columns_to_drop:
                    current_columns.discard(col)
                    # Also remove from dependencies dict if it's there
                    if col in dependencies:
                        del dependencies[col]

        return dependencies

    @staticmethod
    def queue_operation(df: "DataFrame", op_name: str, payload: Any) -> "DataFrame":
        """Queue an operation for lazy evaluation.

        Args:
            df: Source DataFrame
            op_name: Operation name (select, filter, join, etc.)
            payload: Operation parameters

        Returns:
            New DataFrame with queued operation
        """
        from ..dataframe import DataFrame

        # Infer new schema for operations that change schema
        new_schema = df.schema
        if op_name == "select":
            new_schema = LazyEvaluationEngine._infer_select_schema(df, payload)
        elif op_name == "join":
            new_schema = LazyEvaluationEngine._infer_join_schema(df, payload)
        elif op_name == "withColumn":
            new_schema = LazyEvaluationEngine._infer_withcolumn_schema(df, payload)

        new_operations = df._operations_queue + [(op_name, payload)]

        return DataFrame(
            df.data,
            new_schema,
            df.storage,
            operations=new_operations,
        )

    def optimize_operations(
        self, operations: List[Tuple[str, Any]]
    ) -> List[Tuple[str, Any]]:
        """Optimize operations using the query optimizer.

        Args:
            operations: List of (operation_name, payload) tuples

        Returns:
            Optimized list of operations
        """
        if not self.enable_optimization or self._optimizer is None:
            return operations

        try:
            # Convert operations to optimizer format
            optimizer_ops = self._convert_to_optimizer_operations(operations)

            # Apply optimization
            optimized_ops = self._optimizer.optimize(optimizer_ops)

            # Convert back to original format
            return self._convert_from_optimizer_operations(optimized_ops)
        except Exception:
            # If optimization fails, return original operations
            return operations

    def _convert_to_optimizer_operations(
        self, operations: List[Tuple[str, Any]]
    ) -> List[Any]:
        """Convert operations to optimizer format."""
        from ..optimizer.query_optimizer import Operation, OperationType

        optimizer_ops = []
        for op_name, payload in operations:
            if op_name == "select":
                optimizer_ops.append(
                    Operation(
                        type=OperationType.SELECT,
                        columns=payload if isinstance(payload, list) else [payload],
                        predicates=[],
                        join_conditions=[],
                        group_by_columns=[],
                        order_by_columns=[],
                        limit_count=None,
                        window_specs=[],
                        metadata={},
                    )
                )
            elif op_name == "filter":
                optimizer_ops.append(
                    Operation(
                        type=OperationType.FILTER,
                        columns=[],
                        predicates=[
                            {"column": str(payload), "operator": "=", "value": True}
                        ],
                        join_conditions=[],
                        group_by_columns=[],
                        order_by_columns=[],
                        limit_count=None,
                        window_specs=[],
                        metadata={},
                    )
                )
            # Add more operation types as needed

        return optimizer_ops

    def _convert_from_optimizer_operations(
        self, optimizer_ops: List[Any]
    ) -> List[Tuple[str, Any]]:
        """Convert optimizer operations back to original format."""
        operations = []
        for op in optimizer_ops:
            if op.type == OperationType.SELECT:
                operations.append(("select", op.columns))
            elif op.type == OperationType.FILTER:
                for pred in op.predicates:
                    operations.append(("filter", pred["column"]))
            # Add more operation types as needed

        return operations

    @staticmethod
    def materialize(df: "DataFrame") -> "DataFrame":
        """Materialize queued lazy operations.

        Args:
            df: Lazy DataFrame with queued operations

        Returns:
            Eager DataFrame with operations applied
        """
        if not df._operations_queue:
            from ..dataframe import DataFrame

            return DataFrame(df.data, df.schema, df.storage)

        # Check if operations require manual materialization
        if LazyEvaluationEngine._requires_manual_materialization(df._operations_queue):
            return LazyEvaluationEngine._materialize_manual(df)
        # Use backend factory to get materializer
        try:
            from sparkless.backend.factory import BackendFactory

            # Detect backend type from DataFrame's storage
            backend_type = BackendFactory.get_backend_type(df.storage)
            materializer = BackendFactory.create_materializer(backend_type)
            try:
                # Check capabilities upfront before materialization
                can_handle_all, unsupported_ops = materializer.can_handle_operations(
                    df._operations_queue
                )

                if not can_handle_all:
                    # Fail fast with clear error message
                    from ..core.exceptions.operation import (
                        SparkUnsupportedOperationError,
                    )

                    raise SparkUnsupportedOperationError(
                        operation=f"Operations: {', '.join(unsupported_ops)}",
                        reason=f"Backend '{backend_type}' does not support these operations",
                        alternative="Consider using _materialize_manual() or a different backend",
                    )

                # Compute final schema after all operations
                from ..dataframe.schema.schema_manager import SchemaManager

                final_schema = SchemaManager.project_schema_with_operations(
                    df.schema, df._operations_queue
                )

                # Let materializer optimize and execute the operations
                rows = materializer.materialize(
                    df.data, df.schema, df._operations_queue
                )

                # Convert rows back to data format using final schema
                materialized_data = LazyEvaluationEngine._convert_materialized_rows(
                    rows, final_schema
                )

                # Post-process for cached string concatenation compatibility
                # If DataFrame is cached, set string concatenation results to None
                if getattr(df, "_is_cached", False):
                    materialized_data = (
                        LazyEvaluationEngine._handle_cached_string_concatenation(
                            materialized_data, df._operations_queue, final_schema
                        )
                    )

                # Update schema to match actual columns in materialized data
                # This handles Polars deduplication/renaming of duplicate columns.
                # Python dicts can only have unique keys, so materialized_data has deduplicated columns.
                # The schema should match the actual data structure after materialization.
                if materialized_data:
                    actual_data_keys = set(materialized_data[0].keys())
                    schema_column_names = {f.name for f in final_schema.fields}
                    # Check if schema has duplicates (length of fields list > length of unique names set)
                    # or if the unique column sets don't match
                    schema_has_duplicates = len(final_schema.fields) > len(
                        schema_column_names
                    )
                    if schema_has_duplicates or actual_data_keys != schema_column_names:
                        # Schema has duplicates but data is deduplicated - update schema to match data
                        from ..core.schema_inference import infer_schema_from_data

                        try:
                            final_schema = infer_schema_from_data(materialized_data)
                        except ValueError:
                            # If schema inference fails (e.g., empty data with complex types),
                            # keep the projected schema but deduplicate it
                            # Build deduplicated schema from projected schema
                            seen_names = set()
                            deduplicated_fields = []
                            for field in final_schema.fields:
                                if field.name not in seen_names:
                                    deduplicated_fields.append(field)
                                    seen_names.add(field.name)
                            from ..spark_types import StructType

                            final_schema = StructType(deduplicated_fields)

                # Create new eager DataFrame with materialized data and final schema
                # IMPORTANT: Clear operations queue since all operations have been materialized
                # Also preserve cached state for PySpark compatibility
                from ..dataframe import DataFrame

                result_df = DataFrame(
                    materialized_data,
                    final_schema,
                    df.storage,
                    operations=[],  # Clear operations queue - all operations have been applied
                )

                # Preserve cached state
                result_df._is_cached = getattr(df, "_is_cached", False)

                return result_df
            finally:
                materializer.close()

        except ImportError:
            # Fallback to manual materialization if backend is not available
            return LazyEvaluationEngine._materialize_manual(df)

    @staticmethod
    def _handle_cached_string_concatenation(
        data: List[Dict[str, Any]],
        operations: List[Tuple[str, Any]],
        schema: "StructType",
    ) -> List[Dict[str, Any]]:
        """Post-process materialized data to handle cached string concatenation.

        When a DataFrame is cached, string concatenation with + operator should
        return None to match PySpark behavior.

        Args:
            data: Materialized data
            operations: List of operations that were applied
            schema: Final schema

        Returns:
            Post-processed data with string concatenation results set to None

        Note:
            This implementation uses a heuristic-based approach to detect string
            concatenation by looking for the "+" operator in expressions. The heuristic
            marks any "+" operation as potential string concatenation, then verifies
            the result is actually a string before setting it to None.

            Limitations of the heuristic approach:
            - Cannot distinguish between string concatenation and numeric addition at
              the expression level (relies on runtime type checking of results)
            - Nested operations are detected recursively, but complex expression trees
              might have edge cases
            - Only detects string concatenation in withColumn operations, not in other
              contexts where + might be used

            This matches PySpark's behavior where string concatenation with + returns
            None for cached DataFrames. For reliable string concatenation, use F.concat()
            instead of the + operator.
        """
        # Find columns that are the result of string concatenation with +
        # by examining withColumn operations
        string_concat_columns = set()

        def _has_string_concatenation(expr: Any) -> bool:
            """Recursively check if expression contains string concatenation with +.

            This is a heuristic that detects the "+" operator in expressions. It cannot
            distinguish between string concatenation and numeric addition at the expression
            level, so the caller verifies the result is actually a string before applying
            the cache behavior.

            Args:
                expr: Expression to check

            Returns:
                True if expression contains a "+" operation (potential string concat)
            """
            if not hasattr(expr, "operation"):
                return False

            if expr.operation == "+":
                # Mark any + operation as potential string concat
                # The caller will verify the result is actually a string
                return True

            # Check nested operations recursively
            if (
                hasattr(expr, "column")
                and hasattr(expr.column, "operation")
                and _has_string_concatenation(expr.column)
            ):
                return True
            return (
                hasattr(expr, "value")
                and hasattr(expr.value, "operation")
                and _has_string_concatenation(expr.value)
            )

        for op_name, payload in operations:
            if op_name == "withColumn":
                col_name, expression = payload
                # Check if expression contains string concatenation with +
                if _has_string_concatenation(expression):
                    string_concat_columns.add(col_name)

        # Set string concatenation columns to None in cached DataFrames
        # Only set to None if the result is actually a string (to avoid affecting numeric addition)
        if string_concat_columns:
            for row in data:
                for col_name in string_concat_columns:
                    # Verify the result is actually a string before setting to None
                    # This prevents numeric addition (which also uses +) from being affected
                    if col_name in row and isinstance(row[col_name], str):
                        row[col_name] = None

        return data

    @staticmethod
    def _has_default_values(data: List[Dict[str, Any]], schema: "StructType") -> bool:
        """Check if data contains default values that indicate backend couldn't handle the operations.

        .. deprecated::
            This heuristic-based method is deprecated in favor of explicit capability checks
            via materializer.can_handle_operations(). It may be removed in a future version.
        """
        if not data:
            # Empty data could be valid (e.g., filter with no matches, join with no matches)
            # Only return True if we have a schema but no data AND schema has fields
            # This is a heuristic - empty result might be valid
            return False

        # First, check for unevaluated operation objects (e.g., WindowFunction objects)
        # This is a clear indicator that the backend couldn't handle the operation
        # and returned the operation object instead of evaluating it
        for row in data:
            for field in schema.fields:
                if field.name in row:
                    value = row[field.name]
                    # Check if value is an unevaluated operation object
                    # WindowFunction objects should be evaluated, not returned as-is
                    # Check by class name first (more reliable than isinstance due to import timing)
                    if hasattr(value, "__class__"):
                        class_name = value.__class__.__name__
                        if class_name == "WindowFunction":
                            # Backend returned WindowFunction object instead of evaluating it
                            return True
                    # Also try isinstance check as backup
                    try:
                        from sparkless.functions.window_execution import WindowFunction

                        if isinstance(value, WindowFunction):
                            return True
                    except (ImportError, AttributeError):
                        pass

        # Check if any numeric fields have default values (0.0, 0, None)
        # But ignore None values in fields that start with "right_" as these are expected from joins
        # Also ignore None values in nullable fields - they are valid (e.g., window function lag/lead)
        # IMPORTANT: For numeric defaults (0.0, 0), only return True if ALL rows have the same default value.
        # If only some rows have 0, it's likely a valid value (e.g., 0 * 2 = 0), not a default.
        # This fixes the issue where valid 0 values trigger fallback to manual materialization.
        # For other cases (None in StringType, None in non-nullable fields), return True immediately
        # to catch backend failures quickly.
        default_value_fields: Dict[str, List[Any]] = {}
        for row in data:
            for field in schema.fields:
                if field.name in row:
                    value = row[field.name]
                    # Skip fields that are expected to be None from joins
                    if field.name.startswith("right_") and value is None:
                        continue
                    # Skip None values in nullable fields - they are valid (e.g., window functions)
                    if value is None and field.nullable:
                        continue
                    # Check for default values that indicate the backend couldn't evaluate the function
                    # For numeric fields, track which fields have default values (need all rows check)
                    if (
                        value == 0.0
                        and field.dataType.__class__.__name__
                        in [
                            "DoubleType",
                            "FloatType",
                        ]
                    ) or (
                        value == 0
                        and field.dataType.__class__.__name__
                        in [
                            "IntegerType",
                            "LongType",
                        ]
                    ):
                        if field.name not in default_value_fields:
                            default_value_fields[field.name] = []
                        default_value_fields[field.name].append(value)
                    # For non-numeric cases, return True immediately (original behavior)
                    elif value is None and (
                        field.dataType.__class__.__name__ in ["StringType"]
                        or not getattr(field, "nullable", True)
                    ):
                        return True

        # Only return True if ALL rows have the same default value for numeric fields
        # This indicates the backend couldn't handle the operation, not that 0 is a valid result
        for field_name, values in default_value_fields.items():
            if len(values) == len(data):
                # All rows have the same default value - likely indicates backend failure
                return True

        return False

    @staticmethod
    def _requires_manual_materialization(
        operations_queue: List[Tuple[str, Any]],
    ) -> bool:
        """Check if operations require manual materialization.

        .. deprecated::
            This heuristic-based method is deprecated in favor of explicit capability checks
            via materializer.can_handle_operations(). It may be removed in a future version.
        """
        for op_name, op_val in operations_queue:
            if op_name == "select":
                # Check if select contains operations that require manual materialization
                for col in op_val:
                    # Window functions (incl. arithmetic) are handled by Polars backend
                    # via operation_executor.apply_select; do not force manual materialization
                    # Check for CaseWhen (when/otherwise expressions)
                    # CaseWhen can be translated by Polars, so don't force manual materialization
                    if hasattr(col, "conditions"):
                        # CaseWhen can be translated by Polars, so allow backend to handle it
                        continue
                    # Check for ColumnOperation
                    if hasattr(col, "operation") and col.operation in [
                        "months_between",
                        # "array_distinct",  # Removed feature
                        "pi",  # Force manual materialization for constant functions
                        "e",  # Force manual materialization for constant functions
                        # Allow backend to handle datediff via SQL path
                        # "cast",  # Removed - Polars handles cast operations
                        # "when",  # Removed - Polars handles when/otherwise via CaseWhen
                        # "otherwise",  # Removed - Polars handles when/otherwise via CaseWhen
                    ]:
                        return True
            elif op_name == "withColumn":
                # Window functions in withColumn are handled by Polars backend
                # via operation_executor.apply_withColumn; do not force manual materialization
                pass
            elif op_name == "filter":
                # Check if filter contains F.expr() expressions or complex operations
                # that the backend might not handle correctly
                filter_expr = op_val
                if LazyEvaluationEngine._has_expr_expression(filter_expr):
                    return True
        return False

    @staticmethod
    def _has_window_function(expr: Any) -> bool:
        """Check if expression contains WindowFunction objects that require manual materialization.

        Args:
            expr: Expression to check (Column, ColumnOperation, WindowFunction, or nested structure)

        Returns:
            True if expression contains WindowFunction objects
        """
        # Check if this is a WindowFunction directly
        if hasattr(expr, "__class__") and expr.__class__.__name__ == "WindowFunction":
            return True
        # Recursively check nested expressions
        if hasattr(expr, "column") and LazyEvaluationEngine._has_window_function(
            expr.column
        ):
            return True
        if hasattr(expr, "value") and LazyEvaluationEngine._has_window_function(
            expr.value
        ):
            return True
        return bool(
            hasattr(expr, "function")
            and LazyEvaluationEngine._has_window_function(expr.function)
        )

    @staticmethod
    def _has_expr_expression(expr: Any) -> bool:
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
            if hasattr(expr, "column") and LazyEvaluationEngine._has_expr_expression(
                expr.column
            ):
                return True
            if hasattr(expr, "value") and LazyEvaluationEngine._has_expr_expression(
                expr.value
            ):
                return True
        # Check if this is a Column (simple reference, no issue)
        elif hasattr(expr, "name") and not hasattr(expr, "operation"):
            return False
        return False

    @staticmethod
    def _convert_materialized_rows(
        rows: List[Any], schema: "StructType"
    ) -> List[Dict[str, Any]]:
        """Convert materialized rows to proper data format with type conversion.

        Args:
            rows: Materialized rows from backend
            schema: Expected schema

        Returns:
            List of dictionaries with proper types
        """
        from ..spark_types import ArrayType

        materialized_data = []
        for row in rows:
            row_dict = (
                row.asDict()
                if hasattr(row, "asDict")
                else dict(row)
                if isinstance(row, dict)
                else {}
            )

            # Handle duplicate column names from joins (Polars renames them with _right suffix)
            # PySpark allows duplicate names in schema, so we need to map Polars columns to schema fields
            # Build dict matching schema field order, handling _right suffix columns
            schema_field_names = [f.name for f in schema.fields]
            converted_dict = {}
            seen_field_names: Dict[str, int] = {}

            for field_name in schema_field_names:
                # Count occurrences of this field name in schema
                current_occurrence = seen_field_names.get(field_name, 0) + 1
                seen_field_names[field_name] = current_occurrence

                if current_occurrence == 1:
                    # First occurrence - use left value if available, otherwise right
                    if field_name in row_dict:
                        converted_dict[field_name] = row_dict[field_name]
                    elif f"{field_name}_right" in row_dict:
                        converted_dict[field_name] = row_dict[f"{field_name}_right"]
                    else:
                        converted_dict[field_name] = None
                else:
                    # Subsequent occurrence (duplicate) - use right value
                    right_key = f"{field_name}_right"
                    if right_key in row_dict:
                        converted_dict[field_name] = row_dict[right_key]
                    elif field_name in row_dict:
                        # Fallback to left value if right not available
                        converted_dict[field_name] = row_dict[field_name]
                    else:
                        converted_dict[field_name] = None

            # Ensure all schema fields are present (use None if missing from row_dict)
            # This handles cases where schema expects fields that aren't in the row
            has_right_columns = any(
                f"{name}_right" in row_dict for name in schema_field_names
            )
            for field in schema.fields:
                if field.name not in converted_dict:
                    # Only check for _right version if we didn't already process it above
                    if not has_right_columns:
                        right_key = f"{field.name}_right"
                        converted_dict[field.name] = converted_dict.get(right_key)
                    else:
                        converted_dict[field.name] = None

            # Convert values to match their declared schema types
            for field in schema.fields:
                if field.name not in row_dict:
                    # Field expected in schema but not in row - keep as None or skip
                    continue

                value = row_dict[field.name]

                # Handle ArrayType
                if isinstance(field.dataType, ArrayType):
                    # Backend may return arrays as strings like "['a', 'b']" or as lists
                    if isinstance(value, str):
                        # Try different array formats
                        if value.startswith("[") and value.endswith("]"):
                            # Parse string representation of list: "['a', 'b']"
                            import ast

                            try:
                                converted_dict[field.name] = ast.literal_eval(value)
                            except Exception:  # noqa: E722
                                # If parsing fails, split manually
                                converted_dict[field.name] = value[1:-1].split(",")
                        elif value.startswith("{") and value.endswith("}"):
                            # PostgreSQL array format: "{a,b}"
                            converted_dict[field.name] = value[1:-1].split(",")

                # Handle numeric types that come back as strings
                elif isinstance(field.dataType, (IntegerType, LongType)):
                    if isinstance(value, str):
                        with contextlib.suppress(ValueError, TypeError):
                            converted_dict[field.name] = int(value)
                        # Keep as string if conversion fails

                elif isinstance(field.dataType, DoubleType):
                    if isinstance(value, str):
                        with contextlib.suppress(ValueError, TypeError):
                            converted_dict[field.name] = float(value)
                        # Keep as string if conversion fails
                    else:
                        # Convert Decimal or other numeric types to float
                        with contextlib.suppress(ValueError, TypeError):
                            converted_dict[field.name] = float(value)
                        # Keep original value if conversion fails

            materialized_data.append(converted_dict)

        return materialized_data

    @staticmethod
    def _materialize_manual(df: "DataFrame") -> "DataFrame":
        """Fallback manual materialization when backend materialization is not available.

        Args:
            df: Lazy DataFrame

        Returns:
            Eager DataFrame with operations applied
        """
        from ..dataframe import DataFrame

        # Preserve schema from original DataFrame - this ensures empty DataFrames
        # with explicit schemas maintain their column information
        # Also preserve cached state for PySpark compatibility
        # Use df._schema (base schema) instead of df.schema (projected schema) for current
        # This ensures current._schema is the base schema, not the projected schema
        current = DataFrame(df.data, df._schema, df.storage)
        current._is_cached = getattr(df, "_is_cached", False)

        # Track schema at each step to validate expressions against the correct schema
        # This fixes issue #160 where expressions created before a select() operation
        # are validated against the schema after select(), causing "cannot resolve" errors
        # Start with the base schema (before any operations)
        from ..dataframe.schema.schema_manager import SchemaManager

        # When df has queued operations, df._schema is NOT the base schema - it's the schema
        # after those operations were applied. We need to infer the true base schema from
        # the data itself, which hasn't been transformed yet during materialization.
        # This fixes issue #173 where validation fails because base_schema was wrong.
        if df.data and len(df.data) > 0:
            # Infer base schema from actual data (this gives us the true base schema)
            try:
                from sparkless.core.schema_inference import infer_schema_from_data

                base_schema = infer_schema_from_data(df.data)
            except (ValueError, Exception):
                # If schema inference fails (e.g., all-null columns, type conflicts),
                # extract only fields from df._schema that exist in df.data.
                # This gives us the true base schema (columns in original data) even when
                # inference fails, instead of using df._schema which includes columns from
                # queued operations.
                from ..spark_types import StructType, StructField

                # Get keys that exist in the actual data
                data_keys: Set[str] = set()
                for row in df.data:
                    if isinstance(row, dict):
                        data_keys.update(row.keys())

                # Filter df._schema.fields to only include fields whose names exist in data
                base_fields = [
                    field for field in df._schema.fields if field.name in data_keys
                ]

                # We have matching fields - this is the true base schema, or fall back to df._schema
                base_schema = StructType(base_fields) if base_fields else df._schema
        else:
            # Fall back to df._schema if no data available
            base_schema = df._schema
        operations_applied_so_far = []
        schema_at_operation = base_schema

        for op_name, op_val in df._operations_queue:
            try:
                if op_name == "filter":
                    # Manual filter implementation
                    from ..core.condition_evaluator import ConditionEvaluator

                    filtered_data = []
                    for row in current.data:
                        if ConditionEvaluator.evaluate_condition(row, op_val):
                            filtered_data.append(row)
                    current = DataFrame(filtered_data, current.schema, current.storage)

                elif op_name == "withColumn":
                    col_name, col = op_val
                    # Temporarily set the schema to the one that existed when this operation was queued
                    # This ensures expressions are validated against the correct schema
                    # (fixes issue #160 where expressions are validated against schema after select())
                    current._schema = (
                        schema_at_operation  # Set _schema directly to avoid projection
                    )
                    try:
                        # Manually evaluate withColumn instead of queuing it again
                        # This ensures column values are actually computed during materialization
                        from ..dataframe.evaluation.expression_evaluator import (
                            ExpressionEvaluator,
                        )
                        from ..spark_types import StructType, StructField
                        from ..core.schema_inference import SchemaInferenceEngine

                        # Check if this is a WindowFunction or ColumnOperation wrapping a WindowFunction
                        # WindowFunction.evaluate() returns a sequence for all rows, not per-row
                        from ..functions.window_execution import WindowFunction
                        from ..functions import ColumnOperation

                        # Check if col is a WindowFunction directly
                        is_window_function = isinstance(col, WindowFunction)
                        # Check if col is a ColumnOperation wrapping a WindowFunction (e.g., WindowFunction.cast())
                        is_window_function_cast = (
                            isinstance(col, ColumnOperation)
                            and col.operation == "cast"
                            and isinstance(col.column, WindowFunction)
                        )

                        if is_window_function or is_window_function_cast:
                            # Extract WindowFunction and cast type
                            window_func = (
                                col if isinstance(col, WindowFunction) else col.column
                            )
                            cast_type = col.value if is_window_function_cast else None

                            # Window function (lag, lead, rank, etc.)
                            try:
                                # Evaluate window function for all rows at once
                                result_raw = window_func.evaluate(current.data)

                                result_seq: Optional[Sequence[Any]]
                                if result_raw is None:
                                    result_seq = None
                                elif isinstance(result_raw, Sequence):
                                    result_seq = result_raw
                                else:
                                    result_seq = cast("Sequence[Any]", result_raw)

                                # Apply cast if this is a cast operation
                                if is_window_function_cast and cast_type is not None:
                                    from ..dataframe.casting.type_converter import (
                                        TypeConverter,
                                    )
                                    from ..spark_types import (
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

                                    if cast_type is not None and result_seq is not None:
                                        result_seq = [
                                            TypeConverter.cast_to_type(r, cast_type)
                                            if r is not None
                                            else None
                                            for r in result_seq
                                        ]

                                # Create new data with window function results
                                new_data = []
                                for row_index, row in enumerate(current.data):
                                    new_row = row.copy()
                                    # Get the result for this specific row using the row_index
                                    if result_seq is not None and row_index < len(
                                        result_seq
                                    ):
                                        new_row[col_name] = result_seq[row_index]
                                    else:
                                        new_row[col_name] = None
                                    new_data.append(new_row)

                            except Exception:
                                # If window function evaluation fails, set all to None
                                new_data = []
                                for row in current.data:
                                    new_row = row.copy()
                                    new_row[col_name] = None
                                    new_data.append(new_row)
                        else:
                            # Regular expression - use ExpressionEvaluator
                            evaluator = ExpressionEvaluator(current)

                            # Evaluate the column expression for each row
                            new_data = []
                            for row in current.data:
                                new_row = row.copy()
                                try:
                                    # Evaluate the column expression
                                    col_value = evaluator.evaluate_expression(row, col)
                                    new_row[col_name] = col_value
                                except Exception:
                                    # If evaluation fails, set to None
                                    new_row[col_name] = None
                                new_data.append(new_row)

                        # Infer the new column's type from the evaluated values
                        col_values = [
                            row.get(col_name) for row in new_data if col_name in row
                        ]
                        if col_values:
                            col_type = SchemaInferenceEngine._infer_type(col_values[0])
                        else:
                            # Fallback to inferring from the expression itself
                            col_type = SchemaInferenceEngine._infer_type(col)

                        # Add the new field to the schema
                        new_fields = list(current.schema.fields)
                        # Check if column already exists (replace case)
                        existing_field_index = None
                        for i, field in enumerate(new_fields):
                            if field.name == col_name:
                                existing_field_index = i
                                break

                        new_field = StructField(col_name, col_type, nullable=True)
                        if existing_field_index is not None:
                            new_fields[existing_field_index] = new_field
                        else:
                            new_fields.append(new_field)

                        new_schema = StructType(new_fields)
                        current = DataFrame(new_data, new_schema, current.storage)
                    finally:
                        # Compute the schema after this withColumn operation for next operation
                        # Update operations_applied_so_far and recompute schema
                        operations_applied_so_far.append((op_name, op_val))
                        schema_at_operation = (
                            SchemaManager.project_schema_with_operations(
                                base_schema, operations_applied_so_far
                            )
                        )
                        # Restore current._schema to the projected schema (current.schema will use projection)
                        current._schema = schema_at_operation
                elif op_name == "select":
                    # Manual select implementation
                    from ..core.schema_inference import SchemaInferenceEngine
                    from ..functions.core.column import Column, ColumnOperation
                    from ..functions.core.literals import Literal
                    from ..spark_types import StructType, StructField

                    new_fields = []
                    for col in op_val:
                        if isinstance(col, str):
                            # String column name - find in current schema
                            found = False
                            schema_field: Optional[StructField] = None
                            if hasattr(current.schema, "_field_map"):
                                schema_field = current.schema._field_map.get(col)
                            if schema_field is not None:
                                new_fields.append(schema_field)
                                found = True
                            if not found:
                                # Column not found in schema, might be from join or new column
                                # Use StringType as fallback
                                from ..spark_types import StringType

                                new_fields.append(StructField(col, StringType(), True))
                        elif isinstance(col, Column) and (
                            not hasattr(col, "operation") or col.operation is None
                        ):
                            # Simple column reference
                            field = None
                            if hasattr(current.schema, "_field_map"):
                                field = current.schema._field_map.get(col.name)
                            if field is not None:
                                new_fields.append(field)
                        elif isinstance(col, ColumnOperation):
                            # Check if this is a ColumnOperation wrapping a WindowFunction (e.g., WindowFunction.cast())
                            from ..functions.window_execution import WindowFunction

                            is_window_function_cast = (
                                col.operation == "cast"
                                and isinstance(col.column, WindowFunction)
                            )

                            if is_window_function_cast:
                                # Handle WindowFunction cast - extract WindowFunction and cast type
                                window_func = col.column
                                cast_type = col.value

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
                                        window_func.function_name = (
                                            function_name_from_func
                                        )

                                # Get alias name
                                col_name = (
                                    getattr(col, "name", None)
                                    or getattr(col, "_alias_name", None)
                                    or (
                                        f"{window_func.function_name}_over"
                                        if hasattr(window_func, "function_name")
                                        else "window_result"
                                    )
                                )

                                # Evaluate window function for all rows at once
                                try:
                                    result_raw = window_func.evaluate(current.data)

                                    result_seq_select: Optional[Sequence[Any]]
                                    if result_raw is None:
                                        result_seq_select = None
                                    elif isinstance(result_raw, Sequence):
                                        result_seq_select = result_raw
                                    else:
                                        result_seq_select = cast(
                                            "Sequence[Any]", result_raw
                                        )

                                    # Apply cast if this is a cast operation
                                    if cast_type is not None:
                                        from ..dataframe.casting.type_converter import (
                                            TypeConverter,
                                        )
                                        from ..spark_types import (
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
                                            cast_type = type_name_map.get(
                                                cast_type.lower()
                                            )

                                        if (
                                            cast_type is not None
                                            and result_seq_select is not None
                                        ):
                                            result_seq_select = [
                                                TypeConverter.cast_to_type(r, cast_type)
                                                if r is not None
                                                else None
                                                for r in result_seq_select
                                            ]

                                    # Store results for later use in data evaluation
                                    # We'll add these to new_data when evaluating rows
                                    if not hasattr(current, "_window_function_results"):
                                        setattr(current, "_window_function_results", {})
                                    window_results = getattr(
                                        current, "_window_function_results", {}
                                    )
                                    window_results[col_name] = result_seq_select
                                    setattr(
                                        current,
                                        "_window_function_results",
                                        window_results,
                                    )

                                    # Infer type from cast type or window function result
                                    if cast_type is not None:
                                        col_type = cast_type
                                    elif (
                                        result_seq_select and len(result_seq_select) > 0
                                    ):
                                        col_type = SchemaInferenceEngine._infer_type(
                                            result_seq_select[0]
                                        )
                                    else:
                                        col_type = SchemaInferenceEngine._infer_type(
                                            1
                                        )  # Default to IntegerType

                                    new_fields.append(
                                        StructField(col_name, col_type, True)
                                    )
                                    continue  # Skip to next column
                                except Exception:
                                    # If window function evaluation fails, treat as regular ColumnOperation
                                    pass  # Fall through to regular ColumnOperation handling

                            # Column operation - need to evaluate
                            col_name = getattr(col, "name", "result")

                            # Handle transform operations specially
                            if col.operation == "transform":
                                # For transform, we need to evaluate the lambda on the array
                                from ..functions.core.lambda_parser import LambdaParser

                                # Get the column being transformed
                                transform_col = col.column
                                if isinstance(transform_col, Column):
                                    col_name = transform_col.name

                                # Get the lambda function
                                lambda_func = col.value

                                # Parse the lambda function
                                try:
                                    parser = LambdaParser(lambda_func)
                                    parser_any = cast("Any", parser)
                                    # Lambda parsing for transform operations
                                    lambda_expr = (
                                        parser_any.parse()
                                        if hasattr(parser_any, "parse")
                                        else None
                                    )
                                except Exception as e:
                                    print(
                                        f"Warning: Failed to parse lambda for transform: {e}"
                                    )
                                    lambda_expr = None

                                # Create a field for the transformed result
                                col_type = SchemaInferenceEngine._infer_type(
                                    []
                                )  # Array type
                                new_fields.append(StructField(col_name, col_type, True))
                            else:
                                # For other operations, use the standard approach
                                col_type = SchemaInferenceEngine._infer_type(col)
                                new_fields.append(StructField(col_name, col_type, True))
                        elif hasattr(col, "get_result_type"):
                            # CaseWhen or similar conditional expression
                            col_name = getattr(col, "name", "case_when")
                            col_type = col.get_result_type()
                            new_fields.append(StructField(col_name, col_type, True))
                        elif isinstance(col, Literal):
                            # Literal value
                            col_name = getattr(col, "name", "literal")
                            col_type = SchemaInferenceEngine._infer_type(col)
                            new_fields.append(StructField(col_name, col_type, True))
                        elif hasattr(col, "function_name") and hasattr(
                            col, "window_spec"
                        ):
                            # Window function (lag, lead, rank, etc.)
                            # For window functions, col.name should be the alias, not the object itself
                            if hasattr(col, "name") and isinstance(col.name, str):
                                col_name = col.name
                            else:
                                # Fallback to generating a name
                                col_name = f"{col.function_name}_over"

                            # Infer type based on function
                            if col.function_name in [
                                "row_number",
                                "rank",
                                "dense_rank",
                            ]:
                                col_type = SchemaInferenceEngine._infer_type(
                                    1
                                )  # IntegerType
                            elif col.function_name in ["lag", "lead"]:
                                # Try to infer from source column
                                field = None
                                if col.column_name and hasattr(
                                    current.schema, "_field_map"
                                ):
                                    field = current.schema._field_map.get(
                                        col.column_name
                                    )
                                    if field is not None:
                                        col_type = field.dataType
                                    else:
                                        col_type = SchemaInferenceEngine._infer_type(
                                            ""
                                        )  # StringType
                                else:
                                    col_type = SchemaInferenceEngine._infer_type(
                                        ""
                                    )  # StringType
                            else:
                                col_type = SchemaInferenceEngine._infer_type(
                                    0.0
                                )  # DoubleType

                            new_fields.append(StructField(col_name, col_type, True))
                        else:
                            # Fallback for other types
                            col_name = str(col)
                            col_type = SchemaInferenceEngine._infer_type(col)
                            new_fields.append(StructField(col_name, col_type, True))

                    new_schema = StructType(new_fields)

                    # Evaluate the select operation on each row
                    new_data = []
                    for row_index, row in enumerate(current.data):
                        new_row = {}
                        for i, col in enumerate(op_val):
                            if isinstance(col, str):
                                # String column name - look up in current row
                                # Use the field name from schema (which matches the column name)
                                field_name = (
                                    new_fields[i].name if i < len(new_fields) else col
                                )
                                # First try exact match
                                if col in row:
                                    new_row[field_name] = row[col]
                                elif field_name in row:
                                    # Try using field_name (might be different from col due to schema inference)
                                    new_row[field_name] = row[field_name]
                                elif (
                                    hasattr(current.schema, "_field_map")
                                    and field_name in current.schema._field_map
                                ):
                                    # Try to get the value using the field name from schema
                                    field = current.schema._field_map[field_name]
                                    if field.name in row:
                                        new_row[field_name] = row[field.name]
                                    else:
                                        new_row[field_name] = row.get(col, None)
                                else:
                                    new_row[field_name] = row.get(col, None)
                            elif isinstance(col, Column) and (
                                not hasattr(col, "operation") or col.operation is None
                            ):
                                # Simple column reference
                                field_name = (
                                    new_fields[i].name
                                    if i < len(new_fields)
                                    else col.name
                                )
                                if col.name in row:
                                    new_row[field_name] = row[col.name]
                                else:
                                    # Column name not in row - try to get it
                                    new_row[field_name] = row.get(col.name, None)
                            elif isinstance(col, ColumnOperation):
                                # Check if this is a ColumnOperation wrapping a WindowFunction (e.g., WindowFunction.cast())
                                from ..functions.window_execution import WindowFunction

                                is_window_function_cast = (
                                    col.operation == "cast"
                                    and isinstance(col.column, WindowFunction)
                                )

                                if is_window_function_cast:
                                    # Use pre-computed window function results
                                    field_name = (
                                        new_fields[i].name
                                        if i < len(new_fields)
                                        else getattr(col, "name", "result")
                                    )
                                    if (
                                        hasattr(current, "_window_function_results")
                                        and field_name
                                        in current._window_function_results
                                    ):
                                        result_seq = current._window_function_results[
                                            field_name
                                        ]
                                        if result_seq is not None and row_index < len(
                                            result_seq
                                        ):
                                            new_row[field_name] = result_seq[row_index]
                                        else:
                                            new_row[field_name] = None
                                    else:
                                        new_row[field_name] = None
                                    continue  # Skip to next column

                                # Column operation - evaluate using condition evaluator
                                if col.operation == "transform":
                                    # Handle transform operation for higher-order array functions
                                    try:
                                        from ..core.condition_evaluator import (
                                            ConditionEvaluator,
                                        )

                                        result = ConditionEvaluator.evaluate_condition(
                                            row, col
                                        )
                                        if i < len(new_fields):
                                            new_row[new_fields[i].name] = result
                                    except Exception as e:
                                        print(
                                            f"Warning: Failed to evaluate transform operation: {e}"
                                        )
                                        if i < len(new_fields):
                                            new_row[new_fields[i].name] = None
                                elif col.operation == "cast":
                                    # Handle cast operation
                                    try:
                                        # Get the source value
                                        if hasattr(col, "column") and hasattr(
                                            col.column, "name"
                                        ):
                                            source_value = row.get(col.column.name)
                                        else:
                                            source_value = None

                                        # Perform the cast
                                        cast_type = col.value
                                        if isinstance(cast_type, str):
                                            if cast_type.lower() == "long":
                                                # Convert to Unix timestamp for timestamp strings
                                                if isinstance(source_value, str):
                                                    from datetime import datetime

                                                    try:
                                                        # Try parsing as timestamp
                                                        dt = datetime.strptime(
                                                            source_value,
                                                            "%Y-%m-%d %H:%M:%S",
                                                        )
                                                        if i < len(new_fields):
                                                            new_row[
                                                                new_fields[i].name
                                                            ] = int(dt.timestamp())
                                                    except Exception:
                                                        if i < len(new_fields):
                                                            new_row[
                                                                new_fields[i].name
                                                            ] = None
                                                else:
                                                    if i < len(new_fields):
                                                        new_row[new_fields[i].name] = (
                                                            int(source_value)
                                                            if source_value is not None
                                                            else None
                                                        )
                                            elif cast_type.lower() in [
                                                "int",
                                                "integer",
                                            ]:
                                                if i < len(new_fields):
                                                    new_row[new_fields[i].name] = (
                                                        int(source_value)
                                                        if source_value is not None
                                                        else None
                                                    )
                                            elif cast_type.lower() in [
                                                "double",
                                                "float",
                                            ]:
                                                if i < len(new_fields):
                                                    new_row[new_fields[i].name] = (
                                                        float(source_value)
                                                        if source_value is not None
                                                        else None
                                                    )
                                            elif cast_type.lower() in [
                                                "string",
                                                "varchar",
                                            ]:
                                                if i < len(new_fields):
                                                    new_row[new_fields[i].name] = (
                                                        str(source_value)
                                                        if source_value is not None
                                                        else None
                                                    )
                                            elif cast_type.lower() in [
                                                "boolean",
                                                "bool",
                                            ]:
                                                if i < len(new_fields):
                                                    new_row[new_fields[i].name] = (
                                                        bool(source_value)
                                                        if source_value is not None
                                                        else None
                                                    )
                                            else:
                                                if i < len(new_fields):
                                                    new_row[new_fields[i].name] = (
                                                        source_value
                                                    )
                                        else:
                                            if i < len(new_fields):
                                                new_row[new_fields[i].name] = (
                                                    source_value
                                                )
                                    except Exception:
                                        if i < len(new_fields):
                                            new_row[new_fields[i].name] = None
                                else:
                                    # Other column operations (arithmetic, functions, etc.)
                                    from ..core.condition_evaluator import (
                                        ConditionEvaluator,
                                    )

                                    try:
                                        # Pass cached state information via row metadata
                                        # for string concatenation compatibility
                                        eval_row = row.copy()
                                        if current._is_cached:
                                            eval_row["__dataframe_is_cached__"] = True

                                        result = ConditionEvaluator.evaluate_expression(
                                            eval_row, col
                                        )
                                        if i < len(new_fields):
                                            new_row[new_fields[i].name] = result
                                    except Exception:
                                        if i < len(new_fields):
                                            new_row[new_fields[i].name] = None
                            elif hasattr(col, "evaluate") and hasattr(
                                col, "conditions"
                            ):
                                # CaseWhen or similar conditional expression
                                try:
                                    result = col.evaluate(row)
                                    if i < len(new_fields):
                                        new_row[new_fields[i].name] = result
                                except Exception:
                                    if i < len(new_fields):
                                        new_row[new_fields[i].name] = None
                            elif isinstance(col, Literal):
                                # Literal value
                                if i < len(new_fields):
                                    new_row[new_fields[i].name] = col.value
                            elif hasattr(col, "function_name") and hasattr(
                                col, "window_spec"
                            ):
                                # Window function (lag, lead, rank, etc.)
                                try:
                                    # The col is already a WindowFunction, just evaluate it
                                    result_raw = col.evaluate(current.data)
                                    result_seq_window: Optional[Sequence[Any]]
                                    if result_raw is None:
                                        result_seq_window = None
                                    elif isinstance(result_raw, Sequence):
                                        result_seq_window = result_raw
                                    else:
                                        result_seq_window = cast(
                                            "Sequence[Any]", result_raw
                                        )

                                    # Get the result for this specific row using the row_index
                                    if (
                                        result_seq_window is not None
                                        and row_index < len(result_seq_window)
                                        and i < len(new_fields)
                                    ):
                                        new_row[new_fields[i].name] = result_seq_window[
                                            row_index
                                        ]
                                    elif i < len(new_fields):
                                        new_row[new_fields[i].name] = None
                                except Exception:
                                    # Silently handle errors
                                    if i < len(new_fields):
                                        new_row[new_fields[i].name] = None
                            else:
                                # Fallback
                                if i < len(new_fields):
                                    new_row[new_fields[i].name] = None
                        new_data.append(new_row)

                    # Update current with new data and schema, preserving cached state
                    current = DataFrame(new_data, new_schema, current.storage)
                    current._is_cached = getattr(current, "_is_cached", False)
                    # Update schema tracking for next operation
                    operations_applied_so_far.append((op_name, op_val))
                    schema_at_operation = SchemaManager.project_schema_with_operations(
                        base_schema, operations_applied_so_far
                    )
                elif op_name == "distinct":
                    # Deduplicate current.data by schema order (matches eager distinct)
                    field_names = [f.name for f in current.schema.fields]
                    seen: Set[Tuple[Any, ...]] = set()
                    distinct_data: List[Dict[str, Any]] = []
                    for row in current.data:
                        row_tuple = tuple(row.get(name) for name in field_names)
                        if row_tuple not in seen:
                            seen.add(row_tuple)
                            distinct_data.append(row)
                    current = DataFrame(distinct_data, current.schema, current.storage)
                    current._is_cached = getattr(current, "_is_cached", False)
                    operations_applied_so_far.append((op_name, op_val))
                    schema_at_operation = SchemaManager.project_schema_with_operations(
                        base_schema, operations_applied_so_far
                    )
                elif op_name == "groupBy":
                    current_ops = cast("SupportsDataFrameOps", current)
                    grouped = current_ops.groupBy(*op_val)  # Returns GroupedData
                    # GroupedData is not a DataFrame, but we handle it in the next iteration
                    # This cast is intentional for the lazy evaluation flow
                    current = cast("DataFrame", grouped)
                elif op_name == "join":
                    other_df, on, how = op_val
                    # Manual join implementation
                    from ..core.condition_evaluator import ConditionEvaluator

                    # Materialize other DataFrame if needed
                    if other_df._operations_queue:
                        other_df = other_df._materialize_if_lazy()

                    # Handle join condition
                    # Extract column names from ColumnOperation if it's a == comparison
                    join_conditions = []
                    if hasattr(on, "operation") and on.operation == "==":
                        # Extract column names from == comparison
                        # Handle case where left and right columns have different names
                        left_col = (
                            on.column.name
                            if hasattr(on.column, "name")
                            else str(on.column)
                        )
                        # Check if the value is a Column (different column names)
                        if hasattr(on, "value") and hasattr(on.value, "name"):
                            # Different column names: left_col == right_col
                            right_col = on.value.name
                            join_conditions.append((left_col, right_col))
                        else:
                            # Same column name in both DataFrames
                            join_conditions.append((left_col, left_col))
                    elif isinstance(on, str):
                        # Single column name (same in both DataFrames)
                        join_conditions.append((on, on))
                    elif isinstance(on, (list, tuple)):
                        # List of column names (same in both DataFrames)
                        for col in on:
                            join_conditions.append((col, col))
                    else:
                        # Try to extract column name(s) from the object
                        col_name = on.name if hasattr(on, "name") else str(on)
                        join_conditions.append((col_name, col_name))

                    # Perform the join
                    joined_data = []
                    for left_row in current.data:
                        matched = False
                        for right_row in other_df.data:
                            # Check if join condition is met
                            join_match = True
                            for left_col, right_col in join_conditions:
                                if left_row.get(left_col) != right_row.get(right_col):
                                    join_match = False
                                    break

                            if join_match:
                                matched = True
                                # For semi/anti joins, only use left row (no right columns)
                                if how.lower() in [
                                    "semi",
                                    "left_semi",
                                    "anti",
                                    "left_anti",
                                ]:
                                    joined_row = left_row.copy()
                                    joined_data.append(joined_row)
                                else:
                                    # For other joins, combine rows
                                    joined_row = left_row.copy()
                                    for key, value in right_row.items():
                                        # Avoid duplicate column names (Polars deduplicates)
                                        if key not in joined_row:
                                            joined_row[key] = value
                                        # Skip duplicates - Polars automatically deduplicates
                                    joined_data.append(joined_row)

                                # For inner join, only add matching rows
                                if how.lower() in ["inner", "inner_join"]:
                                    break

                        # For left/outer joins, if no match found, add left row with null values for right columns
                        if not matched and how.lower() in [
                            "left",
                            "outer",
                            "full",
                            "full_outer",
                        ]:
                            joined_row = left_row.copy()
                            # Add null values for right DataFrame columns that don't exist in left
                            existing_left_cols = set(left_row.keys())
                            for field in other_df.schema.fields:
                                if (
                                    field is not None
                                    and field.name not in existing_left_cols
                                ):
                                    joined_row[field.name] = None
                            joined_data.append(joined_row)

                    # Create new schema combining both schemas
                    # For semi/anti joins, only use left DataFrame schema
                    if how.lower() in ["semi", "left_semi", "anti", "left_anti"]:
                        new_schema = current.schema
                        # For semi/anti joins, remove duplicates from joined_data
                        # by keeping only left columns
                        left_col_names = {f.name for f in current.schema.fields}
                        joined_data = [
                            {k: v for k, v in row.items() if k in left_col_names}
                            for row in joined_data
                        ]
                    else:
                        # Explicitly import StructField and StructType to avoid UnboundLocalError
                        from ..spark_types import StructField, StructType

                        merged_fields: List[StructField] = [
                            existing_field
                            for existing_field in current.schema.fields
                            if existing_field is not None
                        ]
                        for field in other_df.schema.fields:
                            if field is None:
                                continue
                            # Avoid duplicate field names (Polars deduplicates automatically)
                            # So we should match that behavior in schema
                            if not any(f.name == field.name for f in merged_fields):
                                merged_fields.append(field)
                            # Skip duplicates - Polars deduplicates columns automatically
                        new_schema = StructType(merged_fields)
                    current = DataFrame(joined_data, new_schema, current.storage)
                    # Update operations_applied_so_far and schema_at_operation after join
                    # This ensures subsequent withColumn operations can validate against
                    # the correct schema that includes join columns (fixes test_columns_preserved_in_double_join_with_empty_aggregated)
                    operations_applied_so_far.append((op_name, op_val))
                    schema_at_operation = SchemaManager.project_schema_with_operations(
                        base_schema, operations_applied_so_far
                    )
                    # Update current._schema to match the projected schema
                    current._schema = schema_at_operation
                elif op_name == "union":
                    other_df = op_val
                    # Use SetOperations for union
                    from .operations.set_operations import SetOperations

                    result_data, result_schema = SetOperations.union(
                        current.data,
                        current.schema,
                        other_df.data,
                        other_df.schema,
                        current.storage,
                    )
                    current = DataFrame(result_data, result_schema, current.storage)
                elif op_name == "orderBy":
                    # Sort the current data by the specified columns
                    # op_val is a tuple of column names/Column objects
                    if op_val:
                        # Get column names from op_val
                        sort_cols = []
                        for col in op_val:
                            if isinstance(col, str):
                                sort_cols.append(col)
                            elif hasattr(col, "name"):
                                sort_cols.append(col.name)
                            else:
                                sort_cols.append(str(col))

                        # Sort the data
                        if sort_cols:
                            # Use the first column for sorting
                            sort_key = sort_cols[0]
                            sorted_data = sorted(
                                current.data,
                                key=lambda row: (
                                    row.get(sort_key) is not None,
                                    row.get(sort_key),
                                ),
                            )
                            # Preserve the current data and schema - don't reset
                            current = DataFrame(
                                sorted_data, current.schema, current.storage
                            )
                            current._is_cached = getattr(current, "_is_cached", False)
                elif op_name == "transform":
                    # Manual transform implementation for higher-order array functions
                    from ..core.condition_evaluator import ConditionEvaluator
                    from ..functions.core.lambda_parser import LambdaParser

                    # op_val should be (column_name, lambda_function)
                    if len(op_val) == 2:
                        col_name, lambda_func = op_val

                        # Parse the lambda function
                        try:
                            parser = LambdaParser(lambda_func)
                            parser_any = cast("Any", parser)
                            # Lambda parsing for transform operations
                            lambda_expr = (
                                parser_any.parse()
                                if hasattr(parser_any, "parse")
                                else None
                            )
                        except Exception as e:
                            # If lambda parsing fails, skip the transform
                            print(f"Warning: Failed to parse lambda for transform: {e}")
                            continue

                        # Apply the transform to each row
                        new_data = []
                        for row in current.data:
                            new_row = row.copy()
                            if (
                                col_name in row
                                and row[col_name] is not None
                                and isinstance(row[col_name], list)
                            ):
                                # Apply the lambda function to each element of the array
                                try:
                                    # Evaluate lambda function directly in Python
                                    if lambda_expr and callable(lambda_func):
                                        # Apply lambda function to each element
                                        new_row[col_name] = [
                                            lambda_func(x) for x in row[col_name]
                                        ]
                                    else:
                                        # If lambda parsing failed, keep original array
                                        pass
                                except Exception as e:
                                    # If lambda evaluation fails, skip the transform
                                    print(
                                        f"Warning: Failed to evaluate transform lambda: {e}"
                                    )
                                    pass
                            new_data.append(new_row)

                        current = DataFrame(new_data, current.schema, current.storage)
                else:
                    # Unknown ops ignored for now
                    continue
            except Exception as e:
                # If an operation fails due to column not found,
                # it might be because the operation was queued but the column
                # was removed by a previous operation. Skip this operation.
                if "Column" in str(e) and "does not exist" in str(e):
                    # Skip this operation - it's likely a dependency issue
                    continue
                else:
                    # Re-raise other exceptions
                    raise e
        return current

    @staticmethod
    def _infer_select_schema(df: "DataFrame", columns: Any) -> "StructType":
        """Infer schema for select operation.

        Args:
            df: Source DataFrame
            columns: Columns to select

        Returns:
            Inferred schema for selected columns
        """
        from ..functions import AggregateFunction

        new_fields = []
        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Add all existing fields
                    new_fields.extend(df.schema.fields)
                else:
                    # Use existing field, or add as StringType if not found
                    found = False
                    for field in df.schema.fields:
                        if field.name == col:
                            new_fields.append(field)
                            found = True
                            break
                    if not found:
                        # Column not in current schema, might come from join
                        new_fields.append(StructField(col, StringType()))
            elif hasattr(col, "operation") and hasattr(col, "column"):
                # Handle ColumnOperation
                col_name = col.name

                # Check operation type
                if col.operation == "cast":
                    # Cast operation - infer type from cast parameter
                    # For cast operations, use the original column name, not the cast expression name
                    original_col_name = (
                        col.column.name
                        if hasattr(col, "column") and hasattr(col.column, "name")
                        else col_name
                    )
                    cast_type = getattr(col, "value", "string")
                    if isinstance(cast_type, str):
                        # String type name, convert to actual type
                        if cast_type.lower() in ["double", "float"]:
                            new_fields.append(
                                StructField(original_col_name, DoubleType())
                            )
                        elif cast_type.lower() in ["int", "integer"]:
                            new_fields.append(
                                StructField(original_col_name, IntegerType())
                            )
                        elif cast_type.lower() in ["long", "bigint"]:
                            new_fields.append(
                                StructField(original_col_name, LongType())
                            )
                        elif cast_type.lower() in ["string", "varchar"]:
                            new_fields.append(
                                StructField(original_col_name, StringType())
                            )
                        elif cast_type.lower() in ["boolean", "bool"]:
                            new_fields.append(
                                StructField(original_col_name, BooleanType())
                            )
                        else:
                            new_fields.append(
                                StructField(original_col_name, StringType())
                            )
                    else:
                        # Type object, use directly
                        new_fields.append(StructField(original_col_name, cast_type))
                elif col.operation in ["upper", "lower"]:
                    new_fields.append(StructField(col_name, StringType()))
                elif col.operation == "length":
                    new_fields.append(StructField(col_name, IntegerType()))
                elif col.operation == "split":
                    # Split returns ArrayType of strings
                    new_fields.append(StructField(col_name, ArrayType(StringType())))
                elif col.operation in ["isnull", "isnotnull", "isnan"]:
                    # Boolean operations - always return BooleanType and are non-nullable

                    new_fields.append(
                        StructField(col_name, BooleanType(), nullable=False)
                    )
                elif col.operation == "coalesce":
                    # Coalesce with a non-null literal fallback is non-nullable
                    # Determine the result type from the first argument
                    source_type: Any = StringType()
                    if hasattr(col, "column") and hasattr(col.column, "name"):
                        # Find source column type
                        for field in df.schema.fields:
                            if field.name == col.column.name:
                                source_type = field.dataType
                                break
                    # Mark as non-nullable if there's a literal fallback
                    new_fields.append(
                        StructField(col_name, source_type, nullable=False)
                    )
                elif col.operation == "datediff":
                    new_fields.append(StructField(col_name, IntegerType()))
                elif col.operation == "months_between":
                    new_fields.append(StructField(col_name, DoubleType()))
                elif col.operation in [
                    "hour",
                    "minute",
                    "second",
                    "day",
                    "dayofmonth",
                    "month",
                    "year",
                    "quarter",
                    "dayofweek",
                    "dayofyear",
                    "weekofyear",
                ]:
                    new_fields.append(StructField(col_name, IntegerType()))
                else:
                    # Default to StringType for unknown operations
                    new_fields.append(StructField(col_name, StringType()))
            elif isinstance(col, AggregateFunction):
                # Handle aggregate functions - set proper nullability
                col_name = col.name

                # Determine nullable based on function type
                non_nullable_functions = {
                    "count",
                    "countDistinct",
                    "count_if",
                    "row_number",
                    "rank",
                    "dense_rank",
                }

                nullable = col.function_name not in non_nullable_functions

                # Use provided data type or default to LongType for counts, DoubleType otherwise
                if col.data_type:
                    data_type = col.data_type
                    if hasattr(data_type, "nullable"):
                        data_type.nullable = nullable
                elif col.function_name in {"count", "countDistinct", "count_if"}:
                    data_type = LongType(nullable=nullable)
                else:
                    data_type = DoubleType(nullable=nullable)

                new_fields.append(StructField(col_name, data_type, nullable=nullable))

            elif hasattr(col, "function_name") and hasattr(col, "window_spec"):
                # Handle WindowFunction (e.g., rank().over(window))
                # For window functions, col.name should be the alias, not the object itself
                if hasattr(col, "name") and isinstance(col.name, str):
                    col_name = col.name
                else:
                    # Fallback to generating a name
                    col_name = f"{col.function_name}_over"

                # Window functions that are non-nullable
                non_nullable_window_functions = {
                    "row_number",
                    "rank",
                    "dense_rank",
                }

                # Determine type and nullability based on function
                if col.function_name in non_nullable_window_functions:
                    # Ranking functions return IntegerType and are non-nullable
                    new_fields.append(
                        StructField(col_name, IntegerType(), nullable=False)
                    )
                elif col.function_name in ["lag", "lead"]:
                    # Lag/lead can return null (out of bounds)
                    if col.column_name:
                        # Find source column type
                        source_type2: Any = StringType()
                        for field in df.schema.fields:
                            if field.name == col.column_name:
                                source_type2 = field.dataType
                                break
                        new_fields.append(
                            StructField(col_name, source_type2, nullable=True)
                        )
                    else:
                        new_fields.append(
                            StructField(col_name, StringType(), nullable=True)
                        )
                elif col.function_name in ["sum", "avg", "min", "max"]:
                    # Aggregate window functions - nullable
                    new_fields.append(
                        StructField(col_name, DoubleType(), nullable=True)
                    )
                elif col.function_name in ["count", "countDistinct"]:
                    # Count functions are non-nullable
                    new_fields.append(StructField(col_name, LongType(), nullable=False))
                else:
                    # Default for other window functions
                    new_fields.append(
                        StructField(col_name, DoubleType(), nullable=True)
                    )

            elif hasattr(col, "value") and hasattr(col, "data_type"):
                # Handle Literal objects - literals are never nullable
                col_name = col.name
                # Use the literal's data_type and explicitly set nullable=False

                data_type = col.data_type
                # Create a new instance of the data type with nullable=False
                data_type_non_null: DataType
                if isinstance(data_type, BooleanType):
                    data_type_non_null = BooleanType(nullable=False)
                elif isinstance(data_type, IntegerType):
                    data_type_non_null = IntegerType(nullable=False)
                elif isinstance(data_type, LongType):
                    data_type_non_null = LongType(nullable=False)
                elif isinstance(data_type, DoubleType):
                    data_type_non_null = DoubleType(nullable=False)
                elif isinstance(data_type, StringType):
                    data_type_non_null = StringType(nullable=False)
                else:
                    # For other types, create a new instance with nullable=False
                    data_type_non_null = data_type.__class__(nullable=False)

                new_fields.append(
                    StructField(col_name, data_type_non_null, nullable=False)
                )
            elif hasattr(col, "conditions") and hasattr(col, "default_value"):
                # Handle CaseWhen objects - use get_result_type() method
                col_name = col.name
                from ..functions.conditional import CaseWhen

                if isinstance(col, CaseWhen):
                    # Use the proper type inference method
                    inferred_type = col.get_result_type()
                    new_fields.append(
                        StructField(col_name, inferred_type, nullable=False)
                    )
                else:
                    # Fallback for other conditional objects
                    new_fields.append(StructField(col_name, IntegerType()))
            elif hasattr(col, "conditions"):
                # Handle CaseWhen objects that didn't match the first condition
                col_name = col.name
                from ..functions.conditional import CaseWhen

                if isinstance(col, CaseWhen):
                    # Use the proper type inference method
                    inferred_type = col.get_result_type()
                    new_fields.append(
                        StructField(col_name, inferred_type, nullable=False)
                    )
                else:
                    # Default to StringType for unknown operations
                    new_fields.append(StructField(col_name, StringType()))
            elif hasattr(col, "name"):
                # Handle Column
                col_name = col.name
                if col_name == "*":
                    # Add all existing fields
                    new_fields.extend(df.schema.fields)
                else:
                    # Use existing field or add as string
                    found = False
                    for field in df.schema.fields:
                        if field.name == col_name:
                            new_fields.append(field)
                            found = True
                            break
                    if not found:
                        # Column not in current schema, might come from join
                        new_fields.append(StructField(col_name, StringType()))

        return StructType(new_fields)

    @staticmethod
    def _infer_join_schema(df: "DataFrame", join_params: Any) -> "StructType":
        """Infer schema for join operation.

        Args:
            df: Source DataFrame
            join_params: Join parameters (other_df, on, how)

        Returns:
            Inferred schema after join
        """
        from ..spark_types import StructType

        other_df, on, how = join_params

        # Start with all fields from left DataFrame
        new_fields = df.schema.fields.copy()

        # Add fields from right DataFrame.
        #
        # PySpark behavior for `df.join(other, on=["k1", "k2"], ...)` is to include
        # the join key columns only once (from the left side) and include the
        # remaining right-side columns. This avoids producing duplicate join keys
        # like ["Name", "Name"] which later makes column resolution ambiguous.
        #
        # We still allow duplicates for *non-join-key* columns (Spark can disambiguate
        # via qualifiers; Sparkless is more limited but keeps schema parity where possible).
        left_field_names = {f.name for f in df.schema.fields}

        # Normalize join keys for case-(in)sensitive comparison.
        case_sensitive = df._is_case_sensitive()
        join_keys: List[str] = []
        if isinstance(on, str):
            join_keys = [on]
        elif isinstance(on, (list, tuple)):
            join_keys = [c for c in on if isinstance(c, str)]
        # ColumnOperation join conditions are not handled here (schema inference only).

        if case_sensitive:
            join_key_set = set(join_keys)
        else:
            join_key_set = {k.lower() for k in join_keys}

        def _is_join_key(field_name: str) -> bool:
            if not join_key_set:
                return False
            return (
                field_name in join_key_set
                if case_sensitive
                else field_name.lower() in join_key_set
            )

        # First add right fields that don't exist in left
        for field in other_df.schema.fields:
            if field.name not in left_field_names:
                new_fields.append(field)

        # Then add right fields that DO exist in left (duplicates)
        for field in other_df.schema.fields:
            if field.name in left_field_names:
                # Sparkless generally avoids duplicate column names.
                # For joins on column names, keep only the left-side columns.
                if _is_join_key(field.name):
                    continue
                # Also skip other duplicates (non-join keys) for consistency.
                continue

        return StructType(new_fields)

    @staticmethod
    def _infer_withcolumn_schema(
        df: "DataFrame", withcolumn_params: Any
    ) -> "StructType":
        """Infer schema for withColumn operation.

        Args:
            df: Source DataFrame
            withcolumn_params: withColumn parameters (col_name, col)

        Returns:
            Inferred schema after withColumn
        """
        from ..spark_types import (
            StructType,
            StructField,
            BooleanType,
            IntegerType,
            LongType,
            DoubleType,
            StringType,
            DateType,
            TimestampType,
            DecimalType,
        )
        from ..functions.core.column import ColumnOperation

        col_name, col = withcolumn_params

        # Start with all existing fields except the one being added/replaced
        new_fields = [field for field in df.schema.fields if field.name != col_name]

        # Infer the type of the new column
        if (
            isinstance(col, ColumnOperation)
            and hasattr(col, "operation")
            and col.operation == "cast"
        ):
            # Cast operation - use the target data type from col.value
            cast_type = col.value
            if isinstance(cast_type, str):
                # String type name, convert to actual type
                if cast_type.lower() in ["double", "float"]:
                    new_fields.append(StructField(col_name, DoubleType()))
                elif cast_type.lower() in ["int", "integer"]:
                    new_fields.append(StructField(col_name, IntegerType()))
                elif cast_type.lower() in ["long", "bigint"]:
                    new_fields.append(StructField(col_name, LongType()))
                elif cast_type.lower() in ["string", "varchar"]:
                    new_fields.append(StructField(col_name, StringType()))
                elif cast_type.lower() in ["boolean", "bool"]:
                    new_fields.append(StructField(col_name, BooleanType()))
                elif cast_type.lower() in ["date"]:
                    new_fields.append(StructField(col_name, DateType()))
                elif cast_type.lower() in ["timestamp"]:
                    new_fields.append(StructField(col_name, TimestampType()))
                elif cast_type.lower().startswith("decimal"):
                    # Parse decimal(10,2) format
                    import re

                    match = re.match(r"decimal\((\d+),(\d+)\)", cast_type.lower())
                    if match:
                        precision, scale = int(match.group(1)), int(match.group(2))
                        new_fields.append(
                            StructField(col_name, DecimalType(precision, scale))
                        )
                    else:
                        new_fields.append(StructField(col_name, DecimalType(10, 2)))
                else:
                    # Default to StringType for unknown types
                    new_fields.append(StructField(col_name, StringType()))
            else:
                # Already a DataType object
                if hasattr(cast_type, "__class__"):
                    new_fields.append(
                        StructField(col_name, cast_type.__class__(nullable=True))
                    )
                else:
                    new_fields.append(StructField(col_name, cast_type))
        elif (
            isinstance(col, ColumnOperation)
            and hasattr(col, "operation")
            and col.operation in ["+", "-", "*", "/", "%"]
        ):
            # Arithmetic operations - infer type from operands
            left_type = None
            right_type = None

            # Get left operand type
            if hasattr(col.column, "name"):
                for field in df.schema.fields:
                    if field.name == col.column.name:
                        left_type = field.dataType
                        break

            # Get right operand type
            if (
                hasattr(col, "value")
                and col.value is not None
                and hasattr(col.value, "name")
            ):
                for field in df.schema.fields:
                    if field.name == col.value.name:
                        right_type = field.dataType
                        break

            # If either operand is DoubleType, result is DoubleType
            if (left_type and isinstance(left_type, DoubleType)) or (
                right_type and isinstance(right_type, DoubleType)
            ):
                new_fields.append(StructField(col_name, DoubleType()))
            else:
                new_fields.append(StructField(col_name, LongType()))
        elif (
            isinstance(col, ColumnOperation)
            and hasattr(col, "operation")
            and col.operation == "datediff"
        ):
            new_fields.append(StructField(col_name, IntegerType()))
        elif (
            isinstance(col, ColumnOperation)
            and hasattr(col, "operation")
            and col.operation == "months_between"
        ):
            new_fields.append(StructField(col_name, DoubleType()))
        elif (
            isinstance(col, ColumnOperation)
            and hasattr(col, "operation")
            and col.operation
            in [
                "hour",
                "minute",
                "second",
                "day",
                "dayofmonth",
                "month",
                "year",
                "quarter",
                "dayofweek",
                "dayofyear",
                "weekofyear",
            ]
        ):
            new_fields.append(StructField(col_name, IntegerType()))
        elif (
            isinstance(col, ColumnOperation)
            and hasattr(col, "operation")
            and col.operation
            in [
                "==",
                "!=",
                ">",
                "<",
                ">=",
                "<=",
                "and",
                "or",
                "not",
                "like",
                "isin",
                "between",
                "isnull",
                "isnotnull",
            ]
        ):
            # Boolean operations return BooleanType
            new_fields.append(StructField(col_name, BooleanType()))
        elif (
            isinstance(col, ColumnOperation)
            and hasattr(col, "operation")
            and col.operation
            in [
                "upper",
                "lower",
                "trim",
                "ltrim",
                "rtrim",
                "concat",
                "substring",
                "regexp_replace",
                "split",
                "length",
            ]
        ):
            # String operations return StringType
            new_fields.append(StructField(col_name, StringType()))
        elif (
            isinstance(col, ColumnOperation)
            and hasattr(col, "operation")
            and col.operation
            in [
                "abs",
                "round",
                "ceil",
                "floor",
                "sqrt",
                "exp",
                "log",
                "log10",
                "log2",
                "sin",
                "cos",
                "tan",
                "asin",
                "acos",
                "atan",
            ]
        ):
            # Math operations typically return DoubleType
            new_fields.append(StructField(col_name, DoubleType()))
        elif (
            isinstance(col, ColumnOperation)
            and hasattr(col, "operation")
            and col.operation == "to_date"
        ):
            # to_date returns DateType
            from ..spark_types import DateType

            new_fields.append(StructField(col_name, DateType()))
        elif (
            isinstance(col, ColumnOperation)
            and hasattr(col, "operation")
            and col.operation == "to_timestamp"
        ):
            # to_timestamp returns TimestampType
            from ..spark_types import TimestampType

            new_fields.append(StructField(col_name, TimestampType()))
        elif isinstance(col, ColumnOperation) and hasattr(col, "operation"):
            # For other ColumnOperations, use SchemaManager to infer type
            from ..schema.schema_manager import SchemaManager

            inferred_field = SchemaManager._infer_expression_type(col)
            new_fields.append(
                StructField(col_name, inferred_field.dataType, inferred_field.nullable)
            )
        else:
            # For other column types, default to StringType
            # Additional type inference can be added here for more operations
            new_fields.append(StructField(col_name, StringType()))

        return StructType(new_fields)

    @staticmethod
    def _filter_depends_on_original_columns(
        filter_condition: Any, original_schema: "StructType"
    ) -> bool:
        """Check if a filter condition depends on original columns.

        Args:
            filter_condition: Filter condition to check
            original_schema: Original schema before operations

        Returns:
            True if filter depends on original columns
        """
        # Get the original column names from the provided schema
        original_columns = {field.name for field in original_schema.fields}

        # Check if the filter references any of the original columns
        if hasattr(filter_condition, "column") and hasattr(
            filter_condition.column, "name"
        ):
            column_name = filter_condition.column.name
            return column_name in original_columns
        elif hasattr(filter_condition, "name"):
            column_name = filter_condition.name
            return column_name in original_columns

        return True  # Default to early filter if we can't determine
