"""
Column validation for DataFrame operations.

This module provides centralized column validation logic that was previously
scattered throughout DataFrame, ensuring consistent validation across
all operations.
"""

from typing import Any, List, Optional
from ...spark_types import StructType
from ...functions import Column, ColumnOperation
from ...core.exceptions.operation import SparkColumnNotFoundError


def is_literal(expression: Any) -> bool:
    """Check if expression is a literal value that doesn't need column validation.

    Args:
        expression: The expression to check

    Returns:
        True if expression is a literal value (Literal, str, int, etc)
    """
    from ...functions.core.literals import Literal

    # Check if it's a Literal
    if isinstance(expression, Literal):
        return True

    # Check if it's a ColumnOperation with a Literal
    if isinstance(expression, ColumnOperation):
        if hasattr(expression, "value") and isinstance(expression.value, Literal):
            return True
        if hasattr(expression, "column") and isinstance(expression.column, Literal):
            return True

    # Check if it's a string representation of a Literal
    return bool(
        isinstance(expression, str)
        and "<sparkless.functions.core.literals.Literal" in expression
    )


class ColumnValidator:
    """Validates column existence and expressions for DataFrame operations.

    This class centralizes all column validation logic that was previously
    scattered throughout DataFrame, ensuring consistent validation
    across all operations.
    """

    @staticmethod
    def _find_column_case_insensitive(
        schema: StructType, column_name: str
    ) -> Optional[str]:
        """Find column name in schema case-insensitively."""
        for field in schema.fields:
            if field.name.lower() == column_name.lower():
                return field.name
        return None

    @staticmethod
    def validate_column_exists(
        schema: StructType, column_name: str, operation: str
    ) -> None:
        """Validate that a single column exists in schema (case-insensitive).

        Args:
            schema: The DataFrame schema to validate against.
            column_name: Name of the column to validate.
            operation: Name of the operation being performed (for error messages).

        Raises:
            SparkColumnNotFoundError: If column doesn't exist in schema.
        """
        # Skip validation for wildcard selector
        if column_name == "*":
            return

        if ColumnValidator._find_column_case_insensitive(schema, column_name) is None:
            column_names = [field.name for field in schema.fields]
            raise SparkColumnNotFoundError(column_name, column_names)

    @staticmethod
    def validate_columns_exist(
        schema: StructType, column_names: List[str], operation: str
    ) -> None:
        """Validate that multiple columns exist in schema (case-insensitive).

        Args:
            schema: The DataFrame schema to validate against.
            column_names: List of column names to validate.
            operation: Name of the operation being performed (for error messages).

        Raises:
            SparkColumnNotFoundError: If any column doesn't exist in schema.
        """
        available_columns = [field.name for field in schema.fields]
        missing_columns = []
        for col in column_names:
            if ColumnValidator._find_column_case_insensitive(schema, col) is None:
                missing_columns.append(col)
        if missing_columns:
            raise SparkColumnNotFoundError(missing_columns[0], available_columns)

    @staticmethod
    def validate_filter_expression(
        schema: StructType,
        condition: Any,
        operation: str,
        has_pending_joins: bool = False,
    ) -> None:
        """Validate filter expressions before execution.

        Args:
            schema: The DataFrame schema to validate against.
            condition: The filter condition to validate.
            operation: Name of the operation being performed.
            has_pending_joins: Whether there are pending join operations.
        """
        # Skip validation for empty dataframes - they can filter on any column
        if len(schema.fields) == 0:
            return

        # Skip validation for complex expressions - let SQL generation handle them
        # Only validate simple column references

        # Import ColumnOperation for type checking
        from sparkless.functions.base import ColumnOperation

        # If condition is a ColumnOperation, validate its column references
        if isinstance(condition, ColumnOperation):
            # Validate operations that reference columns
            if hasattr(condition, "column"):
                # For filter operations, use lazy materialization mode to allow
                # column references from original DataFrame context (PySpark behavior)
                is_lazy = (operation == "filter" and has_pending_joins) or (
                    operation == "filter"
                )  # Always allow lazy mode for filters
                # Recursively validate the column references in the expression
                ColumnValidator.validate_expression_columns(
                    schema, condition, operation, in_lazy_materialization=is_lazy
                )
            return

        if hasattr(condition, "column") and hasattr(condition.column, "name"):
            # Check if this is a complex operation before validating
            if hasattr(condition, "operation") and condition.operation in [
                "between",
                "and",
                "or",
                "&",
                "|",
                "isin",
                "not_in",
                "!",
                ">",
                "<",
                ">=",
                "<=",
                "==",
                "!=",
                "*",
                "+",
                "-",
                "/",
            ]:
                # Validate column references in the expression
                # For filter operations, allow lazy materialization mode
                is_lazy = operation == "filter"
                ColumnValidator.validate_expression_columns(
                    schema, condition, operation, in_lazy_materialization=is_lazy
                )
                return
            # Simple column reference
            ColumnValidator.validate_column_exists(
                schema, condition.column.name, operation
            )
        elif (
            hasattr(condition, "name")
            and not hasattr(condition, "operation")
            and not hasattr(condition, "value")
            and not hasattr(condition, "data_type")
        ):
            # Simple column reference without operation, value, or data_type (not a literal)
            ColumnValidator.validate_column_exists(schema, condition.name, operation)
        # For complex expressions (with operations, literals, etc.), skip validation
        # as they will be handled by SQL generation

    @staticmethod
    def _column_exists_in_schema(schema: StructType, column_name: str) -> bool:
        """Check if column exists in schema (case-insensitive).

        Args:
            schema: The DataFrame schema to check against.
            column_name: Name of the column to check.

        Returns:
            True if column exists in schema, False otherwise.
        """
        return (
            ColumnValidator._find_column_case_insensitive(schema, column_name)
            is not None
        )

    @staticmethod
    def validate_expression_columns(
        schema: StructType,
        expression: Any,
        operation: str,
        in_lazy_materialization: bool = False,
    ) -> None:
        """Recursively validate column references in complex expressions.

        Args:
            schema: The DataFrame schema to validate against.
            expression: The expression to validate.
            operation: Name of the operation being performed.
            in_lazy_materialization: Whether we're in lazy materialization context.
        """
        # Skip validation for literal values
        if is_literal(expression):
            return

        if isinstance(expression, ColumnOperation):
            # Skip validation for expr operations - they don't reference actual columns
            if hasattr(expression, "operation") and expression.operation == "expr":
                return

            # Check if this is a column reference
            if hasattr(expression, "column"):
                # Check if it's a Literal - skip validation
                if is_literal(expression.column):
                    pass  # Skip literals
                # Check if it's a DataFrame (has 'data' attribute) - skip validation
                elif hasattr(expression.column, "data") and hasattr(
                    expression.column, "schema"
                ):
                    pass  # Skip DataFrame objects
                elif isinstance(expression.column, ColumnOperation):
                    # The column itself is a ColumnOperation (e.g., struct, array) - validate it recursively
                    # But first check if this ColumnOperation represents a column that exists in the schema
                    # If it does, skip recursive validation to avoid checking dropped columns (issue #168)
                    should_skip_recursive = False
                    if hasattr(expression.column, "name"):
                        col_name = expression.column.name
                        if ColumnValidator._column_exists_in_schema(schema, col_name):
                            should_skip_recursive = True

                    if not should_skip_recursive:
                        ColumnValidator.validate_expression_columns(
                            schema,
                            expression.column,
                            operation,
                            in_lazy_materialization,
                        )
                elif isinstance(expression.column, Column):
                    # Skip validation for dummy columns created by F.expr() and F.struct()
                    if expression.column.name in (
                        "__expr__",
                        "__struct_dummy__",
                        "__create_map_base__",
                        "__create_map_dummy__",
                    ):
                        return

                    # Check if the column name is actually a Literal (string representation)
                    col_name = expression.column.name
                    if (
                        isinstance(col_name, str)
                        and "<sparkless.functions.core.literals.Literal" in col_name
                    ):
                        # This is a Literal used as a column - skip validation
                        pass
                    elif col_name != "*":
                        # Skip validation for wildcard selector
                        # Always validate column exists in schema, even for filters
                        # This ensures consistent error messages and catches errors early
                        ColumnValidator.validate_column_exists(
                            schema, col_name, operation
                        )
                        # If column exists in schema, skip recursive validation of its internal structure
                        # This prevents validation errors when the column was created from expressions
                        # that referenced dropped columns (issue #168)
                        if ColumnValidator._column_exists_in_schema(schema, col_name):
                            # Column exists in schema - skip recursive validation of internal structure
                            # The column is already validated as existing, so we don't need to check
                            # its internal ColumnOperation structure which might reference dropped columns
                            pass
                        else:
                            # Column doesn't exist in schema - might be a complex expression being built
                            # Continue with recursive validation
                            pass

            # Recursively validate nested expressions
            # Only validate if the column doesn't exist in schema (for complex expressions being built)
            if hasattr(expression, "column"):
                if is_literal(expression.column):
                    # Skip validation for literals used as columns
                    pass
                elif isinstance(expression.column, ColumnOperation):
                    # Only recursively validate if the ColumnOperation doesn't represent an existing column
                    # Check if this ColumnOperation represents a column that exists in the schema
                    # If it does, skip recursive validation to avoid checking dropped columns
                    should_skip_recursive = False
                    if hasattr(expression.column, "name"):
                        col_name = expression.column.name
                        if ColumnValidator._column_exists_in_schema(schema, col_name):
                            should_skip_recursive = True

                    if not should_skip_recursive:
                        ColumnValidator.validate_expression_columns(
                            schema,
                            expression.column,
                            operation,
                            in_lazy_materialization,
                        )
                elif isinstance(expression.column, Column):
                    # If this Column exists in schema, skip recursive validation
                    col_name = expression.column.name
                    if ColumnValidator._column_exists_in_schema(schema, col_name):
                        # Column exists - already validated, skip recursive validation
                        pass
                    else:
                        # Column doesn't exist - might need recursive validation for complex expressions
                        # But for simple Column references, we've already validated above
                        pass
            if hasattr(expression, "value") and isinstance(
                expression.value, ColumnOperation
            ):
                # Check if this ColumnOperation represents a column that exists in the schema
                # If it does, skip recursive validation to avoid checking dropped columns (issue #168)
                should_skip_recursive = False
                if hasattr(expression.value, "name"):
                    col_name = expression.value.name
                    if ColumnValidator._column_exists_in_schema(schema, col_name):
                        should_skip_recursive = True

                if not should_skip_recursive:
                    ColumnValidator.validate_expression_columns(
                        schema, expression.value, operation, in_lazy_materialization
                    )
            elif hasattr(expression, "value") and is_literal(expression.value):
                # Skip validation for literals
                pass
            elif (
                hasattr(expression, "value")
                and isinstance(expression.value, Column)
                and not in_lazy_materialization
                and expression.value.name != "*"
            ):
                # Direct column reference in value
                # Skip validation for wildcard selector
                ColumnValidator.validate_column_exists(
                    schema, expression.value.name, operation
                )
            # Handle list/tuple of values (e.g., create_map with multiple args, array with literals)
            elif hasattr(expression, "value") and isinstance(
                expression.value, (list, tuple)
            ):
                for item in expression.value:
                    if is_literal(item):
                        continue  # Skip literals
                    elif isinstance(item, ColumnOperation):
                        # Recursively validate nested ColumnOperations (e.g., struct inside array)
                        # But first check if this ColumnOperation represents a column that exists in the schema
                        # If it does, skip recursive validation to avoid checking dropped columns (issue #168)
                        should_skip_recursive = False
                        if hasattr(item, "name"):
                            col_name = item.name
                            if ColumnValidator._column_exists_in_schema(
                                schema, col_name
                            ):
                                should_skip_recursive = True

                        if not should_skip_recursive:
                            ColumnValidator.validate_expression_columns(
                                schema, item, operation, in_lazy_materialization
                            )
                    elif (
                        isinstance(item, Column)
                        and not in_lazy_materialization
                        and item.name != "*"
                    ):
                        ColumnValidator.validate_column_exists(
                            schema, item.name, operation
                        )
                    # Skip other non-column types
        elif isinstance(expression, Column):
            # Check if this is an aliased column with an original column reference
            if (
                hasattr(expression, "_original_column")
                and expression._original_column is not None
            ):
                # This is an aliased column - validate the original column
                # Check if it's a DataFrame first
                if hasattr(expression._original_column, "data") and hasattr(
                    expression._original_column, "schema"
                ):
                    pass  # Skip DataFrame objects
                elif isinstance(expression._original_column, Column):
                    if (
                        not in_lazy_materialization
                        and expression._original_column.name != "*"
                    ):
                        # Skip validation for wildcard selector
                        ColumnValidator.validate_column_exists(
                            schema, expression._original_column.name, operation
                        )
                elif isinstance(expression._original_column, ColumnOperation):  # type: ignore[unreachable]
                    ColumnValidator.validate_expression_columns(
                        schema,
                        expression._original_column,
                        operation,
                        in_lazy_materialization,
                    )
            elif hasattr(expression, "column") and isinstance(
                expression.column, Column
            ):
                # This is a column operation - validate the column reference
                if not in_lazy_materialization and expression.column.name != "*":
                    # Skip validation for wildcard selector
                    ColumnValidator.validate_column_exists(
                        schema, expression.column.name, operation
                    )
            else:
                # Simple column reference - validate directly
                if not in_lazy_materialization and expression.name != "*":
                    # Skip validation for wildcard selector
                    ColumnValidator.validate_column_exists(
                        schema, expression.name, operation
                    )
