"""
Validation handler for DataFrame operations.

This module provides centralized validation logic for DataFrame operations,
following the Single Responsibility Principle by extracting validation concerns
from the main DataFrame class.
"""

from typing import Any, List, TYPE_CHECKING

if TYPE_CHECKING:
    from ..spark_types import StructType

from .validation.column_validator import ColumnValidator


class ValidationHandler:
    """Handles data validation for DataFrame operations.

    This class consolidates all validation logic that was previously
    scattered throughout DataFrame, providing a single point of
    validation for column existence, expression validation, and
    filter condition validation.
    """

    def __init__(self, dataframe: Any = None):
        """Initialize ValidationHandler with optional DataFrame reference.

        Args:
            dataframe: Optional DataFrame instance to get case sensitivity from.
        """
        self._dataframe = dataframe

    def _get_case_sensitive(self) -> bool:
        """Get case sensitivity setting from DataFrame.

        Returns:
            True if case-sensitive mode is enabled, False otherwise.
        """
        if self._dataframe and hasattr(self._dataframe, "_is_case_sensitive"):
            return bool(self._dataframe._is_case_sensitive())
        return False  # Default to case-insensitive

    def validate_column_exists(
        self, schema: "StructType", column_name: str, operation: str
    ) -> None:
        """Validate that a single column exists in schema.

        Args:
            schema: The DataFrame schema to validate against.
            column_name: Name of the column to validate.
            operation: Name of the operation being performed (for error messages).

        Raises:
            SparkColumnNotFoundError: If column doesn't exist in schema.
        """
        case_sensitive = self._get_case_sensitive()
        ColumnValidator.validate_column_exists(
            schema, column_name, operation, case_sensitive
        )

    def validate_columns_exist(
        self, schema: "StructType", column_names: List[str], operation: str
    ) -> None:
        """Validate that multiple columns exist in schema.

        Args:
            schema: The DataFrame schema to validate against.
            column_names: List of column names to validate.
            operation: Name of the operation being performed (for error messages).

        Raises:
            SparkColumnNotFoundError: If any column doesn't exist in schema.
        """
        case_sensitive = self._get_case_sensitive()
        ColumnValidator.validate_columns_exist(
            schema, column_names, operation, case_sensitive
        )

    def validate_filter_expression(
        self,
        schema: "StructType",
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
        case_sensitive = self._get_case_sensitive()
        ColumnValidator.validate_filter_expression(
            schema, condition, operation, has_pending_joins, case_sensitive
        )

    def validate_expression_columns(
        self,
        schema: "StructType",
        expression: Any,
        operation: str,
        in_lazy_materialization: bool = False,
    ) -> None:
        """Validate column references in complex expressions.

        Args:
            schema: The DataFrame schema to validate against.
            expression: The expression to validate.
            operation: Name of the operation being performed.
            in_lazy_materialization: Whether we're in lazy materialization context.
        """
        case_sensitive = self._get_case_sensitive()
        ColumnValidator.validate_expression_columns(
            schema, expression, operation, in_lazy_materialization, case_sensitive
        )
