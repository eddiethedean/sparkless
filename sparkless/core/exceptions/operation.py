"""
Operation-specific exceptions for Sparkless.

Provides detailed error messages for DataFrame operations,
SQL generation, and query execution failures.
"""

from typing import Any, Dict, List, Optional
from .base import SparkException


class SparkOperationError(SparkException):
    """Raised when DataFrame operation fails."""

    def __init__(self, operation: str, column: str, issue: str, suggestion: str = ""):
        """Initialize operation error.

        Args:
            operation: The operation that failed (e.g., "withColumn", "filter")
            column: The column involved in the operation
            issue: Description of what went wrong
            suggestion: Optional suggestion for fixing the issue
        """
        self.operation = operation
        self.column = column
        self.issue = issue
        self.suggestion = suggestion

        msg = f"Operation failed in {operation}\n"
        msg += f"Column: {column}\n"
        msg += f"Issue: {issue}"
        if suggestion:
            msg += f"\nSuggestion: {suggestion}"
        super().__init__(msg)


class SparkValidationError(SparkException):
    """Raised when data validation fails."""

    def __init__(self, column: str, value: str, expected_type: str, actual_type: str):
        """Initialize validation error.

        Args:
            column: The column being validated
            value: The value that failed validation
            expected_type: Expected data type
            actual_type: Actual data type
        """
        self.column = column
        self.value = value
        self.expected_type = expected_type
        self.actual_type = actual_type

        msg = f"Validation failed for column '{column}'\n"
        msg += f"Value: {value}\n"
        msg += f"Expected type: {expected_type}\n"
        msg += f"Actual type: {actual_type}"
        super().__init__(msg)


class SparkCompatibilityError(SparkException):
    """Raised when PySpark compatibility issue is detected."""

    def __init__(self, feature: str, pyspark_version: str, mock_version: str):
        """Initialize compatibility error.

        Args:
            feature: The feature that's not compatible
            pyspark_version: PySpark version being emulated
            mock_version: Mock-spark version
        """
        self.feature = feature
        self.pyspark_version = pyspark_version
        self.mock_version = mock_version

        msg = f"Compatibility issue with feature '{feature}'\n"
        msg += f"PySpark version: {pyspark_version}\n"
        msg += f"Mock-spark version: {mock_version}\n"
        msg += (
            "This feature may not be fully supported in the current compatibility mode."
        )
        super().__init__(msg)


class SparkSQLGenerationError(SparkException):
    """Raised when SQL generation fails."""

    def __init__(self, operation: str, sql_fragment: str, error: str):
        """Initialize SQL generation error.

        Args:
            operation: The operation being converted to SQL
            sql_fragment: The SQL fragment that failed
            error: The underlying error
        """
        self.operation = operation
        self.sql_fragment = sql_fragment
        self.error = error

        msg = f"SQL generation failed for operation '{operation}'\n"
        msg += f"SQL fragment: {sql_fragment}\n"
        msg += f"Error: {error}\n"
        msg += (
            "This may be due to unsupported SQL syntax or backend compatibility issues."
        )
        super().__init__(msg)


class SparkQueryExecutionError(SparkException):
    """Raised when query execution fails."""

    def __init__(self, sql: str, error: str, context: Optional[Dict[str, Any]] = None):
        """Initialize query execution error.

        Args:
            sql: The SQL query that failed
            error: The underlying error
            context: Optional context information
        """
        self.sql = sql
        self.error = error
        self.context = context or {}

        msg = "Query execution failed\n"
        msg += f"SQL: {sql}\n"
        msg += f"Error: {error}"
        if context:
            msg += f"\nContext: {context}"
        super().__init__(msg)


class SparkColumnNotFoundError(SparkException, AttributeError):
    """Raised when a column is not found."""

    def __init__(
        self,
        column_name: str,
        available_columns: List[str],
        custom_message: Optional[str] = None,
    ):
        """Initialize column not found error.

        Args:
            column_name: The column that was not found
            available_columns: List of available columns
            custom_message: Optional custom error message (for materialization errors, etc.)
        """
        self.column_name = column_name
        self.available_columns = available_columns

        if custom_message:
            msg = custom_message
        else:
            # Use PySpark-style error message format
            msg = f"cannot resolve '{column_name}' given input columns: [{', '.join(available_columns)}]"
        super().__init__(msg)


class SparkTypeMismatchError(SparkException):
    """Raised when there's a type mismatch in operations."""

    def __init__(
        self, operation: str, expected_type: str, actual_type: str, column: str = ""
    ):
        """Initialize type mismatch error.

        Args:
            operation: The operation that failed
            expected_type: Expected data type
            actual_type: Actual data type
            column: Optional column name
        """
        self.operation = operation
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.column = column

        msg = f"Type mismatch in operation '{operation}'\n"
        if column:
            msg += f"Column: {column}\n"
        msg += f"Expected type: {expected_type}\n"
        msg += f"Actual type: {actual_type}"
        super().__init__(msg)


class SparkUnsupportedOperationError(SparkException):
    """Raised when an operation is not supported."""

    def __init__(self, operation: str, reason: str = "", alternative: str = ""):
        """Initialize unsupported operation error.

        Args:
            operation: The operation that's not supported
            reason: Reason why it's not supported
            alternative: Alternative approach if available
        """
        self.operation = operation
        self.reason = reason
        self.alternative = alternative

        msg = f"Operation '{operation}' is not supported"
        if reason:
            msg += f"\nReason: {reason}"
        if alternative:
            msg += f"\nAlternative: {alternative}"
        super().__init__(msg)
