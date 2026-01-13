"""
Analysis exception classes for Sparkless.

This module provides exception classes for analysis-related errors,
including SQL parsing, query analysis, and schema validation errors.
"""

from typing import Any, Dict, List, Optional
from .base import SparkException


class AnalysisException(SparkException):
    """Exception raised for SQL analysis errors.

    Raised when SQL queries or DataFrame operations fail due to analysis
    errors such as column not found, invalid syntax, or type mismatches.

    Args:
        message: Error message describing the analysis error.
        stackTrace: Optional stack trace information.
        error_code: Optional error code for programmatic handling.
        context: Optional context information (table name, query, etc).

    Example:
        >>> raise AnalysisException("Column 'unknown' does not exist", error_code="COLUMN_NOT_FOUND")
    """

    def __init__(
        self,
        message: str,
        stackTrace: Optional[Any] = None,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        error_class: Optional[str] = None,
        message_parameters: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, stackTrace)
        self.error_code = error_code
        self.context = context or {}
        self.error_class = error_class
        self.message_parameters = message_parameters or {}
        self._hint: Optional[str] = None

        # Add helpful migration hints for common errors
        message_lower = str(message).lower()
        if (
            "table or view not found" in message_lower
            or "table" in message_lower
            and "not found" in message_lower
        ):
            self._hint = (
                "In sparkless, ensure tables are created using "
                "spark.sql('CREATE TABLE ...') or storage API. "
                "For PySpark compatibility, use standard SQL commands."
            )
        elif "database" in message_lower and "not found" in message_lower:
            self._hint = (
                "In sparkless, create databases using "
                "spark.sql('CREATE DATABASE ...') or spark._storage.create_schema(...). "
                "For PySpark compatibility, use standard SQL commands."
            )
        elif "column" in message_lower and "not found" in message_lower:
            self._hint = (
                "Check that the column name is spelled correctly and exists in the DataFrame. "
                "Use df.printSchema() to see available columns."
            )

    def __str__(self) -> str:
        """String representation including migration hints if available."""
        message = super().__str__()
        if self._hint:
            return f"{message}\n\nHint: {self._hint}"
        return message


class ParseException(AnalysisException):
    """Exception raised for SQL parsing errors.

    Raised when SQL queries fail to parse due to syntax errors
    or invalid SQL constructs.

    Args:
        message: Error message describing the parsing error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise ParseException("Invalid SQL syntax")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class SchemaException(AnalysisException):
    """Exception raised for schema-related errors.

    Raised when schema operations fail due to invalid schema
    definitions or schema mismatches.

    Args:
        message: Error message describing the schema error.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise SchemaException("Schema mismatch detected")
    """

    def __init__(self, message: str, stackTrace: Optional[Any] = None):
        super().__init__(message, stackTrace)


class ColumnNotFoundException(AnalysisException):
    """Exception raised when a column is not found.

    Raised when attempting to access a column that doesn't exist
    in the DataFrame or table.

    Args:
        column_name: Name of the column that was not found.
        available_columns: List of available column names.
        table_name: Optional table name for context.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise ColumnNotFoundException("unknown_column", available_columns=["id", "name"])
    """

    def __init__(
        self,
        column_name: str,
        available_columns: Optional[List[str]] = None,
        table_name: Optional[str] = None,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            # Build enhanced error message
            message = f"Column '{column_name}' does not exist"

            if table_name:
                message += f" in table '{table_name}'"

            if available_columns:
                message += f". Available columns: {', '.join(available_columns)}"

                # Add suggestion if there's a similar column name
                suggestions = self._find_similar_columns(column_name, available_columns)
                if suggestions:
                    if len(suggestions) == 1:
                        message += f". Did you mean '{suggestions[0]}'?"
                    else:
                        suggestions_str = ", ".join(f"'{s}'" for s in suggestions)
                        message += f". Did you mean one of: {suggestions_str}?"

        context = {"table_name": table_name, "available_columns": available_columns}
        super().__init__(
            message, stackTrace, error_code="COLUMN_NOT_FOUND", context=context
        )
        self.column_name = column_name
        self.available_columns = available_columns or []
        self.table_name = table_name

    @staticmethod
    def _find_similar_columns(
        target: str, columns: List[str], max_suggestions: int = 3
    ) -> List[str]:
        """Find similar column names using Levenshtein-like similarity."""

        def similarity_score(s1: str, s2: str) -> float:
            """Simple similarity score based on character overlap (0-1)."""
            s1_lower = s1.lower()
            s2_lower = s2.lower()

            # Exact case-insensitive match
            if s1_lower == s2_lower:
                return 1.0

            # Check if one contains the other
            if s1_lower in s2_lower or s2_lower in s1_lower:
                return 0.8

            # Character overlap score
            s1_set = set(s1_lower)
            s2_set = set(s2_lower)
            if not s1_set or not s2_set:
                return 0.0

            intersection = len(s1_set & s2_set)
            union = len(s1_set | s2_set)
            return intersection / union if union > 0 else 0.0

        # Score each column and filter by threshold
        scored_columns = [(col, similarity_score(target, col)) for col in columns]

        # Filter columns with score > 0.5 and sort by score
        similar = [col for col, score in scored_columns if score > 0.5]

        # Sort by similarity score (descending)
        similar.sort(key=lambda col: similarity_score(target, col), reverse=True)

        return similar[:max_suggestions]


class TableNotFoundException(AnalysisException):
    """Exception raised when a table is not found.

    Raised when attempting to access a table that doesn't exist
    in the catalog.

    Args:
        table_name: Name of the table that was not found.
        available_tables: Optional list of available table names.
        database_name: Optional database name for context.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise TableNotFoundException("unknown_table", available_tables=["users", "orders"])
    """

    def __init__(
        self,
        table_name: str,
        available_tables: Optional[List[str]] = None,
        database_name: Optional[str] = None,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = f"Table '{table_name}' does not exist"

            if database_name:
                message += f" in database '{database_name}'"

            if available_tables:
                message += f". Available tables: {', '.join(available_tables)}"

        context = {"database_name": database_name, "available_tables": available_tables}
        super().__init__(
            message, stackTrace, error_code="TABLE_NOT_FOUND", context=context
        )
        self.table_name = table_name
        self.available_tables = available_tables or []
        self.database_name = database_name


class DatabaseNotFoundException(AnalysisException):
    """Exception raised when a database is not found.

    Raised when attempting to access a database that doesn't exist
    in the catalog.

    Args:
        database_name: Name of the database that was not found.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise DatabaseNotFoundException("unknown_database")
    """

    def __init__(
        self,
        database_name: str,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = f"Database '{database_name}' does not exist"
        super().__init__(message, stackTrace)
        self.database_name = database_name


class TypeMismatchException(AnalysisException):
    """Exception raised for type mismatch errors.

    Raised when there's a type mismatch between expected and actual
    data types in operations.

    Args:
        expected_type: Expected data type.
        actual_type: Actual data type.
        column_name: Optional column name for context.
        operation: Optional operation name for context.
        message: Optional custom error message.
        stackTrace: Optional stack trace information.

    Example:
        >>> raise TypeMismatchException("string", "integer", column_name="age")
    """

    def __init__(
        self,
        expected_type: str,
        actual_type: str,
        column_name: Optional[str] = None,
        operation: Optional[str] = None,
        message: Optional[str] = None,
        stackTrace: Optional[Any] = None,
    ):
        if message is None:
            message = f"Type mismatch: expected {expected_type}, got {actual_type}"

            if column_name:
                message += f" for column '{column_name}'"

            if operation:
                message += f" in operation '{operation}'"

        context = {
            "expected_type": expected_type,
            "actual_type": actual_type,
            "column_name": column_name,
            "operation": operation,
        }
        super().__init__(
            message, stackTrace, error_code="TYPE_MISMATCH", context=context
        )
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.column_name = column_name
        self.operation = operation
