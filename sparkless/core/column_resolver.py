"""
Centralized column name resolution for Sparkless.

This module provides a unified column name resolution system that respects
the spark.sql.caseSensitive configuration setting, matching PySpark behavior.
"""

from typing import Dict, List, Optional
from ..core.exceptions.analysis import AnalysisException


class ColumnResolver:
    """Centralized column name resolution respecting case sensitivity.

    This class provides static methods for resolving column names based on
    the case sensitivity configuration, matching PySpark's behavior.
    """

    @staticmethod
    def resolve_column_name(
        column_name: str,
        available_columns: List[str],
        case_sensitive: bool = False,
    ) -> Optional[str]:
        """Resolve column name from available columns.

        Args:
            column_name: Column name to resolve.
            available_columns: List of available column names.
            case_sensitive: Whether to use case-sensitive matching.
                If False (default), performs case-insensitive matching.

        Returns:
            Actual column name if found, None otherwise.

        Raises:
            AnalysisException: If multiple columns match (ambiguity) when
                case_sensitive=False.

        Example:
            >>> ColumnResolver.resolve_column_name("name", ["Name", "Age"], False)
            'Name'
            >>> ColumnResolver.resolve_column_name("name", ["Name", "Age"], True)
            None
        """
        if case_sensitive:
            # Case-sensitive: exact match only
            return column_name if column_name in available_columns else None
        else:
            # Case-insensitive: find first match, but check for ambiguity
            column_name_lower = column_name.lower()
            matches = [
                col for col in available_columns if col.lower() == column_name_lower
            ]

            if len(matches) == 0:
                return None
            elif len(matches) == 1:
                return matches[0]
            else:
                # Ambiguity: multiple columns differ only by case
                raise AnalysisException(
                    f"Ambiguous column name '{column_name}'. "
                    f"Found multiple columns matching: {matches}. "
                    f"Use case-sensitive mode or rename columns to avoid ambiguity."
                )

    @staticmethod
    def resolve_columns(
        column_names: List[str],
        available_columns: List[str],
        case_sensitive: bool = False,
    ) -> Dict[str, str]:
        """Resolve multiple column names to actual names.

        Args:
            column_names: List of column names to resolve.
            available_columns: List of available column names.
            case_sensitive: Whether to use case-sensitive matching.

        Returns:
            Dictionary mapping requested column names to actual column names.

        Raises:
            AnalysisException: If any column cannot be resolved or if there
                is ambiguity (when case_sensitive=False).

        Example:
            >>> ColumnResolver.resolve_columns(
            ...     ["name", "age"], ["Name", "Age", "City"], False
            ... )
            {'name': 'Name', 'age': 'Age'}
        """
        resolved = {}
        for col_name in column_names:
            actual_name = ColumnResolver.resolve_column_name(
                col_name, available_columns, case_sensitive
            )
            if actual_name is None:
                raise AnalysisException(
                    f"Column '{col_name}' not found in available columns: "
                    f"{available_columns}"
                )
            resolved[col_name] = actual_name
        return resolved

    @staticmethod
    def column_exists(
        column_name: str,
        available_columns: List[str],
        case_sensitive: bool = False,
    ) -> bool:
        """Check if a column exists in available columns.

        Args:
            column_name: Column name to check.
            available_columns: List of available column names.
            case_sensitive: Whether to use case-sensitive matching.

        Returns:
            True if column exists, False otherwise.

        Note:
            This method does not raise exceptions for ambiguity. It simply
            checks if at least one match exists.
        """
        if case_sensitive:
            return column_name in available_columns
        else:
            column_name_lower = column_name.lower()
            return any(col.lower() == column_name_lower for col in available_columns)
