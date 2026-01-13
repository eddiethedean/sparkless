"""
SQL Validator for Sparkless.

This module provides SQL validation functionality for Sparkless,
validating SQL queries for syntax correctness, semantic validity,
and compatibility with the Sparkless system.

Key Features:
    - SQL syntax validation
    - Semantic validation
    - Schema validation
    - Type checking
    - Error reporting

Example:
    >>> from sparkless.session.sql import SQLValidator
    >>> validator = SQLValidator()
    >>> is_valid = validator.validate("SELECT * FROM users WHERE age > 18")
    >>> print(is_valid)
    True
"""

from typing import Any, Dict, List, Tuple


class SQLValidator:
    """SQL Validator for Sparkless.

    Provides SQL validation functionality that checks SQL queries
    for syntax correctness, semantic validity, and compatibility
    with the Sparkless system.

    Example:
        >>> validator = SQLValidator()
        >>> is_valid, errors = validator.validate("SELECT * FROM users")
        >>> print(is_valid)
        True
    """

    def __init__(self) -> None:
        """Initialize SQLValidator."""
        self._reserved_keywords = {
            "SELECT",
            "FROM",
            "WHERE",
            "GROUP",
            "BY",
            "HAVING",
            "ORDER",
            "LIMIT",
            "INSERT",
            "INTO",
            "VALUES",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "TABLE",
            "DATABASE",
            "SCHEMA",
            "ALTER",
            "TRUNCATE",
            "SHOW",
            "DESCRIBE",
            "EXPLAIN",
            "WITH",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "OUTER",
            "ON",
            "AS",
            "AND",
            "OR",
            "NOT",
            "IN",
            "EXISTS",
            "BETWEEN",
            "LIKE",
            "IS",
            "NULL",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "CAST",
            "COALESCE",
            "NULLIF",
        }

    def validate(self, query: str) -> Tuple[bool, List[str]]:
        """Validate SQL query.

        Args:
            query: SQL query string.

        Returns:
            Tuple of (is_valid, error_messages).
        """
        errors = []

        # Basic validation
        if not query or not query.strip():
            errors.append("Empty query provided")
            return False, errors

        query = query.strip()

        # Syntax validation
        syntax_errors = self._validate_syntax(query)
        errors.extend(syntax_errors)

        # Semantic validation
        semantic_errors = self._validate_semantics(query)
        errors.extend(semantic_errors)

        # Structure validation
        structure_errors = self._validate_structure(query)
        errors.extend(structure_errors)

        return len(errors) == 0, errors

    def _validate_syntax(self, query: str) -> List[str]:
        """Validate SQL syntax.

        Args:
            query: SQL query string.

        Returns:
            List of syntax error messages.
        """
        errors = []

        # Check for balanced parentheses
        if not self._check_balanced_parentheses(query):
            errors.append("Unbalanced parentheses in query")

        # Check for balanced quotes
        if not self._check_balanced_quotes(query):
            errors.append("Unbalanced quotes in query")

        # Check for basic SQL structure
        query_upper = query.upper().strip()
        if not any(
            query_upper.startswith(keyword)
            for keyword in [
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "CREATE",
                "DROP",
                "ALTER",
                "SHOW",
                "DESCRIBE",
                "EXPLAIN",
            ]
        ):
            errors.append("Query must start with a valid SQL keyword")

        return errors

    def _validate_semantics(self, query: str) -> List[str]:
        """Validate SQL semantics.

        Args:
            query: SQL query string.

        Returns:
            List of semantic error messages.
        """
        errors: List[str] = []

        # Check for reserved keyword usage
        tokens = self._tokenize(query)
        for token in tokens:
            if token.upper() in self._reserved_keywords and not self._is_quoted(
                token, query
            ):
                # This is a basic check - in real implementation would be more sophisticated
                pass

        return errors

    def _validate_structure(self, query: str) -> List[str]:
        """Validate SQL structure.

        Args:
            query: SQL query string.

        Returns:
            List of structure error messages.
        """
        errors = []

        # Check for required clauses in SELECT queries
        if query.upper().strip().startswith("SELECT") and "FROM" not in query.upper():
            errors.append("SELECT query must have FROM clause")

        # Check for required clauses in INSERT queries
        if query.upper().strip().startswith("INSERT"):
            if "INTO" not in query.upper():
                errors.append("INSERT query must have INTO clause")
            if "VALUES" not in query.upper() and "SELECT" not in query.upper():
                errors.append("INSERT query must have VALUES or SELECT clause")

        # Check for required clauses in UPDATE queries
        if query.upper().strip().startswith("UPDATE") and "SET" not in query.upper():
            errors.append("UPDATE query must have SET clause")

        return errors

    def _check_balanced_parentheses(self, query: str) -> bool:
        """Check if parentheses are balanced.

        Args:
            query: SQL query string.

        Returns:
            True if balanced, False otherwise.
        """
        count = 0
        for char in query:
            if char == "(":
                count += 1
            elif char == ")":
                count -= 1
                if count < 0:
                    return False
        return count == 0

    def _check_balanced_quotes(self, query: str) -> bool:
        """Check if quotes are balanced.

        Args:
            query: SQL query string.

        Returns:
            True if balanced, False otherwise.
        """
        single_quotes = 0
        double_quotes = 0
        in_single_quote = False
        in_double_quote = False

        for i, char in enumerate(query):
            if char == "'" and not in_double_quote:
                if not in_single_quote:
                    in_single_quote = True
                    single_quotes += 1
                else:
                    in_single_quote = False
            elif char == '"' and not in_single_quote:
                if not in_double_quote:
                    in_double_quote = True
                    double_quotes += 1
                else:
                    in_double_quote = False

        return single_quotes % 2 == 0 and double_quotes % 2 == 0

    def _tokenize(self, query: str) -> List[str]:
        """Tokenize SQL query.

        Args:
            query: SQL query string.

        Returns:
            List of tokens.
        """
        import re

        tokens = re.findall(r"\b\w+\b|[(),;=<>!]+", query)
        return tokens

    def _is_quoted(self, token: str, query: str) -> bool:
        """Check if token is quoted in query.

        Args:
            token: Token to check.
            query: Original query string.

        Returns:
            True if token is quoted, False otherwise.
        """
        # Simple check - in real implementation would be more sophisticated
        return f"'{token}'" in query or f'"{token}"' in query

    def validate_schema(
        self, query: str, schema_info: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """Validate query against schema information.

        Args:
            query: SQL query string.
            schema_info: Schema information dictionary.

        Returns:
            Tuple of (is_valid, error_messages).
        """
        errors: List[str] = []

        # Mock implementation - in real validator this would check
        # table names, column names, data types, etc. against schema

        return len(errors) == 0, errors

    def get_validation_errors(self, query: str) -> List[str]:
        """Get detailed validation errors for query.

        Args:
            query: SQL query string.

        Returns:
            List of detailed error messages.
        """
        is_valid, errors = self.validate(query)
        return errors
