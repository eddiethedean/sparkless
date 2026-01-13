"""
SQL parameter binder service for SparkSession.

This service handles SQL parameter binding and formatting
following the Single Responsibility Principle.
"""

from typing import Any, Dict, Tuple


class SQLParameterBinder:
    """Service for binding parameters to SQL queries."""

    def bind_parameters(
        self, query: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> str:
        """Bind parameters to SQL query safely.

        Args:
            query: SQL query with placeholders.
            args: Positional parameters.
            kwargs: Named parameters.

        Returns:
            Query with parameters bound.
        """
        # Handle positional parameters (?)
        if args:
            # Count placeholders
            placeholder_count = query.count("?")
            if len(args) != placeholder_count:
                raise ValueError(
                    f"Number of parameters ({len(args)}) does not match "
                    f"number of placeholders ({placeholder_count})"
                )

            # Replace each ? with the corresponding parameter
            result = query
            for arg in args:
                result = result.replace("?", self._format_param(arg), 1)
            query = result

        # Handle named parameters (:name)
        if kwargs:
            for name, value in kwargs.items():
                placeholder = f":{name}"
                if placeholder not in query:
                    raise ValueError(f"Parameter '{name}' not found in query")
                query = query.replace(placeholder, self._format_param(value))

        return query

    def _format_param(self, value: Any) -> str:
        """Format a parameter value for SQL safely.

        Args:
            value: Parameter value.

        Returns:
            Formatted parameter string.
        """
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes for SQL safety
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            # Default to string representation
            return f"'{str(value)}'"
