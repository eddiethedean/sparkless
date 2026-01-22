"""
Attribute handler for DataFrame.

This module provides attribute access handling for DataFrame, including
column access via dot notation and special attributes like .na.
"""

from typing import Any, Dict, List, Optional, Tuple, Union

from ..functions import Column


class NAHandler:
    """Handler for null/NA operations on DataFrame.

    Provides methods for handling null values in a DataFrame, matching PySpark's
    `.na` namespace API.

    Example:
        >>> df.na.fill(0)  # Fill all nulls with 0
        >>> df.na.fill({"col1": 0, "col2": "default"})  # Fill with dict mapping
    """

    def __init__(self, df: Any) -> None:
        """Initialize NAHandler with a DataFrame reference.

        Args:
            df: The DataFrame instance to operate on.
        """
        self._df = df

    def fill(
        self,
        value: Union[Any, Dict[str, Any]],
        subset: Optional[Union[str, List[str], Tuple[str, ...]]] = None,
    ) -> Any:
        """Fill null values (alias for fillna).

        Args:
            value: Value to fill nulls with. Can be a single value or a dict mapping
                   column names to fill values.
            subset: Optional column name(s) to limit fill operation to. Can be a
                    string (single column), list, or tuple of column names. If value
                    is a dict, subset is ignored.

        Returns:
            DataFrame with null values filled.

        Example:
            >>> df.na.fill(0)  # Fill all nulls with 0
            >>> df.na.fill({"col1": 0, "col2": "default"})  # Fill with dict
            >>> df.na.fill(0, subset=["col1", "col2"])  # Fill specific columns
        """
        return self._df.fillna(value, subset)

    def replace(
        self,
        to_replace: Union[int, float, str, List[Any], Dict[Any, Any]],
        value: Optional[Union[int, float, str, List[Any]]] = None,
        subset: Optional[Union[str, List[str], Tuple[str, ...]]] = None,
    ) -> Any:
        """Replace values in DataFrame (PySpark-compatible).

        Args:
            to_replace: Value(s) to replace - can be scalar, list, or dict mapping
                        old values to new values.
            value: Replacement value(s) - required if to_replace is not a dict.
                   If to_replace is a list and value is a list, they must have
                   the same length.
            subset: Optional column name(s) to limit replacement to. Can be a
                    string (single column), list, or tuple of column names.

        Returns:
            DataFrame with replaced values.

        Example:
            >>> # Replace with dict mapping
            >>> df.na.replace({"A": "TypeA", "B": "TypeB"}, subset=["Type"])
            >>> # Replace list of values with single value
            >>> df.na.replace([1, 2], 99, subset=["col1"])
            >>> # Replace single value
            >>> df.na.replace(1, 99)
        """
        # Convert subset to list if it's a string or tuple
        subset_list: Optional[List[str]] = None
        if subset is not None:
            if isinstance(subset, str):
                subset_list = [subset]
            elif isinstance(subset, tuple):
                subset_list = list(subset)
            else:
                subset_list = subset

        return self._df.replace(to_replace, value, subset_list)


class DataFrameAttributeHandler:
    """Handles attribute access for DataFrame."""

    @staticmethod
    def handle_getattribute(obj: Any, name: str, super_getattribute: Any) -> Any:
        """
        Handle __getattribute__ for DataFrame.

        This intercepts all attribute access for DataFrame objects.

        Args:
            obj: The DataFrame instance
            name: Name of the attribute/method being accessed
            super_getattribute: The super().__getattribute__ method

        Returns:
            The requested attribute/method

        Raises:
            AttributeError: If attribute doesn't exist
        """
        # Always allow access to private/protected attributes and core attributes
        if name.startswith("_") or name in ["data", "schema", "storage"]:
            return super_getattribute(name)

        # For public methods, just return the attribute
        return super_getattribute(name)

    @staticmethod
    def handle_getattr(obj: Any, name: str) -> Union[Column, Any]:
        """
        Handle __getattr__ for column access via dot notation.

        Enables df.column_name syntax for column access (PySpark compatibility).

        Args:
            obj: The DataFrame instance
            name: Name of the attribute being accessed

        Returns:
            Column instance for the column or NAHandler for 'na'

        Raises:
            SparkColumnNotFoundError: If column doesn't exist
        """
        # Special case: 'na' attribute returns NAHandler
        # This is a defensive check in case property access doesn't work
        if name == "na":
            return NAHandler(obj)

        # Avoid infinite recursion - access object.__getattribute__ directly
        try:
            columns = object.__getattribute__(obj, "columns")

            # Get case sensitivity setting from DataFrame
            # Use DataFrame's _is_case_sensitive() method if available
            case_sensitive = False
            try:
                if hasattr(obj, "_is_case_sensitive"):
                    case_sensitive = obj._is_case_sensitive()
                else:
                    # Fallback: try to get from SparkSession
                    spark = object.__getattribute__(obj, "_spark")
                    if hasattr(spark, "conf"):
                        case_sensitive = (
                            spark.conf.get("spark.sql.caseSensitive", "false") == "true"
                        )
            except (AttributeError, KeyError, TypeError):
                pass

            # Resolve column name with respect to case sensitivity setting
            from ..core.column_resolver import ColumnResolver

            resolved_name = ColumnResolver.resolve_column_name(
                name, columns, case_sensitive
            )

            if resolved_name:
                # Use F.col to create Column with resolved name
                from ..functions import F

                return F.col(resolved_name)

            # In case-sensitive mode, if no exact match, raise error immediately
            if case_sensitive:
                from ..core.exceptions.operation import SparkColumnNotFoundError

                raise SparkColumnNotFoundError(name, columns)
        except AttributeError:
            pass

        # If not a column, raise SparkColumnNotFoundError for better error messages
        # Use object.__getattribute__ to avoid recursion when accessing columns property
        try:
            available_cols = object.__getattribute__(obj, "columns")
        except AttributeError:
            available_cols = []
        from ..core.exceptions.operation import SparkColumnNotFoundError

        raise SparkColumnNotFoundError(name, available_cols)
