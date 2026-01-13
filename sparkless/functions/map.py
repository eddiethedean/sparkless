"""
Map functions for Sparkless.

This module provides comprehensive map manipulation functions that match PySpark's
map function API. Includes operations for extracting keys, values, entries, and
combining maps for working with map columns in DataFrames.

Key Features:
    - Complete PySpark map function API compatibility
    - Key/value extraction (map_keys, map_values)
    - Entry operations (map_entries)
    - Map combination (map_concat, map_from_arrays)
    - Type-safe operations with proper return types
    - Support for both column references and map literals

Example:
    >>> from sparkless.sql import SparkSession, functions as F
    >>> spark = SparkSession("test")
    >>> data = [{"properties": {"key1": "val1", "key2": "val2"}}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.map_keys(F.col("properties"))).show()
    DataFrame[1 rows, 1 columns]
    map_keys(properties)
    ['key1', 'key2']
"""

from typing import Any, Callable, Optional, Union
from sparkless.functions.base import (
    Column,
    ColumnOperation,
    MockLambdaExpression,
)


class MapFunctions:
    """Collection of map manipulation functions."""

    @staticmethod
    def map_keys(column: Union[Column, str]) -> ColumnOperation:
        """Return an array of all keys in the map.

        Args:
            column: The map column.

        Returns:
            ColumnOperation representing the map_keys function.

        Example:
            >>> df.select(F.map_keys(F.col("properties")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "map_keys", name=f"map_keys({column.name})")

    @staticmethod
    def map_values(column: Union[Column, str]) -> ColumnOperation:
        """Return an array of all values in the map.

        Args:
            column: The map column.

        Returns:
            ColumnOperation representing the map_values function.

        Example:
            >>> df.select(F.map_values(F.col("properties")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "map_values", name=f"map_values({column.name})")

    @staticmethod
    def map_entries(column: Union[Column, str]) -> ColumnOperation:
        """Return an array of structs with key-value pairs.

        Args:
            column: The map column.

        Returns:
            ColumnOperation representing the map_entries function.

        Example:
            >>> df.select(F.map_entries(F.col("properties")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "map_entries", name=f"map_entries({column.name})"
        )

    @staticmethod
    def map_concat(*columns: Union[Column, str]) -> ColumnOperation:
        """Concatenate multiple maps into a single map.

        Args:
            *columns: Map columns to concatenate.

        Returns:
            ColumnOperation representing the map_concat function.

        Example:
            >>> df.select(F.map_concat(F.col("map1"), F.col("map2"), F.col("map3")))
        """
        if not columns:
            raise ValueError("At least one column must be provided")

        base_column = Column(columns[0]) if isinstance(columns[0], str) else columns[0]
        column_names = [
            col.name if hasattr(col, "name") else str(col) for col in columns
        ]

        return ColumnOperation(
            base_column,
            "map_concat",
            columns[1:],
            name=f"map_concat({', '.join(column_names)})",
        )

    @staticmethod
    def map_from_arrays(
        keys: Union[Column, str], values: Union[Column, str]
    ) -> ColumnOperation:
        """Create a map from two arrays (keys and values).

        Args:
            keys: Array column containing keys.
            values: Array column containing values.

        Returns:
            ColumnOperation representing the map_from_arrays function.

        Example:
            >>> df.select(F.map_from_arrays(F.col("keys"), F.col("values")))
        """
        if isinstance(keys, str):
            keys = Column(keys)
        if isinstance(values, str):
            values = Column(values)

        return ColumnOperation(
            keys,
            "map_from_arrays",
            values,
            name=f"map_from_arrays({keys.name}, {values.name})",
        )

    # Advanced Map Functions (PySpark 3.2+)

    @staticmethod
    def create_map(*cols: Union[Column, str, Any]) -> ColumnOperation:
        """Create a map from key-value pairs.

        Args:
            *cols: Alternating key-value columns/literals.

        Returns:
            ColumnOperation representing the create_map function.

        Example:
            >>> df.select(F.create_map(F.col("k1"), F.col("v1"), F.col("k2"), F.col("v2")))
        """
        from .core.literals import Literal

        if len(cols) < 2 or len(cols) % 2 != 0:
            raise ValueError(
                "create_map requires an even number of arguments (key-value pairs)"
            )

        # Use first column/literal as base, store ALL arguments as value
        # If it's a Literal, create a dummy Column for the base
        if isinstance(cols[0], Literal):
            # Create a dummy column for validation purposes
            base_col = Column("__create_map_base__")
        elif isinstance(cols[0], Column):
            base_col = cols[0]
        else:
            base_col = Column(str(cols[0]))

        # Generate PySpark-compatible name: map(key, value) or map(key1, value1, key2, value2, ...)
        from .core.literals import Literal

        name_parts = []
        for col in cols:
            if isinstance(col, Literal):
                name_parts.append(str(col.value))
            elif isinstance(col, Column):
                name_parts.append(col.name)
            else:
                name_parts.append(str(col))
        name = f"map({', '.join(name_parts)})"

        return ColumnOperation(
            base_col,
            "create_map",
            value=cols,  # Store all arguments, not just cols[1:]
            name=name,
        )

    @staticmethod
    def map_contains_key(column: Union[Column, str], key: Any) -> ColumnOperation:
        """Check if map contains a specific key.

        Args:
            column: The map column.
            key: The key to check for.

        Returns:
            ColumnOperation representing the map_contains_key function.

        Example:
            >>> df.select(F.map_contains_key(F.col("map"), "key"))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "map_contains_key",
            key,
            name=f"map_contains_key({column.name}, {key!r})",
        )

    @staticmethod
    def map_from_entries(column: Union[Column, str]) -> ColumnOperation:
        """Convert array of key-value structs to map.

        Args:
            column: Array column containing structs with 'key' and 'value' fields.

        Returns:
            ColumnOperation representing the map_from_entries function.

        Example:
            >>> df.select(F.map_from_entries(F.col("entries")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "map_from_entries", name=f"map_from_entries({column.name})"
        )

    @staticmethod
    def map_filter(
        column: Union[Column, str], function: Callable[[Any, Any], bool]
    ) -> ColumnOperation:
        """Filter map entries based on key-value predicate.

        This is a higher-order function that filters map entries using
        the provided lambda function.

        Args:
            column: The map column to filter.
            function: Lambda function (key, value) -> bool that returns True for entries to keep.

        Returns:
            ColumnOperation representing the map_filter function.

        Example:
            >>> df.select(F.map_filter(F.col("map"), lambda k, v: v > 10))
        """
        if isinstance(column, str):
            column = Column(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return ColumnOperation(
            column,
            "map_filter",
            value=lambda_expr,
            name=f"map_filter({column.name}, <lambda>)",
        )

    @staticmethod
    def transform_keys(
        column: Union[Column, str], function: Callable[[Any, Any], Any]
    ) -> ColumnOperation:
        """Transform map keys using a function.

        This is a higher-order function that transforms map keys using
        the provided lambda function.

        Args:
            column: The map column.
            function: Lambda function (key, value) -> new_key to transform keys.

        Returns:
            ColumnOperation representing the transform_keys function.

        Example:
            >>> df.select(F.transform_keys(F.col("map"), lambda k, v: F.upper(k)))
        """
        if isinstance(column, str):
            column = Column(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return ColumnOperation(
            column,
            "transform_keys",
            value=lambda_expr,
            name=f"transform_keys({column.name}, <lambda>)",
        )

    @staticmethod
    def transform_values(
        column: Union[Column, str], function: Callable[[Any, Any], Any]
    ) -> ColumnOperation:
        """Transform map values using a function.

        This is a higher-order function that transforms map values using
        the provided lambda function.

        Args:
            column: The map column.
            function: Lambda function (key, value) -> new_value to transform values.

        Returns:
            ColumnOperation representing the transform_values function.

        Example:
            >>> df.select(F.transform_values(F.col("map"), lambda k, v: v * 2))
        """
        if isinstance(column, str):
            column = Column(column)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        return ColumnOperation(
            column,
            "transform_values",
            value=lambda_expr,
            name=f"transform_values({column.name}, <lambda>)",
        )

    @staticmethod
    def map_zip_with(
        col1: Union[Column, str],
        col2: Union[Column, str],
        function: Callable[[Any, Any, Any], Any],
    ) -> ColumnOperation:
        """Merge two maps into a single map using a function (PySpark 3.1+).

        This is a higher-order function that combines two maps by applying
        the provided lambda function to matching keys.

        Args:
            col1: The first map column.
            col2: The second map column.
            function: Lambda function (key, value1, value2) -> new_value to combine values.

        Returns:
            ColumnOperation representing the map_zip_with function.

        Example:
            >>> df.select(F.map_zip_with(F.col("map1"), F.col("map2"), lambda k, v1, v2: v1 + v2))
        """
        if isinstance(col1, str):
            col1 = Column(col1)
        if isinstance(col2, str):
            col2 = Column(col2)

        # Wrap the lambda function
        lambda_expr = MockLambdaExpression(function)

        # Store col2 as a tuple with the lambda
        return ColumnOperation(
            col1,
            "map_zip_with",
            value=(col2, lambda_expr),
            name=f"map_zip_with({col1.name}, {col2.name}, <lambda>)",
        )

    @staticmethod
    def str_to_map(
        column: Union[Column, str],
        pair_delim: Optional[str] = ",",
        key_value_delim: Optional[str] = ":",
    ) -> ColumnOperation:
        """Convert string to map using delimiters.

        Args:
            column: The string column to convert.
            pair_delim: Delimiter between key-value pairs (default ',').
            key_value_delim: Delimiter between key and value (default ':').

        Returns:
            ColumnOperation representing the str_to_map function.
        """
        column = Column(column) if isinstance(column, str) else column
        operation = ColumnOperation(
            column,
            "str_to_map",
            (pair_delim, key_value_delim),
            name=f"str_to_map({column.name}, '{pair_delim}', '{key_value_delim}')",
        )
        return operation
