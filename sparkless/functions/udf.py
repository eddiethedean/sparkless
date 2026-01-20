"""
User-Defined Function (UDF) implementation for Sparkless.

This module provides the UserDefinedFunction class for wrapping Python
functions to use in DataFrame transformations.
"""

from typing import Any, Callable, Optional, Union
from sparkless.functions.core.column import Column
from sparkless.functions.core.operations import ColumnOperation


class UserDefinedFunction:
    """User-defined function wrapper (all PySpark versions).

    Wraps a Python function to be used in DataFrame transformations.
    Supports marking as nondeterministic and applying to columns.

    Example:
        >>> def upper_case(s):
        ...     return s.upper()
        >>> udf_func = UserDefinedFunction(upper_case, StringType())
        >>> df.select(udf_func("name").alias("upper_name"))
    """

    def __init__(
        self,
        func: Callable[..., Any],
        returnType: Any,
        name: Optional[str] = None,
        evalType: str = "SQL",
    ):
        """Initialize UserDefinedFunction.

        Args:
            func: Python function to wrap
            returnType: Return data type
            name: Optional function name
            evalType: Evaluation type ("SQL" or "PANDAS")
        """
        self.func = func
        self.returnType = returnType
        self.evalType = evalType
        self._name = name
        self._deterministic = True
        self._is_pandas_udf = evalType == "PANDAS"

    def asNondeterministic(self) -> "UserDefinedFunction":
        """Mark UDF as nondeterministic.

        Nondeterministic UDFs may return different results for the same input.
        This affects query optimization and caching.

        Returns:
            Self with nondeterministic flag set
        """
        self._deterministic = False
        return self

    def __call__(self, *cols: Union[str, Column]) -> ColumnOperation:
        """Apply UDF to columns.

        Args:
            *cols: Column names or Column objects

        Returns:
            ColumnOperation representing the UDF application
        """
        # Convert string column names to Column objects
        column_objs = []
        for col in cols:
            if isinstance(col, str):
                column_objs.append(Column(col))
            else:
                column_objs.append(col)

        # Create the first column operation
        if not column_objs:
            raise ValueError("UDF requires at least one column argument")

        first_col = column_objs[0]
        # Get column name safely
        col_name = getattr(first_col, "name", str(first_col))
        op = ColumnOperation(first_col, "udf", name=self._name or f"udf({col_name})")
        op._udf_func = self.func
        op._udf_return_type = self.returnType
        op._udf_cols = column_objs
        op._is_pandas_udf = self._is_pandas_udf

        return op


class UserDefinedTableFunction:
    """User-defined table function wrapper (PySpark 3.5+).

    Wraps a Python function that returns multiple rows (table-valued function).
    Similar to UserDefinedFunction but for functions that return tables.

    Example:
        >>> def split_string(s):
        ...     return [(char,) for char in s]
        >>> table_udf = UserDefinedTableFunction(split_string, StructType([...]))
        >>> df.select(table_udf("name").alias("chars"))
    """

    def __init__(
        self,
        func: Callable[..., Any],
        returnType: Any,
        name: Optional[str] = None,
    ):
        """Initialize UserDefinedTableFunction.

        Args:
            func: Python function to wrap (should return iterable of rows)
            returnType: Return schema (StructType)
            name: Optional function name
        """
        self.func = func
        self.returnType = returnType
        self._name = name

    def __call__(self, *cols: Union[str, Column]) -> ColumnOperation:
        """Apply table UDF to columns.

        Args:
            *cols: Column names or Column objects

        Returns:
            ColumnOperation representing the table UDF application
        """
        # Convert string column names to Column objects
        column_objs = []
        for col in cols:
            if isinstance(col, str):
                column_objs.append(Column(col))
            else:
                column_objs.append(col)

        # Create the first column operation
        if not column_objs:
            raise ValueError("Table UDF requires at least one column argument")

        first_col = column_objs[0]
        # Get column name safely
        col_name = getattr(first_col, "name", str(first_col))
        op = ColumnOperation(
            first_col, "table_udf", name=self._name or f"table_udf({col_name})"
        )
        op._udf_func = self.func
        op._udf_return_type = self.returnType
        op._udf_cols = column_objs
        op._is_table_udf = True

        return op
