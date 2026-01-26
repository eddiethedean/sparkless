"""
Column implementation for Sparkless.

This module provides the Column class for DataFrame column operations,
maintaining compatibility with PySpark's Column interface.
"""

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

from ...core.interfaces.functions import IColumn
from ...spark_types import DataType, StringType

if TYPE_CHECKING:
    from ...window import WindowSpec
    from ..conditional import CaseWhen
    from ..window_execution import WindowFunction
    from ..base import AggregateFunction
    from .literals import Literal


class ColumnOperatorMixin:
    """Mixin providing common operator methods for Column and ColumnOperation."""

    if TYPE_CHECKING:

        @property
        def name(self) -> str: ...

    def _create_operation(self, operation: str, other: Any) -> "ColumnOperation":
        """Create a ColumnOperation with the given operation and other operand.

        Args:
            operation: The operation to perform (e.g., "==", "+", etc.)
            other: The other operand

        Returns:
            ColumnOperation instance
        """
        return ColumnOperation(self, operation, other)

    def __eq__(self, other: Any) -> "ColumnOperation":  # type: ignore[override]
        """Equality comparison."""
        return self._create_operation("==", other)

    def eqNullSafe(self, other: Any) -> "ColumnOperation":
        """Null-safe equality comparison (PySpark eqNullSafe).

        This behaves like PySpark's eqNullSafe:
        - If both sides are null, the comparison is True.
        - If exactly one side is null, the comparison is False.
        - Otherwise, it behaves like standard equality, including any backend-specific type coercion rules.
        """
        return self._create_operation("eqNullSafe", other)

    def __ne__(self, other: Any) -> "ColumnOperation":  # type: ignore[override]
        """Inequality comparison."""
        return self._create_operation("!=", other)

    def __lt__(self, other: Any) -> "ColumnOperation":
        """Less than comparison."""
        return self._create_operation("<", other)

    def __le__(self, other: Any) -> "ColumnOperation":
        """Less than or equal comparison."""
        return self._create_operation("<=", other)

    def __gt__(self, other: Any) -> "ColumnOperation":
        """Greater than comparison."""
        return self._create_operation(">", other)

    def __ge__(self, other: Any) -> "ColumnOperation":
        """Greater than or equal comparison."""
        return self._create_operation(">=", other)

    def __add__(self, other: Any) -> "ColumnOperation":
        """Addition operation."""
        return self._create_operation("+", other)

    def __sub__(self, other: Any) -> "ColumnOperation":
        """Subtraction operation."""
        return self._create_operation("-", other)

    def __mul__(self, other: Any) -> "ColumnOperation":
        """Multiplication operation."""
        return self._create_operation("*", other)

    def __truediv__(self, other: Any) -> "ColumnOperation":
        """Division operation."""
        return self._create_operation("/", other)

    def __mod__(self, other: Any) -> "ColumnOperation":
        """Modulo operation."""
        return self._create_operation("%", other)

    def __pow__(self, other: Any) -> "ColumnOperation":
        """Power operation (for `col ** 2`)."""
        return self._create_operation("**", other)

    def __radd__(self, other: Any) -> "ColumnOperation":
        """Reverse addition operation (for `2 + col`)."""
        # For commutative operations, we can just swap operands
        return self._create_operation("+", other)

    def __rsub__(self, other: Any) -> "ColumnOperation":
        """Reverse subtraction operation (for `2 - col`)."""
        # For non-commutative operations, create ColumnOperation with literal as left operand
        # This will evaluate as `other - self` which is correct for `2 - col`
        return ColumnOperation(other, "-", self)

    def __rmul__(self, other: Any) -> "ColumnOperation":
        """Reverse multiplication operation (for `2 * col`)."""
        # For commutative operations, we can just swap operands
        return self._create_operation("*", other)

    def __rtruediv__(self, other: Any) -> "ColumnOperation":
        """Reverse division operation (for `2 / col`)."""
        # For non-commutative operations, create ColumnOperation with literal as left operand
        # This will evaluate as `other / self` which is correct for `2 / col`
        return ColumnOperation(other, "/", self)

    def __rmod__(self, other: Any) -> "ColumnOperation":
        """Reverse modulo operation (for `2 % col`)."""
        # For non-commutative operations, create ColumnOperation with literal as left operand
        # This will evaluate as `other % self` which is correct for `2 % col`
        return ColumnOperation(other, "%", self)

    def __rpow__(self, other: Any) -> "ColumnOperation":
        """Reverse power operation (for `2 ** col` or `3.0 ** col`)."""
        # For non-commutative operations, create ColumnOperation with literal as left operand
        # This will evaluate as `other ** self` which is correct for `2 ** col`
        return ColumnOperation(other, "**", self)

    def __and__(self, other: Any) -> "ColumnOperation":
        """Logical AND operation."""
        return self._create_operation("&", other)

    def __or__(self, other: Any) -> "ColumnOperation":
        """Logical OR operation."""
        return self._create_operation("|", other)

    def __invert__(self) -> "ColumnOperation":
        """Logical NOT operation."""
        return self._create_operation("!", None)

    def __neg__(self) -> "ColumnOperation":
        """Unary minus operation (-column)."""
        return self._create_operation("-", None)

    def isnull(self) -> "ColumnOperation":
        """Check if column value is null."""
        return self._create_operation("isnull", None)

    def isnotnull(self) -> "ColumnOperation":
        """Check if column value is not null."""
        return self._create_operation("isnotnull", None)

    def isNull(self) -> "ColumnOperation":
        """Check if column value is null (PySpark compatibility)."""
        return self.isnull()

    def isNotNull(self) -> "ColumnOperation":
        """Check if column value is not null (PySpark compatibility)."""
        return self.isnotnull()

    def isin(self, *values: Any) -> "ColumnOperation":
        """Check if column value is in list of values.

        Args:
            *values: Variable number of values to check against. Can be passed as
                    individual arguments (e.g., col.isin(1, 2, 3)) or as a single
                    list (e.g., col.isin([1, 2, 3])) for backward compatibility.
                    Supports automatic type coercion for mixed types (e.g., checking
                    integers in a string column will convert values to strings).

        Returns:
            ColumnOperation representing the isin check.

        Example:
            >>> df.filter(F.col("value").isin(1, 2, 3))
            >>> df.filter(F.col("value").isin([1, 2, 3]))  # Also supported
            >>> df.filter(F.col("str_col").isin(1, 2, 3))  # Auto-converts to strings

        Note:
            Fixed in version 3.23.0 (Issue #226): Added support for ``*values`` arguments
            and automatic type coercion for mixed types to match PySpark behavior.
        """
        # Normalize: if single list argument provided, use it directly
        # Otherwise convert *args to list
        if len(values) == 1 and isinstance(values[0], (list, tuple)):
            # Backward compatibility: single list/tuple argument
            normalized_values = list(values[0])
        else:
            # Convert *args to list
            normalized_values = list(values)
        return self._create_operation("isin", normalized_values)

    def between(self, lower: Any, upper: Any) -> "ColumnOperation":
        """Check if column value is between lower and upper bounds."""
        return self._create_operation("between", (lower, upper))

    def like(self, pattern: str) -> "ColumnOperation":
        """SQL LIKE pattern matching."""
        return self._create_operation("like", pattern)

    def rlike(self, pattern: str) -> "ColumnOperation":
        """Regular expression pattern matching."""
        # Create operation with proper naming format: RLIKE(name, pattern)
        # Note: Pattern is used as-is without quotes to match PySpark format
        pattern_str = str(pattern) if not isinstance(pattern, str) else pattern
        return ColumnOperation(
            self, "rlike", pattern, name=f"RLIKE({self.name}, {pattern_str})"
        )

    def contains(self, literal: str) -> "ColumnOperation":
        """Check if column contains the literal string."""
        return self._create_operation("contains", literal)

    def startswith(self, literal: str) -> "ColumnOperation":
        """Check if column starts with the literal string."""
        return self._create_operation("startswith", literal)

    def endswith(self, literal: str) -> "ColumnOperation":
        """Check if column ends with the literal string."""
        return self._create_operation("endswith", literal)

    def substr(self, start: int, length: int) -> "ColumnOperation":
        """Extract substring from string column.

        Args:
            start: Starting position (1-indexed, can be negative for reverse indexing).
            length: Length of substring (required).

        Returns:
            ColumnOperation representing the substr operation.

        Example:
            >>> df.select(F.col("name").substr(1, 2))
        """
        return self._create_operation("substr", (start, length))

    def asc(self) -> "ColumnOperation":
        """Ascending sort order."""
        return self._create_operation("asc", None)

    def desc(self) -> "ColumnOperation":
        """Descending sort order."""
        return self._create_operation("desc", None)

    def desc_nulls_last(self) -> "ColumnOperation":
        """Descending sort order with nulls last."""
        return self._create_operation("desc_nulls_last", None)

    def desc_nulls_first(self) -> "ColumnOperation":
        """Descending sort order with nulls first."""
        return self._create_operation("desc_nulls_first", None)

    def asc_nulls_last(self) -> "ColumnOperation":
        """Ascending sort order with nulls last."""
        return self._create_operation("asc_nulls_last", None)

    def asc_nulls_first(self) -> "ColumnOperation":
        """Ascending sort order with nulls first."""
        return self._create_operation("asc_nulls_first", None)

    def cast(self, data_type: DataType) -> "ColumnOperation":
        """Cast column to different data type."""
        return self._create_operation("cast", data_type)

    def astype(self, data_type: Union[DataType, str]) -> "ColumnOperation":
        """Cast column to different data type (alias for cast).

        This method is an alias for cast() and matches PySpark's API.

        Args:
            data_type: The target data type (DataType object or string name like "date", "string", etc.).

        Returns:
            ColumnOperation representing the cast operation.

        Example:
            >>> df.select(F.col("name").astype("string"))
            >>> df.select(F.substring("date", 1, 10).astype("date"))
        """
        # Note: cast() accepts both DataType and str in practice, despite type hint
        return self.cast(data_type)  # type: ignore[arg-type]

    def getItem(self, key: Any) -> "ColumnOperation":
        """Get item from array by index or map by key.

        Args:
            key: Index (int) for array access or key (any) for map access.

        Returns:
            ColumnOperation representing the getItem operation. Returns None for
            out-of-bounds array access (matching PySpark behavior).

        Example:
            >>> df.select(F.col("array_col").getItem(0))
            >>> df.select(F.col("map_col").getItem("key"))
            >>> df.select(F.col("array_col").getItem(999))  # Returns None if out of bounds

        Note:
            Fixed in version 3.23.0 (Issue #227): Out-of-bounds array access now
            returns None instead of raising errors, matching PySpark behavior.
        """
        return self._create_operation("getItem", key)

    def withField(
        self,
        fieldName: str,
        col: Union["Column", "ColumnOperation", "Literal", Any],  # type: ignore[name-defined,unused-ignore]
    ) -> "ColumnOperation":
        """Add or replace a field in a struct column.

        Args:
            fieldName: Name of the field to add or replace
            col: Column expression for the new field value. Can be a Column,
                 ColumnOperation, Literal, or any value that will be converted to a Literal.

        Returns:
            ColumnOperation representing the withField operation.

        Example:
            >>> df.withColumn("my_struct", F.col("my_struct").withField("new_field", F.lit("value")))
            >>> df.withColumn("my_struct", F.col("my_struct").withField("existing_field", F.col("other_col")))

        Note:
            PySpark 3.1.0+ feature. Works only on struct columns.
            If field exists, it will be replaced. If it doesn't exist, it will be added.
        """
        from .literals import Literal

        # Convert col to appropriate type
        if isinstance(col, str):
            col = Column(col)
        elif not isinstance(col, (Column, ColumnOperation, Literal)):
            # Wrap literals in Literal
            col = Literal(col)

        # Store field name and column in a dict for the operation value
        return ColumnOperation(
            self,
            "withField",
            value={"fieldName": fieldName, "column": col},
            name=f"{self.name}.withField({fieldName}, ...)",
        )


class Column(ColumnOperatorMixin, IColumn):
    """Mock column expression for DataFrame operations.

    Provides a PySpark-compatible column expression that supports all comparison
    and logical operations. Used for creating complex DataFrame transformations
    and filtering conditions.
    """

    def __init__(self, name: str, column_type: Optional[DataType] = None):
        """Initialize Column.

        Args:
            name: Column name.
            column_type: Optional data type. Defaults to StringType if not specified.
        """
        self._name = name
        self._original_column: Optional[Column] = None
        self._alias_name: Optional[str] = None
        self.column_name = name
        self.column_type = column_type or StringType()
        self.operation = None
        self.operand = None
        self._operations: List[ColumnOperation] = []
        # Add expr attribute for PySpark compatibility
        self.expr = f"Column('{name}')"

    @property
    def name(self) -> str:
        """Get the column name (alias if set, otherwise original name)."""
        if hasattr(self, "_alias_name") and self._alias_name is not None:
            return self._alias_name
        return self._name

    @property
    def original_column(self) -> "Column":
        """Get the original column (for aliased columns)."""
        return getattr(self, "_original_column", self)

    def __eq__(self, other: Any) -> "ColumnOperation":  # type: ignore[override]
        """Equality comparison."""
        if isinstance(other, Column):
            return ColumnOperation(self, "==", other)
        return ColumnOperation(self, "==", other)

    def __hash__(self) -> int:
        """Hash method to make Column hashable."""
        return hash((self.name, self.column_type))

    def __getitem__(self, key: Any) -> "Column":
        """Support subscript notation for struct field access.

        Args:
            key: Field name (string) for struct field access.

        Returns:
            New Column with the struct field path (e.g., "StructVal.E1").

        Example:
            >>> F.col("StructVal")["E1"]  # Returns Column("StructVal.E1")
            >>> df.select(F.col("Person")["name"])

        Note:
            This enables PySpark-compatible subscript notation for accessing
            struct fields, matching the behavior of F.col("StructVal.E1").
        """
        if not isinstance(key, str):
            raise TypeError(
                f"Column subscript access only supports string keys for struct fields, got {type(key).__name__}"
            )

        # If this column has an alias (via _original_column), use the original column name
        # for the struct field path, not the alias name
        # e.g., F.col("StructVal").alias("SV")["E1"] -> Column("StructVal.E1"), not "SV.E1"
        base_column_name = self.name
        if hasattr(self, "_original_column") and self._original_column is not None:
            # Use the original column's name for the struct path
            base_column_name = self._original_column.name

        # Create a new Column with the struct field path
        # e.g., F.col("StructVal")["E1"] -> Column("StructVal.E1")
        field_path = f"{base_column_name}.{key}"
        return Column(field_path, self.column_type)

    def __str__(self) -> str:
        """Return string representation of column for SQL generation."""
        return self.name

    def alias(self, name: str) -> "IColumn":
        """Create an alias for the column."""
        aliased_column = Column(name, self.column_type)
        aliased_column._original_column = self
        aliased_column._alias_name = name
        return aliased_column

    def when(self, condition: "ColumnOperation", value: Any) -> "CaseWhen":
        """Start a CASE WHEN expression."""
        from ..conditional import CaseWhen

        return CaseWhen(self, condition, value)

    def otherwise(self, value: Any) -> "CaseWhen":
        """End a CASE WHEN expression with default value."""
        from ..conditional import CaseWhen

        return CaseWhen(self, None, value)

    def over(self, window_spec: "WindowSpec") -> "WindowFunction":
        """Apply window function over window specification."""
        from ..window_execution import WindowFunction

        return WindowFunction(self, window_spec)

    def count(self) -> "ColumnOperation":
        """Count non-null values in this column.

        Returns:
            ColumnOperation representing the count operation.
        """
        return ColumnOperation(self, "count", None)

    def avg(self) -> "ColumnOperation":  # noqa: F821
        """Average values in this column.

        Returns:
            ColumnOperation representing the avg function (PySpark-compatible).
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        # Create AggregateFunction first to get correct name generation
        agg_func = AggregateFunction(self, "avg", DoubleType())
        # Create ColumnOperation that wraps the aggregate function internally
        # This matches PySpark's behavior where aggregate functions return Column objects
        op = ColumnOperation(self, "avg", value=None, name=agg_func.name)
        # Store the aggregate function info for evaluation
        op._aggregate_function = agg_func
        return op

    def sum(self) -> "ColumnOperation":  # noqa: F821
        """Sum values in this column.

        Returns:
            ColumnOperation representing the sum function (PySpark-compatible).
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        # Create AggregateFunction first to get correct name generation
        agg_func = AggregateFunction(self, "sum", DoubleType())
        # Create ColumnOperation that wraps the aggregate function internally
        # This matches PySpark's behavior where aggregate functions return Column objects
        op = ColumnOperation(self, "sum", value=None, name=agg_func.name)
        # Store the aggregate function info for evaluation
        op._aggregate_function = agg_func
        return op

    def max(self) -> "ColumnOperation":  # noqa: F821
        """Maximum value in this column.

        Returns:
            ColumnOperation representing the max function (PySpark-compatible).
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        # Create AggregateFunction first to get correct name generation
        agg_func = AggregateFunction(self, "max", DoubleType())
        # Create ColumnOperation that wraps the aggregate function internally
        # This matches PySpark's behavior where aggregate functions return Column objects
        op = ColumnOperation(self, "max", value=None, name=agg_func.name)
        # Store the aggregate function info for evaluation
        op._aggregate_function = agg_func
        return op

    def min(self) -> "ColumnOperation":  # noqa: F821
        """Minimum value in this column.

        Returns:
            ColumnOperation representing the min function (PySpark-compatible).
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        # Create AggregateFunction first to get correct name generation
        agg_func = AggregateFunction(self, "min", DoubleType())
        # Create ColumnOperation that wraps the aggregate function internally
        # This matches PySpark's behavior where aggregate functions return Column objects
        op = ColumnOperation(self, "min", value=None, name=agg_func.name)
        # Store the aggregate function info for evaluation
        op._aggregate_function = agg_func
        return op

    def stddev(self) -> "ColumnOperation":  # noqa: F821
        """Standard deviation of values in this column.

        Returns:
            ColumnOperation representing the stddev function (PySpark-compatible).
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        # Create AggregateFunction first to get correct name generation
        agg_func = AggregateFunction(self, "stddev", DoubleType())
        # Create ColumnOperation that wraps the aggregate function internally
        # This matches PySpark's behavior where aggregate functions return Column objects
        op = ColumnOperation(self, "stddev", value=None, name=agg_func.name)
        # Store the aggregate function info for evaluation
        op._aggregate_function = agg_func
        return op

    def variance(self) -> "ColumnOperation":  # noqa: F821
        """Variance of values in this column.

        Returns:
            ColumnOperation representing the variance function (PySpark-compatible).
        """
        from ..base import AggregateFunction
        from ...spark_types import DoubleType

        # Create AggregateFunction first to get correct name generation
        agg_func = AggregateFunction(self, "variance", DoubleType())
        # Create ColumnOperation that wraps the aggregate function internally
        # This matches PySpark's behavior where aggregate functions return Column objects
        op = ColumnOperation(self, "variance", value=None, name=agg_func.name)
        # Store the aggregate function info for evaluation
        op._aggregate_function = agg_func
        return op

    def bitwise_not(self) -> "ColumnOperation":
        """Bitwise NOT operation on this column.

        Returns:
            ColumnOperation representing the bitwise_not function.
        """
        # PySpark uses ~column for bitwise_not column names
        return ColumnOperation(self, "bitwise_not", name=f"~{self.name}")


class ColumnOperation(Column):
    """Represents a column operation (comparison, arithmetic, etc.).

    This class encapsulates column operations and their operands for evaluation
    during DataFrame operations. Inherits from Column to ensure isinstance() checks
    pass for PySpark compatibility.
    """

    def __init__(
        self,
        column: Any,  # Can be Column, ColumnOperation, IColumn, mixin, or None
        operation: str,
        value: Any = None,
        name: Optional[str] = None,
    ):
        """Initialize ColumnOperation.

        Args:
            column: The column being operated on (can be None for some operations).
            operation: The operation being performed.
            value: The value or operand for the operation.
            name: Optional custom name for the operation.
        """
        # Set attributes needed for _generate_name() before calling super().__init__()
        # Store these temporarily since Column.__init__ will set operation to None
        self.column = column
        self.value = value
        # Store operation in a temporary variable to avoid mypy issues
        _operation = operation

        # Generate the name for the Column base class
        # We need to compute this before calling super().__init__()
        generated_name = name or ColumnOperation._generate_name_early_helper(
            column, _operation, value
        )

        # Call super().__init__() with the generated name
        # This ensures ColumnOperation is a proper Column instance
        super().__init__(generated_name)

        # Set ColumnOperation-specific attributes AFTER super().__init__()
        # (Column.__init__ sets self.operation = None, so we override it here)
        # Type annotation ensures mypy knows this is always a string in ColumnOperation
        self.operation = _operation  # type: ignore[assignment]
        self.function_name = _operation
        self.return_type: Optional[Any] = None  # Type hint for return type

        # Override _name with the actual generated name (in case name was provided)
        if name is not None:
            self._name = name

        # Dynamic attributes for aggregate functions, UDFs, and window operations
        # These are set dynamically and may not always be present
        self._aggregate_function: Optional[AggregateFunction] = None
        self._udf_func: Optional[Any] = None
        self._udf_return_type: Optional[Any] = None
        self._udf_cols: Optional[List[Any]] = None
        self._is_pandas_udf: Optional[bool] = None
        self._is_table_udf: Optional[bool] = None
        self._window_duration: Optional[str] = None
        self._window_slide: Optional[str] = None
        self._window_start: Optional[str] = None

        # Ensure column_name is set (Column.__init__ sets it, but we want the operation name)
        self.column_name = self._name

    @staticmethod
    def _generate_name_early_helper(column: Any, operation: str, value: Any) -> str:
        """Generate a name for this operation (static helper for use before super().__init__()).

        This is a static helper method that contains the same logic as _generate_name()
        but can be called before the Column base class is fully initialized.
        """
        # Extract value from Literal if needed
        if hasattr(value, "value") and hasattr(value, "data_type"):
            # This is a Literal
            value_str = str(value.value)
        else:
            value_str = str(value) if value is not None else "None"

        # Handle column reference - use str() to get proper SQL for ColumnOperation
        if column is None:
            # For functions without column input (like current_date, current_timestamp)
            return operation + "()"
        # Handle Column objects properly
        if hasattr(column, "name"):
            column_ref = column.name
        else:
            # For ColumnOperation or other types, use string representation
            column_ref = str(column) if column is not None else "None"

        if operation == "bitwise_not":
            # PySpark uses ~column for bitwise_not
            return f"~{column_ref}"
        elif operation == "==":
            return f"{column_ref} = {value_str}"
        elif operation == "!=":
            return f"{column_ref} != {value_str}"
        elif operation == "eqNullSafe":
            # Use SQL-style null-safe equality operator semantics in the name
            return f"{column_ref} IS NOT DISTINCT FROM {value_str}"
        elif operation == "<":
            return f"{column_ref} < {value_str}"
        elif operation == "<=":
            return f"{column_ref} <= {value_str}"
        elif operation == ">":
            return f"{column_ref} > {value_str}"
        elif operation == ">=":
            return f"{column_ref} >= {value_str}"
        elif operation == "+":
            return f"({column_ref} + {value_str})"
        elif operation == "-":
            return f"({column_ref} - {value_str})"
        elif operation == "*":
            return f"({column_ref} * {value_str})"
        elif operation == "/":
            return f"({column_ref} / {value_str})"
        elif operation == "%":
            return f"({column_ref} % {value_str})"
        elif operation == "&":
            return f"({column_ref} & {value_str})"
        elif operation == "|":
            return f"({column_ref} | {value_str})"
        elif operation == "!":
            return f"(CASE WHEN {column_ref} THEN FALSE ELSE TRUE END)"
        elif operation == "isnull":
            if column is None or not hasattr(column, "name"):
                return "IS NULL"
            return f"{column.name} IS NULL"
        elif operation == "isnotnull":
            if column is None or not hasattr(column, "name"):
                return "IS NOT NULL"
            return f"{column.name} IS NOT NULL"
        elif operation == "isin":
            if column is None or not hasattr(column, "name"):
                return f"IN {value}"
            return f"{column.name} IN {value}"
        elif operation == "between":
            if column is None or not hasattr(column, "name"):
                if value is None:
                    return "BETWEEN NULL AND NULL"
                return f"BETWEEN {value[0]} AND {value[1]}"
            if value is None:
                return f"{column.name} BETWEEN NULL AND NULL"
            return f"{column.name} BETWEEN {value[0]} AND {value[1]}"
        elif operation == "like":
            if column is None or not hasattr(column, "name"):
                return f"LIKE {value}"
            return f"{column.name} LIKE {value}"
        elif operation == "rlike":
            if column is None or not hasattr(column, "name"):
                return f"RLIKE {value}"
            return f"{column.name} RLIKE {value}"
        elif operation == "asc":
            if column is None or not hasattr(column, "name"):
                return "ASC"
            return f"{column.name} ASC"
        elif operation == "desc":
            if column is None or not hasattr(column, "name"):
                return "DESC"
            return f"{column.name} DESC"
        elif operation == "desc_nulls_last":
            if column is None or not hasattr(column, "name"):
                return "DESC NULLS LAST"
            return f"{column.name} DESC NULLS LAST"
        elif operation == "desc_nulls_first":
            if column is None or not hasattr(column, "name"):
                return "DESC NULLS FIRST"
            return f"{column.name} DESC NULLS FIRST"
        elif operation == "asc_nulls_last":
            if column is None or not hasattr(column, "name"):
                return "ASC NULLS LAST"
            return f"{column.name} ASC NULLS LAST"
        elif operation == "asc_nulls_first":
            if column is None or not hasattr(column, "name"):
                return "ASC NULLS FIRST"
            return f"{column.name} ASC NULLS FIRST"
        elif operation == "cast":
            # Map PySpark type names to DuckDB/SQL type names (DuckDB backend only)
            type_mapping = {
                "int": "INTEGER",
                "integer": "INTEGER",
                "long": "BIGINT",
                "bigint": "BIGINT",
                "double": "DOUBLE",
                "float": "FLOAT",
                "string": "VARCHAR",
                "varchar": "VARCHAR",
                "boolean": "BOOLEAN",
                "bool": "BOOLEAN",
                "date": "DATE",
                "timestamp": "TIMESTAMP",
            }
            if column is None or not hasattr(column, "name"):
                col_name = "column"
            else:
                col_name = column.name
            if isinstance(value, str):
                sql_type = type_mapping.get(value.lower(), value.upper())
            else:
                # If value is a DataType, use its SQL representation
                sql_type = str(value)
            return f"CAST({col_name} AS {sql_type})"
        elif operation == "from_unixtime":
            # Handle from_unixtime function properly
            if column is None or not hasattr(column, "name"):
                col_name = "column"
            else:
                col_name = column.name
            if value is not None:
                return f"from_unixtime({col_name}, '{value}')"
            else:
                return f"from_unixtime({col_name})"
        elif operation == "array_sort":
            # Handle array_sort -> LIST_SORT or LIST_REVERSE_SORT
            if column is None or not hasattr(column, "name"):
                col_name = "column"
            else:
                col_name = column.name
            asc = value if isinstance(value, bool) else True
            if asc:
                return f"LIST_SORT({col_name})"
            else:
                return f"LIST_REVERSE_SORT({col_name})"
        elif operation == "array_reverse":
            if column is None or not hasattr(column, "name"):
                col_name = "column"
            else:
                col_name = column.name
            return f"LIST_REVERSE({col_name})"
        elif operation == "array_size":
            if column is None or not hasattr(column, "name"):
                col_name = "column"
            else:
                col_name = column.name
            return f"LEN({col_name})"
        elif operation == "array_max":
            if column is None or not hasattr(column, "name"):
                col_name = "column"
            else:
                col_name = column.name
            return f"LIST_MAX({col_name})"
        elif operation == "array_min":
            if column is None or not hasattr(column, "name"):
                col_name = "column"
            else:
                col_name = column.name
            return f"LIST_MIN({col_name})"
        elif operation == "struct":
            # Generate struct name from columns/literals in value
            if value is not None and isinstance(value, (list, tuple)):
                col_names = []
                for col in value:
                    if hasattr(col, "value") and hasattr(col, "data_type"):
                        # It's a Literal
                        col_names.append(str(col.value))
                    elif hasattr(col, "name"):
                        col_names.append(col.name)
                    else:
                        col_names.append(str(col))
                # Also include the first column if it's not a dummy
                if (
                    column
                    and hasattr(column, "name")
                    and column.name != "__struct_dummy__"
                ):
                    if hasattr(column, "value") and hasattr(column, "data_type"):
                        col_names.insert(0, str(column.value))
                    else:
                        col_names.insert(0, column.name)
                return f"struct({', '.join(col_names)})"
            # Fallback to default
            return "struct(...)"
        else:
            # For aggregate functions and other operations, generate a standard name
            if column is not None and hasattr(column, "name"):
                return f"{operation}({column.name})"
            else:
                return f"{operation}({column_ref})"

    def _generate_name_early(self) -> str:
        """Generate a name for this operation (called before super().__init__()).

        This method delegates to the static helper method.
        """
        # Use the instance attributes which are set after super().__init__()
        # self.operation is guaranteed to be a string in ColumnOperation
        op_str: str = self.operation  # type: ignore[assignment]
        return ColumnOperation._generate_name_early_helper(
            self.column, op_str, self.value
        )

    @property
    def name(self) -> str:
        """Get column name."""
        # If there's an alias, use it
        if hasattr(self, "_alias_name") and self._alias_name:
            return self._alias_name
        # For cast operations, PySpark keeps the original column name
        # self.operation is guaranteed to be a string in ColumnOperation
        op_str: str = self.operation  # type: ignore[assignment]
        if (
            op_str == "cast"
            and hasattr(self, "column")
            and hasattr(self.column, "name")
        ):
            col_name = getattr(self.column, "name", "")
            return col_name if isinstance(col_name, str) else str(col_name)
        # If _name was explicitly set (e.g., by datetime functions), use it
        # Check this BEFORE falling back to SQL representation for datetime operations
        # This ensures PySpark-style names (like "year(hire_date)") are used instead of SQL
        if self._name:
            # Check if _name was explicitly provided and differs from what _generate_name() would produce
            # For datetime functions, this will be the PySpark-style name like "year(hire_date)"
            generated_name = self._generate_name()
            if self._name != generated_name:
                return self._name
            # Also check if _name differs from SQL representation (str(self))
            # This catches cases where _name was set to PySpark-style but str(self) returns SQL-style
            sql_repr = str(self)
            if self._name != sql_repr:
                return self._name
        # For datetime and comparison operations, use the SQL representation
        # But only if _name wasn't explicitly set to a different value
        if op_str in [
            "hour",
            "minute",
            "second",
            "year",
            "month",
            "day",
            "dayofmonth",
            "dayofweek",
            "dayofyear",
            "weekofyear",
            "quarter",
            "to_date",
            "to_timestamp",
            "==",
            "!=",
            "<",
            ">",
            "<=",
            ">=",
        ]:
            return str(self)
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """Set column name."""
        self._name = value

    def __str__(self) -> str:
        """Generate SQL representation of this operation."""
        # For datetime functions, generate proper SQL
        if self.operation in ["hour", "minute", "second"]:
            return f"extract({self.operation} from TRY_CAST({self.column.name} AS TIMESTAMP))"
        elif self.operation in ["year", "month", "day", "dayofmonth"]:
            part = "day" if self.operation == "dayofmonth" else self.operation
            return f"extract({part} from TRY_CAST({self.column.name} AS DATE))"
        elif self.operation in ["dayofweek", "dayofyear", "weekofyear", "quarter"]:
            part_map: Dict[str, str] = {
                "dayofweek": "dow",
                "dayofyear": "doy",
                "weekofyear": "week",
                "quarter": "quarter",
            }
            # self.operation is guaranteed to be a string in ColumnOperation
            op_str: str = self.operation  # type: ignore[assignment]
            part = part_map.get(op_str, op_str)

            # PySpark dayofweek returns 1-7 (Sunday=1, Saturday=7)
            # DuckDB DOW returns 0-6 (Sunday=0, Saturday=6) - NOTE: DuckDB backend only
            # Add 1 to dayofweek to match PySpark
            if self.operation == "dayofweek":
                return f"CAST(extract({part} from TRY_CAST({self.column.name} AS DATE)) + 1 AS INTEGER)"
            else:
                return f"CAST(extract({part} from TRY_CAST({self.column.name} AS DATE)) AS INTEGER)"
        elif self.operation in ["to_date", "to_timestamp"]:
            if self.value is not None:
                return f"STRPTIME({self.column.name}, '{self.value}')"
            else:
                target_type = "DATE" if self.operation == "to_date" else "TIMESTAMP"
                return f"TRY_CAST({self.column.name} AS {target_type})"
        elif self.operation in ["==", "!=", "<", ">", "<=", ">=", "eqNullSafe"]:
            # For comparison operations, generate proper SQL
            left = (
                str(self.column)
                if hasattr(self.column, "__str__")
                else self.column.name
            )
            right = str(self.value) if self.value is not None else "NULL"
            if self.operation == "eqNullSafe":
                # Implement null-safe equality using SQL's IS NOT DISTINCT FROM,
                # which treats NULL = NULL as TRUE and NULL compared to non-NULL as FALSE.
                return f"({left} IS NOT DISTINCT FROM {right})"
            return f"({left} {self.operation} {right})"
        elif self.operation == "cast":
            # For cast operations, use the generated name which handles proper SQL syntax
            return self._generate_name()
        else:
            # For other operations, use the generated name
            return self._generate_name()

    def _generate_name(self) -> str:
        """Generate a name for this operation.

        This method delegates to _generate_name_early() which contains
        the actual implementation. This allows the same logic to be used
        both before and after super().__init__() is called.
        """
        return self._generate_name_early()

    def alias(self, name: str) -> "IColumn":
        """Create an alias for this operation."""
        # self.operation is guaranteed to be a string in ColumnOperation
        op_str: str = self.operation  # type: ignore[assignment]
        aliased_operation = ColumnOperation(
            self.column, op_str, self.value, name=self._name
        )
        aliased_operation._alias_name = name
        # Preserve _aggregate_function if present (for PySpark-compatible aggregate functions)
        if hasattr(self, "_aggregate_function"):
            aliased_operation._aggregate_function = self._aggregate_function
        return aliased_operation

    def over(self, window_spec: "WindowSpec") -> "WindowFunction":
        """Apply window function over window specification."""
        from ..window_execution import WindowFunction

        return WindowFunction(self, window_spec)
