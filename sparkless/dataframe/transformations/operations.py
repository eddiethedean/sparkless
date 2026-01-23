"""
Transformation operations mixin for DataFrame.

This mixin provides transformation operations that can be mixed into
the DataFrame class to add transformation capabilities.
"""

from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    TYPE_CHECKING,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

from ...functions import Column, ColumnOperation, Literal
from ...spark_types import StructType, StructField
from ...core.exceptions import PySparkValueError
from ..protocols import SupportsDataFrameOps

SupportsDF = TypeVar("SupportsDF", bound=SupportsDataFrameOps)


class TransformationOperations(Generic[SupportsDF]):
    """Mixin providing transformation operations for DataFrame."""

    if TYPE_CHECKING:
        columns: List[str]
        data: List[Dict[str, Any]]
        schema: StructType
        storage: Any
        _operations_queue: List[Tuple[str, Any]]
        _watermark_col: Optional[str]
        _watermark_delay: Optional[str]

        def _validate_operation_types(
            self, col: "ColumnOperation", operation: str
        ) -> None: ...

    @overload
    def select(self: SupportsDF, *columns: str) -> SupportsDF:
        """Select columns by name."""
        ...

    @overload
    def select(self: SupportsDF, *columns: Column) -> SupportsDF:
        """Select columns using Column objects."""
        ...

    @overload
    def select(
        self: SupportsDF, *columns: Union[str, Column, Literal, Any]
    ) -> SupportsDF:
        """Select columns from the DataFrame."""
        ...

    def select(
        self: SupportsDF, *columns: Union[str, Column, Literal, Any]
    ) -> SupportsDF:
        """Select columns from the DataFrame.

        Args:
            *columns: Column names, Column objects, or expressions to select.
                     Use "*" to select all columns.
                     Can also accept a list/tuple of column names: df.select(["col1", "col2"])

        Returns:
            New DataFrame with selected columns.

        Raises:
            AnalysisException: If specified columns don't exist.

        Example:
            >>> df.select("name", "age")
            >>> df.select(["name", "age"])  # PySpark-compatible: list of column names
            >>> df.select("*")
            >>> df.select(F.col("name"), F.col("age") * 2)
        """
        if not columns:
            return self

        # PySpark compatibility: if a single list/tuple is passed, unpack it
        # This allows df.select(["col1", "col2"]) to work like df.select("col1", "col2")
        # Also supports df.select([F.col("col1"), F.col("col2")])
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):
            # Unpack list/tuple of columns, whether they're strings, Column objects, or mixed
            columns = tuple(columns[0])

        # Validate column names eagerly (even in lazy mode) to match PySpark behavior
        # But skip validation if there are pending join operations (columns might come from other DF)
        has_pending_joins = any(op[0] == "join" for op in self._operations_queue)

        if not has_pending_joins:
            for col in columns:
                if isinstance(col, str) and col != "*":
                    # Check if column exists
                    if col not in self.columns:
                        from ...core.exceptions.operation import (
                            SparkColumnNotFoundError,
                        )

                        raise SparkColumnNotFoundError(col, self.columns)
                elif isinstance(col, Column):
                    if hasattr(col, "operation"):
                        # Complex expression - validate column references
                        self._validate_expression_columns(col, "select")
                    else:
                        # Simple column reference - validate
                        if col.name not in self.columns:
                            from ...core.exceptions.operation import (
                                SparkColumnNotFoundError,
                            )

                            raise SparkColumnNotFoundError(col.name, self.columns)
                elif isinstance(col, ColumnOperation) and not (
                    hasattr(col, "operation")
                    and col.operation in ["months_between", "datediff"]
                ):
                    # Complex expression - validate column references
                    # Skip validation for function operations that will be evaluated later
                    self._validate_expression_columns(col, "select")

            # Always use lazy evaluation
            return cast("SupportsDF", self._queue_op("select", columns))  # type: ignore[redundant-cast,unused-ignore]
        # If there are pending joins, skip validation and go directly to lazy evaluation
        return cast("SupportsDF", self._queue_op("select", columns))  # type: ignore[redundant-cast,unused-ignore]

    def selectExpr(self: SupportsDF, *exprs: str) -> SupportsDF:
        """Select columns or expressions using SQL-like syntax.

        Supports:
        - Simple column names: "col"
        - Aliases: "col AS alias" or "col alias"
        - Complex SQL expressions (CASE WHEN, etc.): Uses F.expr()
        """
        from typing import Union

        # Keywords that indicate complex SQL expressions
        complex_keywords = {
            "case",
            "when",
            "then",
            "else",
            "end",
            "select",
            "from",
            "where",
            "group by",
            "order by",
            "count",
            "sum",
            "avg",
            "max",
            "min",
            "upper",
            "lower",
            "length",
            "concat",
            "round",
            "floor",
            "ceil",
            "abs",
            "substring",
            "replace",
            "trim",
            "cast",
            "as",
        }

        def is_simple_column_name(text: str) -> bool:
            """Check if text is a simple column name."""
            # Simple column names don't contain operators, keywords, or function calls
            if not text or text == "*":
                return False
            # Check for SQL operators
            operators = ["+", "-", "*", "/", "=", ">", "<", "<>", "!=", "%", "(", ")"]
            if any(op in text for op in operators):
                return False
            # Check for SQL keywords
            text_lower = text.lower()
            for keyword in complex_keywords:
                if keyword in text_lower:
                    return False
            # Check for function calls (contains parentheses)
            return "(" not in text

        columns: List[Union[str, Column, ColumnOperation]] = []
        for expr in exprs:
            text = expr.strip()
            if text == "*":
                columns.extend([f.name for f in self.schema.fields])
                continue

            # Check if this is a complex SQL expression
            text_lower = text.lower()
            has_alias = " as " in text_lower or (
                text.count(" ") == 1
                and not any(
                    is_simple_column_name(part)
                    for part in text.split()
                    if len(part) > 0
                )
            )

            if has_alias:
                # Parse alias
                alias = None
                colname = text
                if " as " in text_lower:
                    parts = text.split()
                    try:
                        idx = next(i for i, p in enumerate(parts) if p.lower() == "as")
                        colname = " ".join(parts[:idx])
                        alias = " ".join(parts[idx + 1 :])
                    except StopIteration:
                        colname = text
                else:
                    parts = text.split()
                    if len(parts) == 2:
                        colname, alias = parts[0], parts[1]

                # Check if it's a complex expression
                if is_simple_column_name(colname):
                    if alias:
                        columns.append(Column(colname).alias(alias))
                    else:
                        columns.append(Column(colname))
                else:
                    # Complex expression with alias
                    from ...functions import F

                    if alias:
                        expr_col = F.expr(colname).alias(alias)
                        columns.append(expr_col)  # type: ignore[arg-type]
                    else:
                        expr_col = F.expr(colname)
                        columns.append(expr_col)  # type: ignore[arg-type]
            else:
                # No alias
                if is_simple_column_name(text):
                    columns.append(text)
                else:
                    # Complex expression without alias
                    from ...functions import F

                    columns.append(F.expr(text))  # type: ignore[arg-type]

        return cast("SupportsDF", self.select(*columns))  # type: ignore[redundant-cast,unused-ignore]

    def filter(
        self: SupportsDF,
        condition: Union[ColumnOperation, Column, "Literal", str],
    ) -> SupportsDF:
        """Filter rows based on condition.

        Args:
            condition: Filter condition. Can be:
                - ColumnOperation or Column (e.g., df.salary > 55000)
                - String SQL expression (e.g., "salary > 55000")
                - Literal boolean value
        """
        # PySpark compatibility: if condition is a string, parse it as SQL expression
        if isinstance(condition, str):
            from ...functions import F

            condition = F.expr(condition)  # type: ignore[assignment]

        # Pre-validation: validate filter expression
        self._validate_filter_expression(condition, "filter")

        return cast("SupportsDF", self._queue_op("filter", condition))  # type: ignore[redundant-cast,unused-ignore]

    def where(
        self: SupportsDF,
        condition: Union[ColumnOperation, Column, str],
    ) -> SupportsDF:
        """Alias for filter() - Filter rows based on condition (all PySpark versions).

        Args:
            condition: Filter condition. Can be:
                - ColumnOperation or Column (e.g., df.salary > 55000)
                - String SQL expression (e.g., "salary > 55000")

        Returns:
            Filtered DataFrame
        """
        return cast("SupportsDF", self.filter(condition))  # type: ignore[redundant-cast,unused-ignore]

    def withColumn(
        self: SupportsDF,
        col_name: str,
        col: Union[Column, ColumnOperation, Literal, Any],
    ) -> SupportsDF:
        """Add or replace column."""
        # Validate column references in expressions
        if isinstance(col, Column) and not hasattr(col, "operation"):
            # Simple column reference - validate
            self._validate_column_exists(col.name, "withColumn")
        elif isinstance(col, ColumnOperation):
            # Complex expression - validate column references
            self._validate_expression_columns(col, "withColumn")
            # Validate type requirements for specific operations
            # Type check: _validate_operation_types is defined on DataFrame class
            if hasattr(self, "_validate_operation_types"):
                getattr(self, "_validate_operation_types")(col, "withColumn")
        # For Literal and other cases, skip validation

        return cast("SupportsDF", self._queue_op("withColumn", (col_name, col)))  # type: ignore[redundant-cast,unused-ignore]

    def withColumns(
        self: SupportsDF,
        colsMap: Dict[str, Union[Column, ColumnOperation, Literal, Any]],
    ) -> SupportsDF:
        """Add or replace multiple columns at once (PySpark 3.3+).

        Args:
            colsMap: Dictionary mapping column names to column expressions

        Returns:
            DataFrame with new/replaced columns
        """
        result = self
        for col_name, col_expr in colsMap.items():
            result = result.withColumn(col_name, col_expr)
        return result

    def withColumnRenamed(self: SupportsDF, existing: str, new: str) -> SupportsDF:
        """Rename a column."""
        new_data = []
        for row in self.data:
            new_row = {}
            for k, v in row.items():
                if k == existing:
                    new_row[new] = v
                else:
                    new_row[k] = v
            new_data.append(new_row)

        # Update schema
        new_fields = []
        for field in self.schema.fields:
            if field.name == existing:
                new_fields.append(StructField(new, field.dataType))
            else:
                new_fields.append(field)
        new_schema = StructType(new_fields)
        from ..dataframe import DataFrame

        return cast("SupportsDF", DataFrame(new_data, new_schema, self.storage))

    def withColumnsRenamed(self: SupportsDF, colsMap: Dict[str, str]) -> SupportsDF:
        """Rename multiple columns (PySpark 3.4+).

        Args:
            colsMap: Dictionary mapping old column names to new names

        Returns:
            DataFrame with renamed columns
        """
        result = self
        for old_name, new_name in colsMap.items():
            result = result.withColumnRenamed(old_name, new_name)
        return result

    def drop(self: SupportsDF, *cols: str) -> SupportsDF:
        """Drop columns."""
        new_data = []
        for row in self.data:
            new_row = {k: v for k, v in row.items() if k not in cols}
            new_data.append(new_row)

        # Update schema
        new_fields = [field for field in self.schema.fields if field.name not in cols]
        new_schema = StructType(new_fields)
        from ..dataframe import DataFrame

        return cast("SupportsDF", DataFrame(new_data, new_schema, self.storage))

    def distinct(self: SupportsDF) -> SupportsDF:
        """Return distinct rows."""
        # Queue distinct when there are pending operations so it runs after
        # withColumn/select during materialization (fixes all-None for computed columns)
        if self._operations_queue:
            return cast("SupportsDF", self._queue_op("distinct", ()))  # type: ignore[redundant-cast,unused-ignore]

        seen = set()
        distinct_data = []

        # Get field names in schema order
        field_names = [f.name for f in self.schema.fields]

        for row in self.data:
            # Create tuple in schema order for consistent hashing
            row_tuple = tuple(row.get(name) for name in field_names)
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)

        from ..dataframe import DataFrame

        return cast("SupportsDF", DataFrame(distinct_data, self.schema, self.storage))

    def dropDuplicates(
        self: SupportsDF, subset: Optional[List[str]] = None
    ) -> SupportsDF:
        """Drop duplicate rows."""
        if subset is None:
            return cast("SupportsDF", self.distinct())  # type: ignore[redundant-cast,unused-ignore]

        seen = set()
        distinct_data = []
        for row in self.data:
            row_tuple = tuple(sorted((k, v) for k, v in row.items() if k in subset))
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)

        from ..dataframe import DataFrame

        return cast("SupportsDF", DataFrame(distinct_data, self.schema, self.storage))

    def drop_duplicates(
        self: SupportsDF, subset: Optional[List[str]] = None
    ) -> SupportsDF:
        """Alias for dropDuplicates() (all PySpark versions).

        Args:
            subset: Optional list of column names to consider for deduplication

        Returns:
            DataFrame with duplicates removed
        """
        return cast("SupportsDF", self.dropDuplicates(subset))  # type: ignore[redundant-cast,unused-ignore]

    def orderBy(
        self: SupportsDF, *columns: Union[str, Column], ascending: bool = True
    ) -> SupportsDF:
        """Order by columns.

        Args:
            *columns: Column names or Column objects to order by
            ascending: Whether to sort in ascending order (default: True)

        Returns:
            DataFrame sorted by the specified columns
        """
        # PySpark compatibility: if a single list/tuple is passed, unpack it
        # This allows df.orderBy(["col1", "col2"]) to work like df.orderBy("col1", "col2")
        # Also supports df.orderBy(df.columns)
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):  # type: ignore[unreachable]
            # Unpack list/tuple of columns
            columns = tuple(columns[0])  # type: ignore[unreachable]
        # Pass columns and ascending as a tuple: (columns, ascending)
        return cast(
            "SupportsDF",
            self._queue_op("orderBy", (columns, ascending)),  # type: ignore[redundant-cast,unused-ignore]
        )

    def sort(
        self: SupportsDF, *columns: Union[str, Column], **kwargs: Any
    ) -> SupportsDF:
        """Alias for orderBy() - Sort DataFrame by columns (all PySpark versions).

        Args:
            *columns: Column names or Column objects to sort by
            **kwargs: Additional sort options (e.g., ascending)

        Returns:
            Sorted DataFrame
        """
        # PySpark compatibility: if a single list/tuple is passed, unpack it
        # This allows df.sort(["col1", "col2"]) to work like df.sort("col1", "col2")
        # Also supports df.sort(df.columns)
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):  # type: ignore[unreachable]
            # Unpack list/tuple of columns
            columns = tuple(columns[0])  # type: ignore[unreachable]
        ascending = kwargs.get("ascending", True)
        return cast("SupportsDF", self.orderBy(*columns, ascending=ascending))  # type: ignore[redundant-cast,unused-ignore]

    def limit(self: SupportsDF, n: int) -> SupportsDF:
        """Limit number of rows."""
        return cast("SupportsDF", self._queue_op("limit", n))  # type: ignore[redundant-cast,unused-ignore]

    def offset(self: SupportsDF, n: int) -> SupportsDF:
        """Skip first n rows (SQL OFFSET clause).

        Args:
            n: Number of rows to skip.

        Returns:
            New DataFrame with first n rows skipped.

        Example:
            >>> df.offset(5).show()  # Skip first 5 rows
        """
        if n < 0:
            from ...core.exceptions import PySparkValueError

            raise PySparkValueError(f"OFFSET must be non-negative, got {n}")
        return cast("SupportsDF", self._queue_op("offset", n))  # type: ignore[redundant-cast,unused-ignore]

    def repartition(self: SupportsDF, numPartitions: int, *cols: Any) -> SupportsDF:
        """Repartition DataFrame (no-op in mock; returns self)."""
        return self

    def coalesce(self: SupportsDF, numPartitions: int) -> SupportsDF:
        """Coalesce partitions (no-op in mock; returns self)."""
        return self

    def replace(
        self: SupportsDF,
        to_replace: Union[int, float, str, List[Any], Dict[Any, Any]],
        value: Optional[Union[int, float, str, List[Any]]] = None,
        subset: Optional[List[str]] = None,
    ) -> SupportsDF:
        """Replace values in DataFrame (all PySpark versions).

        Args:
            to_replace: Value(s) to replace - can be scalar, list, or dict
            value: Replacement value(s) - required if to_replace is not a dict
            subset: Optional list of columns to limit replacement to

        Returns:
            New DataFrame with replaced values

        Examples:
            >>> # Replace with dict mapping
            >>> df.replace({'A': 'X', 'B': 'Y'})

            >>> # Replace list of values with single value
            >>> df.replace([1, 2], 99, subset=['col1'])

            >>> # Replace single value
            >>> df.replace(1, 99)
        """
        from copy import deepcopy

        # Determine columns to apply replacement to
        target_columns = subset if subset else self.columns

        # Build replacement map
        replace_map: Dict[Any, Any] = {}
        if isinstance(to_replace, dict):
            replace_map = to_replace
        elif isinstance(to_replace, list):
            if value is None:
                raise PySparkValueError(
                    "value cannot be None when to_replace is a list"
                )
            # If value is also a list, create mapping
            if isinstance(value, list):
                if len(to_replace) != len(value):
                    raise PySparkValueError(
                        "to_replace and value lists must have same length"
                    )
                replace_map = dict(zip(to_replace, value))
            else:
                # All values in list map to single value
                replace_map = dict.fromkeys(to_replace, value)
        else:
            # Scalar to_replace
            if value is None:
                raise PySparkValueError(
                    "value cannot be None when to_replace is a scalar"
                )
            replace_map = {to_replace: value}

        # Apply replacements
        new_data = []
        for row in self.data:
            new_row = deepcopy(row)
            for col in target_columns:
                if col in new_row and new_row[col] in replace_map:
                    new_row[col] = replace_map[new_row[col]]
            new_data.append(new_row)

        from ..dataframe import DataFrame

        return cast("SupportsDF", DataFrame(new_data, self.schema, self.storage))

    def colRegex(self, colName: str) -> Column:
        """Select columns matching a regex pattern (all PySpark versions).

        The regex pattern should be wrapped in backticks: `pattern`

        Args:
            colName: Regex pattern wrapped in backticks, e.g. "`.*id`"

        Returns:
            Column expression that can be used in select()

        Example:
            >>> df = spark.createDataFrame([{"user_id": 1, "post_id": 2, "name": "Alice"}])
            >>> df.select(df.colRegex("`.*id`")).show()  # Selects user_id and post_id
        """
        import re
        from ...functions.base import Column

        # Extract pattern from backticks
        pattern = colName.strip()
        if pattern.startswith("`") and pattern.endswith("`"):
            pattern = pattern[1:-1]

        # Find matching columns (preserve order from DataFrame)
        matching_cols = [col for col in self.columns if re.match(pattern, col)]

        if not matching_cols:
            # Return empty column if no matches (PySpark behavior)
            return Column("")

        # For simplicity, we return a special marker that select() will handle
        # In real implementation, this would return a Column that expands to multiple columns
        result = Column(matching_cols[0])
        # Store the full list of matching columns as metadata
        result._regex_matches = matching_cols  # type: ignore
        return result
