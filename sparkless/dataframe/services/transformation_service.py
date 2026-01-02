"""
Transformation service for DataFrame operations.

This service provides transformation operations using composition instead of mixin inheritance.
"""

from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from ...functions import Column, ColumnOperation, Literal
from ...spark_types import StructType, StructField
from ...core.exceptions import PySparkValueError

if TYPE_CHECKING:
    from ..dataframe import DataFrame
    from ..protocols import SupportsDataFrameOps


class TransformationService:
    """Service providing transformation operations for DataFrame."""

    def __init__(self, df: "DataFrame"):
        """Initialize transformation service with DataFrame instance."""
        self._df = df

    @overload
    def select(self, *columns: str) -> "SupportsDataFrameOps":
        """Select columns by name."""
        ...

    @overload
    def select(self, *columns: Column) -> "SupportsDataFrameOps":
        """Select columns using Column objects."""
        ...

    @overload
    def select(
        self, *columns: Union[str, Column, Literal, Any]
    ) -> "SupportsDataFrameOps":
        """Select columns from the DataFrame."""
        ...

    def select(
        self, *columns: Union[str, Column, Literal, Any]
    ) -> "SupportsDataFrameOps":
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
            return cast("SupportsDataFrameOps", self._df)

        # PySpark compatibility: if a single list/tuple is passed, unpack it
        # This allows df.select(["col1", "col2"]) to work like df.select("col1", "col2")
        if (
            len(columns) == 1
            and isinstance(columns[0], (list, tuple))
            and all(isinstance(col, str) for col in columns[0])
        ):
            # Only unpack if all elements are strings (column names)
            # If it contains Column objects or other types, treat as single column
            columns = tuple(columns[0])

        # Validate column names eagerly (even in lazy mode) to match PySpark behavior
        # But skip validation if there are pending join operations (columns might come from other DF)
        has_pending_joins = any(op[0] == "join" for op in self._df._operations_queue)

        if not has_pending_joins:
            for col in columns:
                if isinstance(col, str) and col != "*":
                    # Check if column exists
                    if col not in self._df.columns:
                        from ...core.exceptions.operation import (
                            SparkColumnNotFoundError,
                        )

                        raise SparkColumnNotFoundError(col, self._df.columns)
                elif isinstance(col, Column):
                    if hasattr(col, "operation"):
                        # Complex expression - validate column references
                        self._df._validate_expression_columns(col, "select")
                    else:
                        # Simple column reference - validate
                        if col.name not in self._df.columns:
                            from ...core.exceptions.operation import (
                                SparkColumnNotFoundError,
                            )

                            raise SparkColumnNotFoundError(col.name, self._df.columns)
                elif isinstance(col, ColumnOperation) and not (
                    hasattr(col, "operation")
                    and col.operation in ["months_between", "datediff"]
                ):
                    # Complex expression - validate column references
                    # Skip validation for function operations that will be evaluated later
                    self._df._validate_expression_columns(col, "select")

            # Always use lazy evaluation
            return self._df._queue_op("select", columns)

        # If there are pending joins, skip validation and go directly to lazy evaluation
        return self._df._queue_op("select", columns)

    def selectExpr(self, *exprs: str) -> "SupportsDataFrameOps":
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

        columns: list[Union[str, Column, ColumnOperation]] = []
        for expr in exprs:
            text = expr.strip()
            if text == "*":
                columns.extend([f.name for f in self._df.schema.fields])
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

        return self.select(*columns)

    def filter(
        self,
        condition: Union[ColumnOperation, Column, "Literal"],
    ) -> "SupportsDataFrameOps":
        """Filter rows based on condition."""
        # Pre-validation: validate filter expression
        self._df._validate_filter_expression(condition, "filter")

        return self._df._queue_op("filter", condition)

    def where(
        self,
        condition: Union[ColumnOperation, Column],
    ) -> "SupportsDataFrameOps":
        """Alias for filter() - Filter rows based on condition (all PySpark versions).

        Args:
            condition: Boolean condition to filter rows

        Returns:
            Filtered DataFrame
        """
        return self.filter(condition)

    def withColumn(
        self,
        col_name: str,
        col: Union[Column, ColumnOperation, Literal, Any],
    ) -> "SupportsDataFrameOps":
        """Add or replace column."""
        # Validate column references in expressions
        if isinstance(col, Column) and not hasattr(col, "operation"):
            # Simple column reference - validate
            self._df._validate_column_exists(col.name, "withColumn")
        elif isinstance(col, ColumnOperation):
            # Complex expression - validate column references
            self._df._validate_expression_columns(col, "withColumn")
            # Validate type requirements for specific operations
            self._df._validate_operation_types(col, "withColumn")
        # For Literal and other cases, skip validation

        return self._df._queue_op("withColumn", (col_name, col))

    def withColumns(
        self,
        colsMap: dict[str, Union[Column, ColumnOperation, Literal, Any]],
    ) -> "SupportsDataFrameOps":
        """Add or replace multiple columns at once (PySpark 3.3+).

        Args:
            colsMap: Dictionary mapping column names to column expressions

        Returns:
            DataFrame with new/replaced columns
        """
        # Materialize if lazy to ensure we work with actual data
        materialized = self._df._materialize_if_lazy()

        # Apply all column transformations by chaining through DataFrame instances
        result_df: DataFrame = materialized  # type: ignore[assignment]
        for col_name, col_expr in colsMap.items():
            # Use the result DataFrame's transformation service for the next operation
            result_df = result_df._transformations.withColumn(col_name, col_expr)  # type: ignore[assignment]
            # Materialize to ensure we have a concrete DataFrame for the next iteration
            result_df = result_df._materialize_if_lazy()  # type: ignore[assignment]

        return cast("SupportsDataFrameOps", result_df)

    def withColumnRenamed(self, existing: str, new: str) -> "SupportsDataFrameOps":
        """Rename a column."""
        # Materialize if lazy to ensure we work with actual data including all columns
        materialized = self._df._materialize_if_lazy()

        new_data = []
        for row in materialized.data:
            new_row = {}
            for k, v in row.items():
                if k == existing:
                    new_row[new] = v
                else:
                    new_row[k] = v
            new_data.append(new_row)

        # Update schema
        new_fields = []
        for field in materialized.schema.fields:
            if field.name == existing:
                new_fields.append(StructField(new, field.dataType))
            else:
                new_fields.append(field)
        new_schema = StructType(new_fields)
        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps", DataFrame(new_data, new_schema, self._df.storage)
        )

    def withColumnsRenamed(self, colsMap: dict[str, str]) -> "SupportsDataFrameOps":
        """Rename multiple columns (PySpark 3.4+).

        Args:
            colsMap: Dictionary mapping old column names to new names

        Returns:
            DataFrame with renamed columns
        """
        # Materialize if lazy to ensure we work with actual data
        materialized = self._df._materialize_if_lazy()

        # Apply all renames in a single pass
        new_data = []
        for row in materialized.data:
            new_row = {}
            for k, v in row.items():
                # Check if this key should be renamed
                new_key = colsMap.get(k, k)
                new_row[new_key] = v
            new_data.append(new_row)

        # Update schema
        new_fields = []
        for field in materialized.schema.fields:
            new_name = colsMap.get(field.name, field.name)
            new_fields.append(StructField(new_name, field.dataType, field.nullable))
        new_schema = StructType(new_fields)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(new_data, new_schema, materialized.storage),
        )

    def drop(self, *cols: str) -> "SupportsDataFrameOps":
        """Drop columns."""
        # Always use lazy evaluation to queue the operation
        return self._df._queue_op(
            "drop", cols if len(cols) > 1 else (cols[0] if cols else ())
        )

    def distinct(self) -> "SupportsDataFrameOps":
        """Return distinct rows."""
        seen = set()
        distinct_data = []

        # Get field names in schema order
        field_names = [f.name for f in self._df.schema.fields]

        for row in self._df.data:
            # Create tuple in schema order for consistent hashing
            row_tuple = tuple(row.get(name) for name in field_names)
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(distinct_data, self._df.schema, self._df.storage),
        )

    def dropDuplicates(
        self, subset: Optional[list[str]] = None
    ) -> "SupportsDataFrameOps":
        """Drop duplicate rows."""
        if subset is None:
            return self.distinct()

        seen = set()
        distinct_data = []
        for row in self._df.data:
            row_tuple = tuple(sorted((k, v) for k, v in row.items() if k in subset))
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(distinct_data, self._df.schema, self._df.storage),
        )

    def drop_duplicates(
        self, subset: Optional[list[str]] = None
    ) -> "SupportsDataFrameOps":
        """Alias for dropDuplicates() (all PySpark versions).

        Args:
            subset: Optional list of column names to consider for deduplication

        Returns:
            DataFrame with duplicates removed
        """
        return self.dropDuplicates(subset)

    def orderBy(self, *columns: Union[str, Column]) -> "SupportsDataFrameOps":
        """Order by columns."""
        return self._df._queue_op("orderBy", columns)

    def sort(
        self, *columns: Union[str, Column], **kwargs: Any
    ) -> "SupportsDataFrameOps":
        """Alias for orderBy() - Sort DataFrame by columns (all PySpark versions).

        Args:
            *columns: Column names or Column objects to sort by
            **kwargs: Additional sort options (e.g., ascending)

        Returns:
            Sorted DataFrame
        """
        return self.orderBy(*columns)

    def limit(self, n: int) -> "SupportsDataFrameOps":
        """Limit number of rows."""
        return self._df._queue_op("limit", n)

    def offset(self, n: int) -> "SupportsDataFrameOps":
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
        return self._df._queue_op("offset", n)

    def repartition(self, numPartitions: int, *cols: Any) -> "SupportsDataFrameOps":
        """Repartition DataFrame (no-op in mock; returns self)."""
        return cast("SupportsDataFrameOps", self._df)

    def coalesce(self, numPartitions: int) -> "SupportsDataFrameOps":
        """Coalesce partitions (no-op in mock; returns self)."""
        return cast("SupportsDataFrameOps", self._df)

    def replace(
        self,
        to_replace: Union[int, float, str, list[Any], dict[Any, Any]],
        value: Optional[Union[int, float, str, list[Any]]] = None,
        subset: Optional[list[str]] = None,
    ) -> "SupportsDataFrameOps":
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
        target_columns = subset if subset else self._df.columns

        # Build replacement map
        replace_map: dict[Any, Any] = {}
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
        for row in self._df.data:
            new_row = deepcopy(row)
            for col in target_columns:
                if col in new_row and new_row[col] in replace_map:
                    new_row[col] = replace_map[new_row[col]]
            new_data.append(new_row)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(new_data, self._df.schema, self._df.storage),
        )

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
        matching_cols = [col for col in self._df.columns if re.match(pattern, col)]

        if not matching_cols:
            # Return empty column if no matches (PySpark behavior)
            return Column("")

        # For simplicity, we return a special marker that select() will handle
        # In real implementation, this would return a Column that expands to multiple columns
        result = Column(matching_cols[0])
        # Store the full list of matching columns as metadata
        result._regex_matches = matching_cols  # type: ignore
        return result
