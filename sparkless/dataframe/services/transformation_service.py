"""
Transformation service for DataFrame operations.

This service provides transformation operations using composition instead of mixin inheritance.
"""

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union, cast, overload

from ...functions import Column, ColumnOperation, Literal
from ...spark_types import StructType, StructField
from ...core.exceptions import PySparkValueError
from ...core.column_resolver import ColumnResolver

if TYPE_CHECKING:
    from ..dataframe import DataFrame
    from ..protocols import SupportsDataFrameOps


class TransformationService:
    """Service providing transformation operations for DataFrame."""

    def __init__(self, df: "DataFrame"):
        """Initialize transformation service with DataFrame instance."""
        self._df = df

    def _find_column(self, column_name: str) -> Optional[str]:
        """Find column name in schema using ColumnResolver.

        Args:
            column_name: Column name to find.

        Returns:
            Actual column name from schema if found, None otherwise.

        Note:
            Uses ColumnResolver to respect spark.sql.caseSensitive configuration.
        """
        case_sensitive = self._df._is_case_sensitive()
        available_columns = self._df.columns
        return ColumnResolver.resolve_column_name(
            column_name, available_columns, case_sensitive
        )

    def _validate_column_reference(self, col: Any) -> None:
        """Recursively validate column references in Column or ColumnOperation.

        Args:
            col: Column, ColumnOperation, or other object to validate.

        Raises:
            SparkColumnNotFoundError: If a column reference doesn't exist.
        """
        from ...core.exceptions.operation import SparkColumnNotFoundError

        if isinstance(col, ColumnOperation):
            # For ColumnOperation, validate underlying column references
            if hasattr(col, "column") and col.column:
                self._validate_column_reference(col.column)
            if hasattr(col, "value") and col.value and isinstance(col.value, Column):
                self._validate_column_reference(col.value)
        elif isinstance(col, Column):
            # For simple Column, validate the column name exists
            col_name = col.name if hasattr(col, "name") else str(col)
            # Skip validation for dummy columns used by special operations
            # (e.g., create_map, struct, expr use placeholder columns)
            if col_name in (
                "__expr__",
                "__struct_dummy__",
                "__create_map_base__",
                "__create_map_dummy__",
            ):
                return
            if col_name and col_name != "*":
                # Handle nested struct field access (e.g., "Person.name")
                if "." in col_name:
                    # Split into struct column and field name
                    parts = col_name.split(".", 1)
                    struct_col = parts[0]
                    # Validate the struct column exists
                    actual_struct_col = self._find_column(struct_col)
                    if actual_struct_col is None:
                        raise SparkColumnNotFoundError(struct_col, self._df.columns)
                    # For nested fields, we don't validate the field name here
                    # as it will be validated during execution
                else:
                    # Regular column validation
                    actual_col_name = self._find_column(col_name)
                    if actual_col_name is None:
                        raise SparkColumnNotFoundError(col_name, self._df.columns)

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
                     ColumnOperation expressions (like F.size(col), F.abs(col)) are validated
                     by checking underlying column references, not the expression name itself.

        Returns:
            New DataFrame with selected columns.

        Raises:
            AnalysisException: If specified columns don't exist.

        Example:
            >>> df.select("name", "age")
            >>> df.select(["name", "age"])  # PySpark-compatible: list of column names
            >>> df.select("*")
            >>> df.select(F.col("name"), F.col("age") * 2)
            >>> df.select(F.size(F.col("scores")))  # ColumnOperation expressions work

        Note:
            Fixed in version 3.23.0: Validation now correctly handles ColumnOperation
            expressions by validating underlying column references rather than treating
            the expression name as a column name.
        """
        if not columns:
            return cast("SupportsDataFrameOps", self._df)

        # PySpark compatibility: if a single list/tuple is passed, unpack it
        # This allows df.select(["col1", "col2"]) to work like df.select("col1", "col2")
        # Also supports df.select([F.col("col1"), F.col("col2")])
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):
            # Unpack list/tuple of columns, whether they're strings, Column objects, or mixed
            columns = tuple(columns[0])

        # Validate column names eagerly (even in lazy mode) to match PySpark behavior
        # But skip validation if there are pending join operations (columns might come from other DF)
        has_pending_joins = any(op[0] == "join" for op in self._df._operations_queue)

        # Always resolve column names, even with pending joins (resolution happens at materialization)
        # For pending joins, we queue the unresolved names and resolve them during materialization
        # when the actual schema (including joined columns) is available
        resolved_columns: List[Union[str, Column, ColumnOperation, Any]] = []

        if not has_pending_joins:
            # No pending joins: resolve now based on current schema
            for col in columns:
                if isinstance(col, str) and col != "*":
                    # Handle nested struct field access (e.g., "Person.name")
                    if "." in col:
                        # Split into struct column and field name
                        parts = col.split(".", 1)
                        struct_col = parts[0]
                        # Resolve the struct column name case-insensitively
                        actual_struct_col = self._find_column(struct_col)
                        if actual_struct_col is None:
                            from ...core.exceptions.operation import (
                                SparkColumnNotFoundError,
                            )

                            raise SparkColumnNotFoundError(struct_col, self._df.columns)
                        # Use the resolved struct column name + field path
                        resolved_columns.append(f"{actual_struct_col}.{parts[1]}")
                    else:
                        # Use ColumnResolver to resolve column name for validation
                        # but keep the original requested column name for output schema
                        # (PySpark behavior: requested column name is used as output name)
                        actual_col_name = self._find_column(col)
                        if actual_col_name is None:
                            from ...core.exceptions.operation import (
                                SparkColumnNotFoundError,
                            )

                            raise SparkColumnNotFoundError(col, self._df.columns)
                        # Keep the original requested column name (PySpark preserves requested case)
                        # The actual column name is resolved during materialization
                        resolved_columns.append(col)
                elif isinstance(col, Column):
                    # ColumnOperation represents an expression (like F.size(col), col + 1, etc.)
                    # It should not be validated as a column name - only the underlying column references should be validated
                    if isinstance(col, ColumnOperation):
                        # For ColumnOperation, validate underlying column references recursively
                        # but don't validate the operation name itself
                        if hasattr(col, "column") and col.column:
                            # Recursively validate the underlying column reference
                            self._validate_column_reference(col.column)
                        if hasattr(col, "value") and col.value:
                            # Also validate the value if it's a column reference
                            if isinstance(col.value, Column) and not isinstance(
                                col.value, ColumnOperation
                            ):
                                # Value is a simple column reference - validate it
                                self._validate_column_reference(col.value)
                            elif isinstance(col.value, ColumnOperation):
                                # Value is also a ColumnOperation - validate recursively
                                self._validate_column_reference(col.value)
                        # ColumnOperation expressions are valid - append as-is
                        resolved_columns.append(col)
                    elif hasattr(col, "_alias_name") and col._alias_name:
                        # This is an aliased Column - validate the underlying column reference
                        # Don't validate the alias name as it's an output column name
                        if hasattr(col, "_original_column") and col._original_column:
                            # Validate the underlying column reference
                            self._validate_column_reference(col._original_column)
                        # For aliased columns, just append them - they're output columns
                        resolved_columns.append(col)
                    else:
                        # Regular Column - validate column name
                        col_name = col.name if hasattr(col, "name") else str(col)
                        # Skip validation for dummy columns used by special operations
                        if col_name in (
                            "__expr__",
                            "__struct_dummy__",
                            "__create_map_base__",
                            "__create_map_dummy__",
                        ):
                            resolved_columns.append(col)
                            continue
                        if col_name and col_name != "*":
                            # Handle nested struct field access (e.g., "Person.name")
                            if "." in col_name:
                                # Split into struct column and field name
                                parts = col_name.split(".", 1)
                                struct_col = parts[0]
                                # Validate the struct column exists
                                actual_struct_col = self._find_column(struct_col)
                                if actual_struct_col is None:
                                    from ...core.exceptions.operation import (
                                        SparkColumnNotFoundError,
                                    )

                                    raise SparkColumnNotFoundError(
                                        struct_col, self._df.columns
                                    )
                                # For nested fields, keep the original Column object as-is
                                # The validation will be done during execution
                                resolved_columns.append(col)
                            else:
                                # Use case-insensitive lookup to check if column exists
                                actual_col_name = self._find_column(col_name)
                                if actual_col_name is None:
                                    from ...core.exceptions.operation import (
                                        SparkColumnNotFoundError,
                                    )

                                    raise SparkColumnNotFoundError(
                                        col_name, self._df.columns
                                    )
                                # For Column objects, we just validate that the column exists
                                # The Column object itself can be used as-is (it references the correct column)
                                # But if the name needs case correction, we need to create a new Column
                                if actual_col_name != col_name:
                                    # Column name needs case correction - create new Column with correct name
                                    resolved_col = Column(
                                        actual_col_name,
                                        col.column_type
                                        if hasattr(col, "column_type")
                                        else None,
                                    )
                                    resolved_columns.append(resolved_col)
                                else:
                                    resolved_columns.append(col)
                        else:
                            resolved_columns.append(col)
                else:
                    resolved_columns.append(col)
            columns = tuple(resolved_columns)
            # Always use lazy evaluation
            return self._df._queue_op("select", columns)

        # If there are pending joins, queue unresolved column names
        # They will be resolved during materialization when the actual schema is known
        if has_pending_joins:
            # Still try to resolve if possible, but preserve requested column name (PySpark behavior)
            # The actual column lookup happens during materialization, but we keep the requested name
            for col in columns:
                if isinstance(col, str) and col != "*":
                    # Validate column exists, but preserve requested column name for output
                    actual_col_name = self._find_column(col)
                    if actual_col_name is not None:
                        # Column exists - keep requested name (will be resolved during materialization)
                        resolved_columns.append(col)
                    else:
                        # Column not found in current schema - might be from joined DataFrame
                        # Keep original name, will be resolved during materialization
                        resolved_columns.append(col)
                else:
                    resolved_columns.append(col)
            columns = tuple(resolved_columns)

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

        columns: List[Union[str, Column, ColumnOperation]] = []
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
                    # Resolve column name before creating Column object
                    resolved_colname = self._find_column(colname)
                    if resolved_colname is None:
                        from ...core.exceptions.operation import (
                            SparkColumnNotFoundError,
                        )

                        raise SparkColumnNotFoundError(colname, self._df.columns)
                    if alias:
                        columns.append(Column(resolved_colname).alias(alias))
                    else:
                        columns.append(Column(resolved_colname))
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
                    # Resolve column name for simple column references
                    resolved_text = self._find_column(text)
                    if resolved_text is None:
                        from ...core.exceptions.operation import (
                            SparkColumnNotFoundError,
                        )

                        raise SparkColumnNotFoundError(text, self._df.columns)
                    columns.append(resolved_text)
                else:
                    # Complex expression without alias
                    from ...functions import F

                    columns.append(F.expr(text))  # type: ignore[arg-type]

        return self.select(*columns)

    def filter(
        self,
        condition: Union[ColumnOperation, Column, "Literal", str],
    ) -> "SupportsDataFrameOps":
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
        self._df._validate_filter_expression(condition, "filter")

        return self._df._queue_op("filter", condition)

    def where(
        self,
        condition: Union[ColumnOperation, Column, str],
    ) -> "SupportsDataFrameOps":
        """Alias for filter() - Filter rows based on condition (all PySpark versions).

        Args:
            condition: Filter condition. Can be:
                - ColumnOperation or Column (e.g., df.salary > 55000)
                - String SQL expression (e.g., "salary > 55000")

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
        colsMap: Dict[str, Union[Column, ColumnOperation, Literal, Any]],
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
        """Rename a column.

        If the column does not exist, this is treated as a no-op and the DataFrame
        is returned unchanged (matching PySpark behavior).
        """
        # Materialize if lazy to ensure we work with actual data including all columns
        materialized = self._df._materialize_if_lazy()

        # Resolve column name case-insensitively
        actual_existing = self._find_column(existing)
        if actual_existing is None:
            # PySpark treats renaming a non-existent column as a no-op
            # Return the DataFrame unchanged
            return materialized

        new_data = []
        for row in materialized.data:
            new_row = {}
            for k, v in row.items():
                if k == actual_existing:
                    new_row[new] = v
                else:
                    new_row[k] = v
            new_data.append(new_row)

        # Update schema
        new_fields = []
        for field in materialized.schema.fields:
            if field.name == actual_existing:
                new_fields.append(StructField(new, field.dataType))
            else:
                new_fields.append(field)
        new_schema = StructType(new_fields)
        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps", DataFrame(new_data, new_schema, self._df.storage)
        )

    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> "SupportsDataFrameOps":
        """Rename multiple columns (PySpark 3.4+).

        Args:
            colsMap: Dictionary mapping old column names to new names

        Returns:
            DataFrame with renamed columns
        """
        # Materialize if lazy to ensure we work with actual data
        materialized = self._df._materialize_if_lazy()

        # Build case-insensitive mapping from actual column names to new names
        # Skip non-existent columns (matching PySpark behavior - no-op for missing columns)
        actual_to_new: Dict[str, str] = {}
        for old_name, new_name in colsMap.items():
            actual_old = self._find_column(old_name)
            if actual_old is not None:
                actual_to_new[actual_old] = new_name

        # Apply all renames in a single pass
        new_data = []
        for row in materialized.data:
            new_row = {}
            for k, v in row.items():
                # Check if this key should be renamed (case-insensitive)
                new_key = actual_to_new.get(k, k)
                new_row[new_key] = v
            new_data.append(new_row)

        # Update schema
        new_fields = []
        for field in materialized.schema.fields:
            new_name = actual_to_new.get(field.name, field.name)
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
        # Queue distinct when there are pending operations so it runs after
        # withColumn/select during materialization (fixes all-None for computed columns)
        if self._df._operations_queue:
            return self._df._queue_op("distinct", ())

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
        self, subset: Optional[List[str]] = None
    ) -> "SupportsDataFrameOps":
        """Drop duplicate rows."""
        if subset is None:
            return self.distinct()

        # Resolve column names case-insensitively
        actual_subset = []
        for col_name in subset:
            actual_col = self._find_column(col_name)
            if actual_col is None:
                from ...core.exceptions.operation import SparkColumnNotFoundError

                raise SparkColumnNotFoundError(col_name, self._df.columns)
            actual_subset.append(actual_col)

        seen = set()
        distinct_data = []
        for row in self._df.data:
            row_tuple = tuple(
                sorted((k, v) for k, v in row.items() if k in actual_subset)
            )
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)

        from ..dataframe import DataFrame

        return cast(
            "SupportsDataFrameOps",
            DataFrame(distinct_data, self._df.schema, self._df.storage),
        )

    def drop_duplicates(
        self, subset: Optional[List[str]] = None
    ) -> "SupportsDataFrameOps":
        """Alias for dropDuplicates() (all PySpark versions).

        Args:
            subset: Optional list of column names to consider for deduplication

        Returns:
            DataFrame with duplicates removed
        """
        return self.dropDuplicates(subset)

    def orderBy(
        self, *columns: Union[str, Column], ascending: bool = True
    ) -> "SupportsDataFrameOps":
        """Order by columns.

        Args:
            *columns: Column names or Column objects to order by
            ascending: Whether to sort in ascending order (default: True)

        Returns:
            DataFrame sorted by the specified columns
        """
        # Pass columns and ascending as a tuple: (columns, ascending)
        return self._df._queue_op("orderBy", (columns, ascending))

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
        # PySpark compatibility: if a single list/tuple is passed, unpack it
        # This allows df.sort(["col1", "col2"]) to work like df.sort("col1", "col2")
        # Also supports df.sort(df.columns)
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):  # type: ignore[unreachable]
            # Unpack list/tuple of columns
            columns = tuple(columns[0])  # type: ignore[unreachable]
        ascending = kwargs.get("ascending", True)
        return self.orderBy(*columns, ascending=ascending)

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
        to_replace: Union[int, float, str, List[Any], Dict[Any, Any]],
        value: Optional[Union[int, float, str, List[Any]]] = None,
        subset: Optional[List[str]] = None,
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

        # Determine columns to apply replacement to and resolve case-insensitively
        if subset:
            from ...core.column_resolver import ColumnResolver

            available_cols = self._df.columns
            case_sensitive = self._df._is_case_sensitive()
            target_columns = []
            for col in subset:
                resolved_col = ColumnResolver.resolve_column_name(
                    col, available_cols, case_sensitive
                )
                if resolved_col is None:
                    from ...core.exceptions.analysis import ColumnNotFoundException

                    raise ColumnNotFoundException(col)
                target_columns.append(resolved_col)
        else:
            target_columns = self._df.columns

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
