"""
SQL Executor for Sparkless.

This module provides SQL execution functionality for Sparkless,
executing parsed SQL queries and returning appropriate results.
It handles different types of SQL operations and integrates with
the storage and DataFrame systems.

Key Features:
    - SQL query execution and result generation
    - Integration with DataFrame operations
    - Support for DDL and DML operations
    - Error handling and validation
    - Result set formatting

Example:
    >>> from sparkless.session.sql import SQLExecutor
    >>> executor = SQLExecutor(session)
    >>> result = executor.execute("SELECT * FROM users WHERE age > 18")
    >>> result.show()
"""

import contextlib
import re
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING, Union, cast
from ...core.exceptions.execution import QueryExecutionException
from ...core.interfaces.dataframe import IDataFrame
from ...core.interfaces.session import ISession
from ...dataframe import DataFrame
from ...spark_types import StructType
from .parser import SQLAST

# Import types for runtime use (needed for type annotations and cast() calls)
from ...functions.core.column import ColumnOperation  # noqa: F401, TC001
from ...functions.base import AggregateFunction  # noqa: F401, TC001
from ...functions.conditional import CaseWhen  # noqa: F401, TC001
from ...functions.core.literals import Literal  # noqa: F401, TC001

if TYPE_CHECKING:
    from ...dataframe.protocols import SupportsDataFrameOps
    from ...functions import Column
    from ...functions.core.interfaces import IColumn


class SQLExecutor:
    """SQL Executor for Sparkless.

    Provides SQL execution functionality that processes parsed SQL queries
    and returns appropriate results. Handles different types of SQL operations
    including SELECT, INSERT, CREATE, DROP, and other DDL/DML operations.

    Attributes:
        session: Sparkless session instance.
        parser: SQL parser instance.

    Example:
        >>> executor = SQLExecutor(session)
        >>> result = executor.execute("SELECT name, age FROM users")
        >>> result.show()
    """

    @staticmethod
    def _normalize_column_item(
        col_item: Union[str, Dict[str, Any]],
    ) -> Tuple[str, Optional[str]]:
        """Normalize column item to (expression, alias) tuple.

        Handles both old format (string) and new format (dict with expression/alias).

        Args:
            col_item: Column item (string or dict)

        Returns:
            Tuple of (column_expression, alias_name)
        """
        if isinstance(col_item, dict):
            # New format: {"expression": "col", "alias": "alias_name"}
            col_expr = col_item.get("expression", "")
            alias = col_item.get("alias")
            return (col_expr.strip(), alias)
        else:
            # Old format: string like "col" or "col AS alias"
            col_expr = str(col_item).strip()
            alias = None
            # Extract alias if present
            alias_match = re.search(r"\s+[Aa][Ss]\s+(\w+)$", col_expr)
            if alias_match:
                alias = alias_match.group(1)
                col_expr = re.sub(r"\s+[Aa][Ss]\s+\w+$", "", col_expr).strip()
            return (col_expr, alias)

    def __init__(self, session: ISession):
        """Initialize SQLExecutor.

        Args:
            session: Sparkless session instance.
        """
        self.session = session
        from .parser import SQLParser

        self.parser = SQLParser()

    def execute(self, query: str) -> IDataFrame:
        """Execute SQL query.

        Args:
            query: SQL query string.

        Returns:
            DataFrame with query results.

        Raises:
            QueryExecutionException: If query execution fails.
        """
        try:
            # Parse the query
            ast = self.parser.parse(query)

            # Execute based on query type
            if ast.query_type == "SELECT":
                return self._execute_select(ast)
            elif ast.query_type == "UNION":
                return self._execute_union(ast)
            elif ast.query_type == "CREATE":
                return self._execute_create(ast)
            elif ast.query_type == "DROP":
                return self._execute_drop(ast)
            elif ast.query_type == "MERGE":
                return self._execute_merge(ast)
            elif ast.query_type == "INSERT":
                return self._execute_insert(ast)
            elif ast.query_type == "UPDATE":
                return self._execute_update(ast)
            elif ast.query_type == "DELETE":
                return self._execute_delete(ast)
            elif ast.query_type == "SHOW":
                return self._execute_show(ast)
            elif ast.query_type == "DESCRIBE":
                return self._execute_describe(ast)
            elif ast.query_type == "REFRESH":
                return self._execute_refresh(ast)
            else:
                raise QueryExecutionException(
                    f"Unsupported query type: {ast.query_type}"
                )

        except Exception as e:
            if isinstance(e, QueryExecutionException):
                raise
            raise QueryExecutionException(f"Failed to execute query: {str(e)}")

    def _execute_select(self, ast: SQLAST) -> IDataFrame:
        """Execute SELECT query.

        Notes (BUG-021 - schema parity):
            Basic SELECT queries must preserve a PySpark-compatible schema:
            - ``SELECT * FROM table`` returns the full table schema.
            - ``SELECT id, name, age FROM table`` projects exactly those
              columns, in order.
            This behavior is covered by:
            - ``tests/unit/session/test_sql_basic_select_schema.py``
            - ``tests/parity/sql/test_queries.py::TestSQLQueriesParity``.

        Args:
            ast: Parsed SQL AST.

        Returns:
            DataFrame with SELECT results.
        """
        components = ast.components

        # Get table name - handle queries without FROM clause
        from_tables = components.get("from_tables", [])
        if not from_tables:
            # Query without FROM clause (e.g., SELECT 1 as test_col)
            # Create a single row DataFrame with the literal values
            # DataFrame is already imported at module level
            from ...spark_types import (
                StructType,
            )

            # For now, create a simple DataFrame with one row
            # This is a basic implementation for literal SELECT queries
            data: List[Dict[str, Any]] = [
                {}
            ]  # Empty row, we'll populate based on SELECT columns
            schema = StructType([])
            df_literal = DataFrame(data, schema)
            df = df_literal
        else:
            # Check for JOINs
            joins = components.get("joins", [])
            table_aliases = components.get("table_aliases", {})

            if joins:
                # Handle JOIN operation
                table_name = from_tables[0]
                try:
                    df1_any = self.session.table(table_name)
                    df1: DataFrame
                    if not isinstance(df1_any, DataFrame):  # type: ignore[unreachable]
                        from ...spark_types import StructType

                        schema = (
                            StructType(df1_any.schema.fields)  # type: ignore[arg-type]
                            if hasattr(df1_any.schema, "fields")
                            else StructType([])
                        )
                        # DataFrame is imported at module level
                        df1 = DataFrame(df1_any.collect(), schema)
                    else:
                        df1 = df1_any  # type: ignore[unreachable]

                    # Get second table
                    join_info = joins[0]
                    table2_name = join_info["table"]
                    df2_any = self.session.table(table2_name)
                    df2: DataFrame
                    if not isinstance(df2_any, DataFrame):  # type: ignore[unreachable]
                        from ...spark_types import StructType

                        schema = (
                            StructType(df2_any.schema.fields)  # type: ignore[arg-type]
                            if hasattr(df2_any.schema, "fields")
                            else StructType([])
                        )
                        df2 = DataFrame(df2_any.collect(), schema)
                    else:
                        df2 = df2_any  # type: ignore[unreachable]

                    # Parse join condition (e.g., "e.dept_id = d.id")
                    join_condition = join_info.get("condition", "")
                    if join_condition:
                        # Extract column names from join condition
                        # Pattern: alias1.col1 = alias2.col2
                        match = re.search(
                            r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", join_condition
                        )
                        if match:
                            alias1, col1, alias2, col2 = match.groups()
                            # Determine which table each alias refers to
                            # table_aliases maps table_name -> alias
                            table1_alias = table_aliases.get(table_name, table_name)
                            table2_alias = table_aliases.get(table2_name, table2_name)

                            # Map each alias to its corresponding DataFrame and column
                            df1_col = None
                            df2_col = None

                            # Check if alias1 refers to table1 (df1)
                            if alias1 in (table1_alias, table_name):
                                df1_col = col1
                            # Check if alias1 refers to table2 (df2)
                            elif alias1 in (table2_alias, table2_name):
                                df2_col = col1

                            # Check if alias2 refers to table1 (df1)
                            if alias2 in (table1_alias, table_name):
                                df1_col = col2
                            # Check if alias2 refers to table2 (df2)
                            elif alias2 in (table2_alias, table2_name):
                                df2_col = col2

                            if df1_col and df2_col:
                                # Rename columns to avoid conflicts (prefix with table alias)
                                # This allows us to distinguish e.name from d.name after the join
                                df1_renamed = df1
                                df2_renamed = df2

                                # Rename all columns in df1 with table1 alias prefix
                                for col in df1.columns:
                                    df1_renamed = cast(
                                        "DataFrame",
                                        df1_renamed.withColumnRenamed(
                                            col, f"{table1_alias}_{col}"
                                        ),
                                    )

                                # Rename all columns in df2 with table2 alias prefix
                                for col in df2.columns:
                                    df2_renamed = cast(
                                        "DataFrame",
                                        df2_renamed.withColumnRenamed(
                                            col, f"{table2_alias}_{col}"
                                        ),
                                    )

                                # Join column references use the renamed columns
                                df1_join_col = f"{table1_alias}_{df1_col}"
                                df2_join_col = f"{table2_alias}_{df2_col}"

                                # Perform join with renamed columns
                                # Get join type from join_info (default to "inner" if not specified)
                                join_type = join_info.get("type", "inner")

                                # Materialize the renamed DataFrames to ensure columns exist
                                # before joining (renames are lazy operations)
                                df1_renamed = cast(
                                    "DataFrame", df1_renamed._materialize_if_lazy()
                                )
                                df2_renamed = cast(
                                    "DataFrame", df2_renamed._materialize_if_lazy()
                                )

                                # Verify the join columns exist in the renamed DataFrames
                                if df1_join_col not in df1_renamed.columns:
                                    raise ValueError(
                                        f"Join column '{df1_join_col}' not found in left DataFrame. "
                                        f"Available columns: {df1_renamed.columns}"
                                    )
                                if df2_join_col not in df2_renamed.columns:
                                    raise ValueError(
                                        f"Join column '{df2_join_col}' not found in right DataFrame. "
                                        f"Available columns: {df2_renamed.columns}"
                                    )

                                # Create join condition using ColumnOperation
                                # We must use ColumnOperation for different column names on left/right
                                join_col = (
                                    df1_renamed[df1_join_col]
                                    == df2_renamed[df2_join_col]
                                )
                                # join_col is a ColumnOperation (boolean expression)

                                # The column names in the ColumnOperation should match the
                                # renamed columns in the materialized DataFrames
                                df = cast(
                                    "DataFrame",
                                    df1_renamed.join(
                                        cast("SupportsDataFrameOps", df2_renamed),
                                        cast("ColumnOperation", join_col),
                                        join_type,
                                    ),
                                )
                            else:
                                # Fallback: try direct column names (assume col1 is from df1, col2 from df2)
                                join_type = join_info.get("type", "inner")
                                if col1 in df1.columns and col2 in df2.columns:
                                    join_col = df1[col1] == df2[col2]

                                    df = cast(
                                        "DataFrame",
                                        df1.join(
                                            cast("SupportsDataFrameOps", df2),
                                            cast("ColumnOperation", join_col),
                                            join_type,
                                        ),
                                    )
                                elif col2 in df1.columns and col1 in df2.columns:
                                    # Try reverse mapping
                                    join_col = df1[col2] == df2[col1]

                                    df = cast(
                                        "DataFrame",
                                        df1.join(
                                            cast("SupportsDataFrameOps", df2),
                                            cast("ColumnOperation", join_col),
                                            join_type,
                                        ),
                                    )
                                else:
                                    # Last resort: cross join
                                    df = cast(
                                        "DataFrame",
                                        df1.crossJoin(
                                            cast("SupportsDataFrameOps", df2)
                                        ),
                                    )
                        else:
                            # Fallback: try to join on common column names
                            join_type = join_info.get("type", "inner")
                            common_cols = set(df1.columns) & set(df2.columns)
                            if common_cols:
                                join_col_name = list(common_cols)[0]
                                join_condition = (
                                    df1[join_col_name] == df2[join_col_name]
                                )

                                df = cast(
                                    "DataFrame",
                                    df1.join(
                                        cast("SupportsDataFrameOps", df2),
                                        cast("ColumnOperation", join_condition),
                                        join_type,
                                    ),
                                )
                            else:
                                # Cast df2 to SupportsDataFrameOps to satisfy type checker
                                # DataFrame implements the protocol at runtime
                                df = cast(
                                    "DataFrame",
                                    df1.crossJoin(cast("SupportsDataFrameOps", df2)),
                                )
                    else:
                        # No condition - cross join
                        # Cast df2 to SupportsDataFrameOps to satisfy type checker
                        df = cast(
                            "DataFrame",
                            df1.crossJoin(cast("SupportsDataFrameOps", df2)),
                        )
                except Exception as e:
                    # Re-raise with more context to debug the issue
                    raise RuntimeError(f"Join execution failed: {e}") from e
            else:
                # Single table (no JOIN)
                table_name = from_tables[0]
                # Try to get table as DataFrame
                try:
                    df_any = self.session.table(table_name)
                    # Convert IDataFrame to DataFrame if needed
                    # DataFrame is already imported at module level

                    if isinstance(df_any, DataFrame):  # type: ignore[unreachable]
                        df = df_any  # type: ignore[unreachable]
                    else:
                        # df_any may be an IDataFrame; construct DataFrame from its public API
                        from ...spark_types import StructType

                        # Convert ISchema to StructType if needed
                        if hasattr(df_any.schema, "fields"):
                            schema = StructType(df_any.schema.fields)  # type: ignore[arg-type]
                        else:
                            schema = StructType([])
                        df = DataFrame(df_any.collect(), schema)
                except Exception:
                    # If table doesn't exist, return empty DataFrame
                    # DataFrame is already imported at module level
                    from ...spark_types import StructType

                    # Log the error for debugging (can be removed in production)
                    # This helps identify why table lookup fails
                    return DataFrame([], StructType([]))  # type: ignore[return-value]

        df_ops = cast("SupportsDataFrameOps", df)

        # Import F for WHERE clause filtering
        from ...functions import F

        # Apply WHERE conditions (before GROUP BY)
        where_conditions = components.get("where_conditions", [])
        if where_conditions:
            # Parse simple WHERE conditions like "column > value", "column < value", etc.
            where_condition = where_conditions[0]

            # Handle subqueries in WHERE conditions (e.g., "column > (SELECT AVG(col) FROM table)").
            # This logic was introduced as part of BUG-008 (SQL subqueries support) and is
            # exercised by:
            #   - tests/parity/sql/test_advanced.py::TestSQLAdvancedParity::test_sql_with_subquery
            # It works by extracting nested SELECT subqueries, executing them recursively via
            # _execute_select, and replacing them with their scalar results before applying
            # the remaining WHERE filters.
            from typing import Optional

            def extract_subquery(text: str) -> Optional[str]:
                """Extract first subquery from text, handling nested parentheses."""
                start = text.find("(SELECT")
                if start == -1:
                    return None

                paren_count = 0
                i = start
                while i < len(text):
                    if text[i] == "(":
                        paren_count += 1
                    elif text[i] == ")":
                        paren_count -= 1
                        if paren_count == 0:
                            return text[start : i + 1]
                    i += 1
                return None

            # Extract and process all subqueries
            subquery = extract_subquery(where_condition)
            while subquery:
                # Remove parentheses
                subquery_text = subquery[1:-1].strip()

                # Execute the subquery
                subquery_ast = self.parser.parse(subquery_text)
                subquery_result = self._execute_select(subquery_ast)

                # Get scalar value from subquery result (assume single row, single column)
                if subquery_result.count() > 0:
                    row = subquery_result.collect()[0]
                    # Get first column value
                    subquery_col_name = subquery_result.columns[0]
                    scalar_value = row[subquery_col_name]

                    # Replace subquery with scalar value in WHERE condition
                    where_condition = where_condition.replace(
                        subquery, str(scalar_value)
                    )

                # Look for next subquery
                subquery = extract_subquery(where_condition)

            # Get case sensitivity setting
            case_sensitive = False
            if hasattr(self.session, "conf"):
                case_sensitive = (
                    self.session.conf.get("spark.sql.caseSensitive", "false") == "true"
                )

            from ...core.column_resolver import ColumnResolver

            # Try to parse different WHERE condition types
            # Check for LIKE clause: column LIKE 'pattern'
            like_match = re.search(
                r"(\w+)\s+LIKE\s+['\"]([^'\"]+)['\"]", where_condition, re.IGNORECASE
            )
            if like_match:
                col_name = like_match.group(1)
                pattern = like_match.group(2)
                resolved_col = ColumnResolver.resolve_column_name(
                    col_name, df.columns, case_sensitive
                )
                if resolved_col:
                    df = cast(
                        "DataFrame", df_ops.filter(F.col(resolved_col).like(pattern))
                    )
                    df_ops = cast("SupportsDataFrameOps", df)
            # Check for string equality: column = 'value'
            elif re.search(r"(\w+)\s*=\s*['\"]", where_condition, re.IGNORECASE):
                eq_match = re.search(
                    r"(\w+)\s*=\s*['\"]([^'\"]+)['\"]", where_condition, re.IGNORECASE
                )
                if eq_match:
                    col_name = eq_match.group(1)
                    value = eq_match.group(2)
                    resolved_col = ColumnResolver.resolve_column_name(
                        col_name, df.columns, case_sensitive
                    )
                    if resolved_col:
                        df = cast(
                            "DataFrame", df_ops.filter(F.col(resolved_col) == value)
                        )
                        df_ops = cast("SupportsDataFrameOps", df)
            # Check for IN clause: column IN (value1, value2, ...)
            elif re.search(r"(\w+)\s+IN\s*\(", where_condition, re.IGNORECASE):
                in_match = re.search(
                    r"(\w+)\s+IN\s*\((.*?)\)", where_condition, re.IGNORECASE
                )
                if in_match:
                    col_name = in_match.group(1)
                    values_str = in_match.group(2).strip()
                    # Parse values (handle both numbers and strings)
                    values: List[Union[float, int, str]] = []
                    for val in values_str.split(","):
                        val = val.strip()
                        # Try to parse as number
                        try:
                            if "." in val:
                                values.append(float(val))
                            else:
                                values.append(int(val))
                        except ValueError:
                            # Remove quotes if present
                            val = val.strip("'\"")
                            values.append(val)

                    resolved_col = ColumnResolver.resolve_column_name(
                        col_name, df.columns, case_sensitive
                    )
                    if resolved_col:
                        df = cast(
                            "DataFrame", df_ops.filter(F.col(resolved_col).isin(values))
                        )
                        df_ops = cast("SupportsDataFrameOps", df)
            # Check for comparison operators: column > value, column < value, etc.
            else:
                match = re.search(r"(\w+)\s*([><=]+)\s*([0-9.]+)", where_condition)
                if match:
                    col_name = match.group(1)
                    operator = match.group(2)
                    # Try to parse as float first, then int
                    try:
                        value = float(match.group(3))
                        # If it's a whole number, use int for cleaner comparison
                        if value.is_integer():
                            value = int(value)
                    except ValueError:
                        value = match.group(3)

                    # Resolve column name case-insensitively
                    resolved_col = ColumnResolver.resolve_column_name(
                        col_name, df.columns, case_sensitive
                    )
                    if resolved_col:
                        if operator == ">":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(resolved_col) > value)
                            )
                        elif operator == "<":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(resolved_col) < value)
                            )
                        elif operator in ("=", "=="):
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(resolved_col) == value)
                            )
                        elif operator == ">=":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(resolved_col) >= value)
                            )
                        elif operator == "<=":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(resolved_col) <= value)
                            )
                        df_ops = cast("SupportsDataFrameOps", df)

        # Check if we have GROUP BY
        group_by_columns = components.get("group_by_columns", [])
        select_columns = components.get("select_columns", ["*"])

        if group_by_columns:
            # Parse aggregate functions from SELECT columns
            from ...functions import F

            agg_exprs: List[
                Union[ColumnOperation, AggregateFunction, CaseWhen, Literal, IColumn]  # noqa: F821
            ] = []
            select_exprs = []

            for col_item in select_columns:
                # Normalize column item (handles both string and dict formats)
                col_expr, alias = self._normalize_column_item(col_item)
                col_expr = col_expr.strip()

                col_upper = col_expr.upper()

                # Check for aggregate functions
                if col_upper.startswith("COUNT("):
                    # Extract column name from COUNT(*column_name) or COUNT(*)
                    if "*" in col_expr:
                        expr = F.count("*")
                    else:
                        # Extract content between parentheses
                        inner = col_expr[
                            col_expr.index("(") + 1 : col_expr.rindex(")")
                        ].strip()
                        expr = F.count(inner) if inner != "*" else F.count("*")
                    agg_exprs.append(expr.alias(alias) if alias else expr)
                elif col_upper.startswith("SUM("):
                    # Extract content between parentheses
                    inner = col_expr[
                        col_expr.index("(") + 1 : col_expr.rindex(")")
                    ].strip()
                    # Handle expressions like SUM(quantity * price)
                    if "*" in inner:
                        parts = [p.strip() for p in inner.split("*")]
                        if len(parts) == 2:
                            col_op = F.col(parts[0]) * F.col(parts[1])
                            # F.sum can accept ColumnOperation directly
                            expr = F.sum(col_op)
                        else:
                            # More complex expression - try to parse
                            expr = F.sum(inner)  # Fallback
                    else:
                        expr = F.sum(inner)
                    agg_exprs.append(expr.alias(alias) if alias else expr)
                elif col_upper.startswith("AVG("):
                    inner = col_expr[
                        col_expr.index("(") + 1 : col_expr.rindex(")")
                    ].strip()
                    if "*" in inner:
                        parts = [p.strip() for p in inner.split("*")]
                        if len(parts) == 2:
                            col_op = F.col(parts[0]) * F.col(parts[1])
                            # F.avg can accept ColumnOperation directly
                            expr = F.avg(col_op)
                        else:
                            expr = F.avg(inner)
                    else:
                        expr = F.avg(inner)
                    agg_exprs.append(expr.alias(alias) if alias else expr)
                elif col_upper.startswith("MAX("):
                    inner = col_expr[
                        col_expr.index("(") + 1 : col_expr.rindex(")")
                    ].strip()
                    expr = F.max(inner)
                    agg_exprs.append(expr.alias(alias) if alias else expr)
                elif col_upper.startswith("MIN("):
                    inner = col_expr[
                        col_expr.index("(") + 1 : col_expr.rindex(")")
                    ].strip()
                    expr = F.min(inner)
                    agg_exprs.append(expr.alias(alias) if alias else expr)
                else:
                    # Non-aggregate column (should be in GROUP BY)
                    col_name = (
                        col_expr.split(" AS ")[0].strip()
                        if " AS " in col_upper
                        else col_expr
                    )
                    # Don't add group-by columns to select_exprs - they're automatically included
                    if col_name not in group_by_columns:
                        select_exprs.append(
                            F.col(col_name).alias(alias) if alias else F.col(col_name)
                        )

            # Perform GROUP BY with aggregations
            # Handle boolean expressions in GROUP BY (e.g., "GROUP BY (age > 30)")
            group_by_cols = []
            temp_df: DataFrame = cast("DataFrame", df_ops)
            for i, col_expr in enumerate(group_by_columns):
                col_expr = col_expr.strip()
                # Check if this is a boolean expression like "(age > 30)"
                if col_expr.startswith("(") and col_expr.endswith(")"):
                    # Extract the expression inside parentheses
                    expr_str = col_expr[1:-1].strip()
                    # Try to parse as a comparison expression (e.g., "age > 30")
                    comparison_match = re.search(r"(\w+)\s*([><=]+)\s*(\d+)", expr_str)
                    if comparison_match:
                        col_name = comparison_match.group(1)
                        operator = comparison_match.group(2)
                        value = int(comparison_match.group(3))
                        # Create a temporary column for the boolean expression
                        temp_col_name = f"_group_by_expr_{i}"
                        # Create a boolean column expression
                        if operator == ">":
                            temp_df = cast(
                                "DataFrame",
                                temp_df.withColumn(
                                    temp_col_name, F.col(col_name) > value
                                ),
                            )
                        elif operator == "<":
                            temp_df = cast(
                                "DataFrame",
                                temp_df.withColumn(
                                    temp_col_name, F.col(col_name) < value
                                ),
                            )
                        elif operator in ("=", "=="):
                            temp_df = cast(
                                "DataFrame",
                                temp_df.withColumn(
                                    temp_col_name, F.col(col_name) == value
                                ),
                            )
                        elif operator == ">=":
                            temp_df = cast(
                                "DataFrame",
                                temp_df.withColumn(
                                    temp_col_name, F.col(col_name) >= value
                                ),
                            )
                        elif operator == "<=":
                            temp_df = cast(
                                "DataFrame",
                                temp_df.withColumn(
                                    temp_col_name, F.col(col_name) <= value
                                ),
                            )
                        else:
                            # Fallback: use as column name
                            temp_col_name = col_expr
                        group_by_cols.append(temp_col_name)
                        df_ops = cast("SupportsDataFrameOps", temp_df)
                    else:
                        # Not a comparison, use as column name
                        group_by_cols.append(col_expr)
                else:
                    # Regular column name
                    group_by_cols.append(col_expr)

            grouped = df_ops.groupBy(*group_by_cols)
            if agg_exprs:
                # Only add aggregate expressions - group by columns are automatically included
                df = cast("DataFrame", grouped.agg(*agg_exprs))
            else:
                # No aggregations, just group by
                if select_exprs:
                    df = cast("DataFrame", grouped.agg(*select_exprs))
                else:
                    df = cast("DataFrame", grouped.agg())

            df_ops = cast("SupportsDataFrameOps", df)

            # Apply HAVING conditions (after GROUP BY, before column selection)
            having_conditions = components.get("having_conditions", [])
            if having_conditions:
                # Parse HAVING condition - simple implementation
                # Example: "AVG(salary) > 55000" or "avg_salary > 55000"
                having_condition = having_conditions[0]

                # Try to match aggregate function patterns like "AVG(salary) > 55000"
                # Match patterns like "AGG_FUNC(col) > value" or "column_alias > value"
                agg_func_match = re.search(
                    r"(\w+)\s*\(\s*(\w+)\s*\)\s*([><=]+)\s*(\d+)",
                    having_condition,
                    re.IGNORECASE,
                )
                if agg_func_match:
                    # HAVING with aggregate function: "AVG(salary) > 55000"
                    agg_func_name = agg_func_match.group(1).upper()
                    agg_col_name = agg_func_match.group(2)
                    operator = agg_func_match.group(3)
                    value = int(agg_func_match.group(4))

                    # Find matching column in result - check both aliased and generated names
                    # The column might be aliased (e.g., "avg_salary") or use generated name (e.g., "avg(salary)")
                    having_col_name: Optional[str] = None
                    # Check in df_ops.columns since that's what we're filtering
                    for col in df_ops.columns:
                        # Check if column name matches the aggregate function pattern
                        if agg_func_name in ["AVG", "MEAN"]:
                            if (
                                col.lower() == f"avg({agg_col_name})"
                                or col.lower() == "avg_salary"
                                or "avg" in col.lower()
                            ):
                                having_col_name = col
                                break
                        elif agg_func_name == "SUM":
                            if (
                                col.lower() == f"sum({agg_col_name})"
                                or "sum" in col.lower()
                            ):
                                having_col_name = col
                                break
                        elif agg_func_name == "COUNT":
                            if (
                                col.lower() == f"count({agg_col_name})"
                                or col.lower() == "count"
                                or "count" in col.lower()
                            ):
                                having_col_name = col
                                break
                        elif agg_func_name == "MAX":
                            if (
                                col.lower() == f"max({agg_col_name})"
                                or "max" in col.lower()
                            ):
                                having_col_name = col
                                break
                        elif agg_func_name == "MIN" and (
                            col.lower() == f"min({agg_col_name})"
                            or "min" in col.lower()
                        ):
                            having_col_name = col
                            break

                    # If not found in loop, try exact match with generated name or alias
                    if not having_col_name:
                        # Try generated name first (e.g., "avg(salary)")
                        generated_name = f"{agg_func_name.lower()}({agg_col_name})"
                        if generated_name in df_ops.columns:
                            having_col_name = generated_name
                        else:
                            # Try alias pattern (lowercase with underscore, e.g., "avg_salary")
                            alias_name = f"{agg_func_name.lower()}_{agg_col_name}"
                            if alias_name in df_ops.columns:
                                having_col_name = alias_name
                            else:
                                # Last resort: find any column that contains the function name and column name
                                for col in df_ops.columns:
                                    if (
                                        agg_func_name.lower() in col.lower()
                                        and agg_col_name.lower() in col.lower()
                                    ):
                                        having_col_name = col
                                        break

                    # Apply filter if column found
                    # Check in df_ops.columns since that's what we're filtering
                    if having_col_name and having_col_name in df_ops.columns:
                        if operator == ">":
                            df = cast(
                                "DataFrame",
                                df_ops.filter(F.col(having_col_name) > value),
                            )
                        elif operator == "<":
                            df = cast(
                                "DataFrame",
                                df_ops.filter(F.col(having_col_name) < value),
                            )
                        elif operator in ("=", "=="):
                            df = cast(
                                "DataFrame",
                                df_ops.filter(F.col(having_col_name) == value),
                            )
                        elif operator == ">=":
                            df = cast(
                                "DataFrame",
                                df_ops.filter(F.col(having_col_name) >= value),
                            )
                        elif operator == "<=":
                            df = cast(
                                "DataFrame",
                                df_ops.filter(F.col(having_col_name) <= value),
                            )
                        # Update df_ops after filter for all operators
                        df_ops = cast("SupportsDataFrameOps", df)
                    else:
                        # Try simple pattern match for column name > value
                        simple_match = re.search(
                            r"(\w+)\s*([><=]+)\s*(\d+)", having_condition
                        )
                        if simple_match:
                            col_name = simple_match.group(1)
                            operator = simple_match.group(2)
                            value = int(simple_match.group(3))

                            # Check if column exists in result
                            if col_name in df_ops.columns:
                                if operator == ">":
                                    df = cast(
                                        "DataFrame",
                                        df_ops.filter(F.col(col_name) > value),
                                    )
                                elif operator == "<":
                                    df = cast(
                                        "DataFrame",
                                        df_ops.filter(F.col(col_name) < value),
                                    )
                                elif operator in ("=", "=="):
                                    df = cast(
                                        "DataFrame",
                                        df_ops.filter(F.col(col_name) == value),
                                    )
                                elif operator == ">=":
                                    df = cast(
                                        "DataFrame",
                                        df_ops.filter(F.col(col_name) >= value),
                                    )
                                elif operator == "<=":
                                    df = cast(
                                        "DataFrame",
                                        df_ops.filter(F.col(col_name) <= value),
                                    )
                                df_ops = cast("SupportsDataFrameOps", df)

            # Now apply column selection after HAVING (if needed)
            # If SELECT clause doesn't include group-by columns, exclude them from result
            # (e.g., "SELECT COUNT(*) FROM ... GROUP BY (age > 30)" should only return count)
            select_columns = components.get("select_columns", ["*"])
            if select_columns != ["*"]:
                # Check if any group-by columns are in the SELECT
                # Extract column names and aliases from normalized format
                select_col_names = []
                select_aliases = []
                for col_item in select_columns:
                    col_expr, alias = self._normalize_column_item(col_item)
                    # Extract column name (handle table.column format)
                    col_name = col_expr.split(".")[-1].strip()
                    select_col_names.append(col_name)
                    if alias:
                        select_aliases.append(alias)

                # Remove temporary group-by columns that aren't in SELECT
                cols_to_keep = []
                for col in df_ops.columns:
                    # Keep if it's in SELECT or if it's a regular group-by column (not temporary)
                    if (
                        col in select_col_names
                        or col in select_aliases
                        or not col.startswith("_group_by_expr_")
                    ):
                        # But only if it's actually in SELECT or is a regular group-by column
                        if (
                            col in select_col_names
                            or col in select_aliases
                            or col in group_by_columns
                        ):
                            cols_to_keep.append(col)
                    # Always keep aggregate columns (like count, avg_salary) if they're in SELECT
                    elif any(
                        agg in col.lower()
                        for agg in ["count", "sum", "avg", "max", "min"]
                    ) and any(
                        agg in sel_col.lower()
                        for sel_col in select_columns
                        for agg in ["count", "sum", "avg", "max", "min"]
                    ):
                        cols_to_keep.append(col)

                # Select only the columns we want to keep
                if cols_to_keep:
                    df = cast("DataFrame", df_ops.select(*cols_to_keep))
                    df_ops = cast("SupportsDataFrameOps", df)
                else:
                    # Fallback: select all columns that match SELECT clause
                    df = cast(
                        "DataFrame",
                        df_ops.select(
                            *[
                                F.col(c)
                                for c in df_ops.columns
                                if c in select_col_names
                                or c in select_aliases
                                or any(
                                    agg in c.lower()
                                    for agg in ["count", "sum", "avg", "max", "min"]
                                )
                            ]
                        ),
                    )
                    df_ops = cast("SupportsDataFrameOps", df)
            else:
                # Try simple pattern match for column name > value
                simple_match = re.search(r"(\w+)\s*([><=]+)\s*(\d+)", having_condition)
                if simple_match:
                    col_name = simple_match.group(1)
                    operator = simple_match.group(2)
                    value = int(simple_match.group(3))

                    # Check if column exists in result
                    if col_name in df.columns:
                        if operator == ">":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(col_name) > value)
                            )
                        elif operator == "<":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(col_name) < value)
                            )
                        elif operator in ("=", "=="):
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(col_name) == value)
                            )
                        elif operator == ">=":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(col_name) >= value)
                            )
                        elif operator == "<=":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(col_name) <= value)
                            )
                        df_ops = cast("SupportsDataFrameOps", df)
        else:
            # No GROUP BY - check if we have aggregate functions (like SELECT AVG(col) FROM table)
            # If so, we need to aggregate over all rows
            select_columns = components.get("select_columns", ["*"])
            has_aggregates = False

            if select_columns != ["*"]:
                for col_item in select_columns:
                    # Normalize column item (handles both string and dict formats)
                    col_expr, _ = self._normalize_column_item(col_item)
                    col_upper = col_expr.upper().strip()
                    # Check for aggregate functions
                    if any(
                        col_upper.startswith(agg)
                        for agg in [
                            "COUNT(",
                            "SUM(",
                            "AVG(",
                            "MAX(",
                            "MIN(",
                            "AVERAGE(",
                        ]
                    ):
                        has_aggregates = True
                        break

            if has_aggregates:
                # We have aggregate functions without GROUP BY - aggregate over all rows
                agg_exprs_no_group: List[
                    Union[ColumnOperation, AggregateFunction, CaseWhen, Literal]
                ] = []

                for col_item in select_columns:
                    # Normalize column item (handles both string and dict formats)
                    col_expr, alias = self._normalize_column_item(col_item)
                    col_expr = col_expr.strip()

                    col_upper = col_expr.upper()

                    # Parse aggregate functions
                    if col_upper.startswith("COUNT("):
                        if "*" in col_expr:
                            expr = F.count("*")
                        else:
                            inner = col_expr[
                                col_expr.index("(") + 1 : col_expr.rindex(")")
                            ].strip()
                            expr = F.count(inner)
                        result_expr = expr.alias(alias) if alias else expr
                        agg_exprs_no_group.append(
                            cast(
                                "Union[ColumnOperation, AggregateFunction, CaseWhen, Literal, IColumn]",  # noqa: F821
                                result_expr,
                            )
                        )
                    elif col_upper.startswith("AVG(") or col_upper.startswith(
                        "AVERAGE("
                    ):
                        inner = col_expr[
                            col_expr.index("(") + 1 : col_expr.rindex(")")
                        ].strip()
                        expr = F.avg(inner)
                        result_expr = expr.alias(alias) if alias else expr
                        agg_exprs_no_group.append(
                            cast(
                                "Union[ColumnOperation, AggregateFunction, CaseWhen, Literal, IColumn]",  # noqa: F821
                                result_expr,
                            )
                        )
                    elif col_upper.startswith("SUM("):
                        inner = col_expr[
                            col_expr.index("(") + 1 : col_expr.rindex(")")
                        ].strip()
                        expr = F.sum(inner)
                        result_expr = expr.alias(alias) if alias else expr
                        agg_exprs_no_group.append(
                            cast(
                                "Union[ColumnOperation, AggregateFunction, CaseWhen, Literal, IColumn]",  # noqa: F821
                                result_expr,
                            )
                        )
                    elif col_upper.startswith("MAX("):
                        inner = col_expr[
                            col_expr.index("(") + 1 : col_expr.rindex(")")
                        ].strip()
                        expr = F.max(inner)
                        result_expr = expr.alias(alias) if alias else expr
                        agg_exprs_no_group.append(
                            cast(
                                "Union[ColumnOperation, AggregateFunction, CaseWhen, Literal, IColumn]",  # noqa: F821
                                result_expr,
                            )
                        )
                    elif col_upper.startswith("MIN("):
                        inner = col_expr[
                            col_expr.index("(") + 1 : col_expr.rindex(")")
                        ].strip()
                        expr = F.min(inner)
                        result_expr = expr.alias(alias) if alias else expr
                        agg_exprs_no_group.append(
                            cast(
                                "Union[ColumnOperation, AggregateFunction, CaseWhen, Literal, IColumn]",  # noqa: F821
                                result_expr,
                            )
                        )
                    else:
                        # Non-aggregate column in aggregate query - use F.col()
                        col_name = col_expr
                        if (
                            "." in col_name
                            and not col_name.startswith("'")
                            and not col_name.startswith('"')
                        ):
                            parts = col_name.split(".", 1)
                            if len(parts) == 2:
                                col_name = parts[1]
                        agg_expr: Any = F.col(col_name)
                        agg_exprs_no_group.append(
                            agg_expr.alias(alias) if alias else agg_expr
                        )

                # Aggregate over all rows (no grouping columns)
                # Convert to DataFrame and group by nothing to aggregate all rows
                df_dataframe = cast("DataFrame", df_ops)
                # Type ignore needed because agg accepts Union[str, Column, ColumnOperation, AggregateFunction, Dict[str, str]]
                # but we're passing a list that may contain Column which is compatible
                df = df_dataframe.groupBy().agg(*agg_exprs_no_group)  # type: ignore[arg-type]
            else:
                # No GROUP BY, no aggregates - just apply column selection
                if select_columns != ["*"]:
                    # Parse column expressions with aliases, table prefixes, and CASE WHEN
                    select_exprs_no_group: List[
                        Union[
                            ColumnOperation,
                            AggregateFunction,
                            CaseWhen,
                            Literal,
                            IColumn,
                        ]  # noqa: F821
                    ] = []
                    for col_item in select_columns:
                        # Normalize column item (handles both string and dict formats)
                        col_expr, alias = self._normalize_column_item(col_item)
                        col = col_expr.strip()

                        # Check if this is a CASE WHEN expression
                        col_upper = col.upper().strip()
                        if col_upper.startswith("CASE") and "END" in col_upper:
                            # Parse CASE WHEN expression using SQLExprParser
                            from ...functions.core.sql_expr_parser import SQLExprParser

                            case_expr: Union[
                                ColumnOperation,
                                AggregateFunction,
                                CaseWhen,
                                Literal,
                                IColumn,  # noqa: F821
                            ] = cast(
                                "Union[ColumnOperation, AggregateFunction, CaseWhen, Literal, IColumn]",  # noqa: F821
                                SQLExprParser._parse_expression(col),
                            )
                            if alias:
                                # alias() returns IColumn, cast to expected type
                                case_expr = cast(
                                    "Union[ColumnOperation, AggregateFunction, CaseWhen, Literal, IColumn]",  # noqa: F821
                                    case_expr.alias(alias),
                                )
                            select_exprs_no_group.append(case_expr)
                        else:
                            literal_expr: Union[Column, CaseWhen, Literal, IColumn] = (
                                None  # noqa: F821
                            )
                            is_string_literal = (
                                col.startswith("'") and col.endswith("'")
                            ) or (col.startswith('"') and col.endswith('"'))
                            if (
                                col_upper in ("NULL", "TRUE", "FALSE")
                                or is_string_literal
                                or re.fullmatch(r"[-+]?\d+(?:\.\d+)?", col)
                            ):
                                literal_value = self._parse_sql_value(col)
                                lit_col = F.lit(literal_value)
                                if is_string_literal:
                                    from ...spark_types import StringType

                                    lit_col = cast("Any", lit_col.cast(StringType()))

                                literal_expr = (
                                    lit_col.alias(alias) if alias else lit_col
                                )

                            if literal_expr is not None:
                                select_exprs_no_group.append(literal_expr)
                                continue

                            # Handle table alias prefix (e.g., "e.name" -> "e_name" after join)
                            # After a join, columns are renamed with table alias prefix
                            if (
                                "." in col
                                and not col.startswith("'")
                                and not col.startswith('"')
                            ):
                                parts = col.split(".", 1)
                                if len(parts) == 2:
                                    table_alias, base_col = parts
                                    # Check if this column exists with the alias prefix (from join)
                                    prefixed_col = f"{table_alias}_{base_col}"
                                    if prefixed_col in df.columns:
                                        col_name = prefixed_col
                                    else:
                                        # If join happened, we should have prefixed columns
                                        # Try to find any column with this base name that matches the alias
                                        # This handles cases where the alias might be different
                                        matching_cols = [
                                            c
                                            for c in df.columns
                                            if c.endswith(f"_{base_col}")
                                            or c == base_col
                                        ]
                                        if matching_cols:
                                            # Prefer the one that matches the alias prefix
                                            alias_matches: List[str] = [
                                                c
                                                for c in matching_cols
                                                if c.startswith(f"{table_alias}_")
                                            ]
                                            if alias_matches:
                                                col_name = alias_matches[0]
                                            else:
                                                # Fallback to first matching column
                                                col_name = matching_cols[0]
                                        else:
                                            # No match found, use base column name as last resort
                                            col_name = base_col
                                else:
                                    col_name = col
                            else:
                                col_name = col

                            # Create column expression with alias if specified
                            if alias:
                                select_exprs_no_group.append(
                                    F.col(col_name).alias(alias)
                                )
                            else:
                                select_exprs_no_group.append(F.col(col_name))

                    df = cast("DataFrame", df_ops.select(*select_exprs_no_group))
            df_ops = cast("SupportsDataFrameOps", df)

        # Apply ORDER BY
        order_by_columns = components.get("order_by_columns", [])
        if order_by_columns:
            # Parse ORDER BY columns - handle DESC/ASC, preserve original case
            # Note: Parser already strips ASC, so we only need to handle DESC
            order_exprs = []
            for col_expr in order_by_columns:
                col_expr_upper = col_expr.upper()
                if " DESC" in col_expr_upper or col_expr_upper.endswith(" DESC"):
                    # Extract column name preserving original case
                    col_name = re.sub(
                        r"\s+DESC\s*$", "", col_expr, flags=re.IGNORECASE
                    ).strip()
                    order_exprs.append(F.col(col_name).desc())
                else:
                    # No DESC specified, default to ascending (ASC is already stripped by parser)
                    order_exprs.append(F.col(col_expr).asc())
            if order_exprs:
                df = cast("DataFrame", df_ops.orderBy(*order_exprs))
                df_ops = cast("SupportsDataFrameOps", df)

        # Apply LIMIT
        limit_value = components.get("limit_value")
        if limit_value:
            df = cast("DataFrame", df_ops.limit(limit_value))
            df_ops = cast("SupportsDataFrameOps", df)

        return cast("IDataFrame", df)

    def _execute_union(self, ast: SQLAST) -> IDataFrame:
        """Execute UNION query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            DataFrame with UNION results.
        """
        components = ast.components

        # Get left and right queries
        left_query = components.get("left_query", "")
        right_query = components.get("right_query", "")

        if not left_query or not right_query:
            raise QueryExecutionException("UNION requires two SELECT statements")

        # Execute both SELECT queries
        left_ast = self.parser.parse(left_query)
        right_ast = self.parser.parse(right_query)

        if left_ast.query_type != "SELECT" or right_ast.query_type != "SELECT":
            raise QueryExecutionException("UNION can only combine SELECT statements")

        left_df = self._execute_select(left_ast)
        right_df = self._execute_select(right_ast)

        # Convert to DataFrame if needed
        from ...dataframe import DataFrame

        if not isinstance(left_df, DataFrame):  # type: ignore[unreachable]
            from ...spark_types import StructType

            schema = (
                StructType(left_df.schema.fields)  # type: ignore[arg-type]
                if hasattr(left_df.schema, "fields")
                else StructType([])
            )
            left_df = DataFrame(left_df.collect(), schema)  # type: ignore[assignment]

        if not isinstance(right_df, DataFrame):  # type: ignore[unreachable]
            from ...spark_types import StructType

            schema = (
                StructType(right_df.schema.fields)  # type: ignore[arg-type]
                if hasattr(right_df.schema, "fields")
                else StructType([])
            )
            right_df = DataFrame(right_df.collect(), schema)  # type: ignore[assignment]

        # Perform union (removes duplicates like UNION, not UNION ALL)
        result = cast("DataFrame", left_df.union(right_df))

        # Materialize if lazy (union() may queue operations)
        if hasattr(result, "_materialize_if_lazy"):
            result = cast("DataFrame", result._materialize_if_lazy())
        elif hasattr(result, "_operations_queue") and result._operations_queue:
            # Force materialization if operations are queued
            result = cast("DataFrame", result._materialize_if_lazy())

        return cast("IDataFrame", result)

    def _execute_create(self, ast: SQLAST) -> IDataFrame:
        """Execute CREATE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.

        Raises:
            AnalysisException: If table already exists and IF NOT EXISTS is not specified.
            QueryExecutionException: If schema parsing fails.
        """
        components = ast.components
        object_type = components.get("object_type", "TABLE").upper()
        object_name = components.get("object_name", "unknown")
        # Default to True for backward compatibility and safer behavior
        ignore_if_exists = components.get("ignore_if_exists", True)
        replace_if_exists = components.get("replace", False)
        table_format = components.get("table_format")

        # Import required types (used by all code paths)
        from ...dataframe import DataFrame
        from ...spark_types import StructType
        from typing import cast

        # Handle both DATABASE and SCHEMA keywords (they're synonymous in Spark)
        if object_type in ("DATABASE", "SCHEMA"):
            self.session.catalog.createDatabase(
                object_name, ignoreIfExists=ignore_if_exists
            )
            # Return empty DataFrame to indicate success
            return cast("IDataFrame", DataFrame([], StructType([])))
        elif object_type == "TABLE":
            # Parse schema and table name
            schema_name = components.get("schema_name")
            table_name = object_name
            # Access storage through catalog (ISession protocol doesn't expose _storage)
            storage = getattr(self.session, "_storage", None)
            if storage is None:
                storage = self.session.catalog.get_storage_backend()
            if schema_name is None:
                schema_name = storage.get_current_schema()

            # Check if table exists
            table_exists = storage.table_exists(schema_name, table_name)

            # Check if this is CREATE TABLE AS SELECT (CTAS)
            select_query = components.get("select_query")

            if table_exists:
                if replace_if_exists:
                    # For CTAS, don't drop before executing SELECT to allow self-references;
                    # the writer will overwrite after the SELECT result is materialized.
                    if not select_query:
                        storage.drop_table(schema_name, table_name)
                        table_exists = False
                elif not ignore_if_exists:
                    from ...errors import AnalysisException

                    raise AnalysisException(
                        f"Table {schema_name}.{table_name} already exists"
                    )
                else:
                    # Table exists and IF NOT EXISTS is specified, skip creation
                    return cast("IDataFrame", DataFrame([], StructType([])))

            if select_query:
                # Execute the SELECT query
                select_ast = self.parser.parse(select_query)
                result_df = self._execute_select(select_ast)

                # Save the result as a table
                # Convert to DataFrame if needed
                if not isinstance(result_df, DataFrame):  # type: ignore[unreachable]
                    from ...spark_types import StructField

                    # Convert IStructField list to StructField list
                    fields = [
                        StructField(f.name, f.data_type, f.nullable)
                        for f in result_df.schema.fields
                    ]
                    result_df = DataFrame(result_df.collect(), StructType(fields))  # type: ignore[assignment]

                # Build table full name
                table_full_name = (
                    f"{schema_name}.{object_name}" if schema_name else object_name
                )

                # Write to table using saveAsTable
                writer = result_df.write
                if table_format:
                    writer = writer.format(table_format)
                writer.mode("overwrite").saveAsTable(table_full_name)

                # Return empty DataFrame to indicate success
                return cast("IDataFrame", DataFrame([], StructType([])))

            # Parse column definitions
            column_definitions = components.get("column_definitions", "")
            if not column_definitions:
                from ...errors import QueryExecutionException

                raise QueryExecutionException(
                    "CREATE TABLE requires column definitions or AS SELECT clause"
                )

            # Parse DDL schema using DDL adapter
            from ...core.ddl_adapter import parse_ddl_schema

            try:
                schema = parse_ddl_schema(column_definitions)
            except Exception as e:
                from ...errors import QueryExecutionException

                raise QueryExecutionException(
                    f"Failed to parse table schema: {str(e)}"
                ) from e

            # Create table in storage backend
            storage.create_table(schema_name, table_name, schema.fields)
            if table_format:
                with contextlib.suppress(Exception):
                    storage.update_table_metadata(
                        schema_name, table_name, {"format": table_format}
                    )

        # Return empty DataFrame to indicate success
        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_drop(self, ast: SQLAST) -> IDataFrame:
        """Execute DROP query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.

        Raises:
            AnalysisException: If table does not exist and IF EXISTS is not specified.
        """
        components = ast.components
        object_type = components.get("object_type", "TABLE").upper()
        object_name = components.get("object_name", "unknown")
        # Default to True for backward compatibility and safer behavior
        ignore_if_not_exists = components.get("ignore_if_not_exists", True)

        # Handle both DATABASE and SCHEMA keywords (they're synonymous in Spark)
        if object_type in ("DATABASE", "SCHEMA"):
            self.session.catalog.dropDatabase(
                object_name, ignoreIfNotExists=ignore_if_not_exists
            )
        elif object_type == "TABLE":
            # Parse schema and table name
            schema_name = components.get("schema_name")
            table_name = object_name
            # Access storage through catalog (ISession protocol doesn't expose _storage)
            storage = getattr(self.session, "_storage", None)
            if storage is None:
                storage = self.session.catalog.get_storage_backend()
            if schema_name is None:
                schema_name = storage.get_current_schema()

            # Check if table exists
            if not storage.table_exists(schema_name, table_name):
                if not ignore_if_not_exists:
                    from ...errors import AnalysisException

                    raise AnalysisException(
                        f"Table {schema_name}.{table_name} does not exist"
                    )
                # Table doesn't exist and IF EXISTS is specified, skip drop
                return cast("IDataFrame", DataFrame([], StructType([])))

            # Drop table from storage backend
            storage.drop_table(schema_name, table_name)

        # Return empty DataFrame to indicate success
        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_insert(self, ast: SQLAST) -> IDataFrame:
        """Execute INSERT query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.

        Raises:
            AnalysisException: If table does not exist.
            QueryExecutionException: If INSERT parsing or execution fails.
        """
        from ...errors import AnalysisException, QueryExecutionException

        components = ast.components
        table_name = components.get("table_name", "unknown")
        schema_name = components.get("schema_name")
        insert_type = components.get("type", "unknown")

        # Access storage through catalog (ISession protocol doesn't expose _storage)
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        if schema_name is None:
            schema_name = storage.get_current_schema()

        # Check if table exists
        if not storage.table_exists(schema_name, table_name):
            raise AnalysisException(f"Table {schema_name}.{table_name} does not exist")

        # Get table schema
        table_schema = storage.get_table_schema(schema_name, table_name)
        if not isinstance(table_schema, StructType):
            raise QueryExecutionException(
                f"Failed to get schema for table {schema_name}.{table_name}"
            )

        # For INSERT VALUES, use the schema field order (PySpark behavior)
        # The schema field order is the canonical order for mapping VALUES to columns
        expected_column_order = [field.name for field in table_schema.fields]

        data: List[Dict[str, Any]] = []

        if insert_type == "VALUES":
            # Parse VALUES-based INSERT
            values = components.get("values", [])
            columns = components.get("columns", [])

            # If columns specified, use them; otherwise use the expected column order
            target_columns = columns or expected_column_order

            # Convert string values to Python types
            for row_values in values:
                row_dict: Dict[str, Any] = {}
                for i, value_str in enumerate(row_values):
                    if i >= len(target_columns):
                        break  # Skip extra values
                    col_name = target_columns[i]
                    # Parse value (handle strings, numbers, null, booleans)
                    parsed_value = self._parse_sql_value(value_str.strip())
                    row_dict[col_name] = parsed_value

                # Fill missing columns with None (only if columns were specified)
                if columns:
                    for field in table_schema.fields:
                        if field.name not in row_dict:
                            row_dict[field.name] = None
                else:
                    # All columns were provided in VALUES, ensure all are present
                    for field in table_schema.fields:
                        if field.name not in row_dict:
                            row_dict[field.name] = None

                data.append(row_dict)

        elif insert_type == "SELECT":
            # Execute SELECT query and get DataFrame
            select_query = components.get("select_query", "")
            if not select_query:
                raise QueryExecutionException(
                    "SELECT query is missing in INSERT ... SELECT"
                )

            # Execute the SELECT part of INSERT ... SELECT.
            # The parser may return either a full SELECT statement or just the
            # projection/FROM fragment, so avoid blindly prefixing with another
            # SELECT (BUG-014).
            query = select_query.strip()
            if not query.upper().startswith("SELECT"):
                query = f"SELECT {query}"

            select_df = self.session.sql(query)
            # Convert DataFrame to list of dictionaries
            data = []
            for row in select_df.collect():
                if isinstance(row, dict):
                    data.append(row)
                elif hasattr(row, "asDict"):
                    # Row object - use asDict() method
                    row_dict = row.asDict()
                    data.append(row_dict if isinstance(row_dict, dict) else {})
                elif hasattr(row, "__dict__"):
                    # Object with __dict__ attribute
                    data.append(row.__dict__)
                else:
                    # Try to convert to dict, or use as-is if it's already a dict-like structure
                    data.append(row if isinstance(row, dict) else {})

        else:
            raise QueryExecutionException(f"Unsupported INSERT type: {insert_type}")

        # Validate and coerce data
        if data:
            from ...core.data_validation import DataValidator

            validator = DataValidator(
                table_schema,
                validation_mode="relaxed",
                enable_coercion=True,
            )
            data = validator.coerce(data)

        # Insert data into storage backend
        # Access storage through catalog (ISession protocol doesn't expose _storage)
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        if data:
            storage.insert_data(schema_name, table_name, data)

        # Return empty DataFrame to indicate success
        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

    def _parse_sql_value(self, value_str: str) -> Any:
        """Parse a SQL value string into Python type.

        Args:
            value_str: SQL value string (e.g., "123", "'text'", "NULL", "true")

        Returns:
            Parsed Python value
        """
        value_str = value_str.strip()

        # Handle NULL
        if value_str.upper() == "NULL" or value_str == "":
            return None

        # Handle quoted strings
        if (value_str.startswith("'") and value_str.endswith("'")) or (
            value_str.startswith('"') and value_str.endswith('"')
        ):
            return value_str[1:-1]  # Remove quotes

        # Handle booleans
        if value_str.upper() == "TRUE":
            return True
        if value_str.upper() == "FALSE":
            return False

        # Handle numbers
        try:
            # Try integer first
            if "." not in value_str and "e" not in value_str.lower():
                return int(value_str)
            # Try float
            return float(value_str)
        except ValueError:
            pass

        # Default: return as string
        return value_str

    def _execute_update(self, ast: SQLAST) -> IDataFrame:
        """Execute UPDATE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.

        Raises:
            AnalysisException: If table does not exist.
            QueryExecutionException: If UPDATE execution fails.
        """
        components = ast.components
        table_name = components.get("table_name", "unknown")
        schema_name = components.get("schema_name")
        set_clauses = components.get("set_clauses", [])
        where_conditions = components.get("where_conditions", [])

        # Access storage through catalog (ISession protocol doesn't expose _storage)
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        if schema_name is None:
            schema_name = storage.get_current_schema()

        # Build qualified table name
        qualified_name = f"{schema_name}.{table_name}" if schema_name else table_name

        # Check if table exists
        if not storage.table_exists(schema_name, table_name):
            from ...errors import AnalysisException

            raise AnalysisException(f"Table {qualified_name} does not exist")

        # Get table data and schema directly (avoid DataFrame operations that use lazy evaluation)
        rows = storage.get_data(schema_name, table_name)
        table_schema = storage.get_table_schema(schema_name, table_name)

        # Import required modules
        import re
        from types import SimpleNamespace
        from ...core.safe_evaluator import SafeExpressionEvaluator

        # Helper function to evaluate condition for a row
        def evaluate_condition(row: Dict[str, Any], condition: str) -> bool:
            """Evaluate WHERE condition for a single row."""
            context = dict(row)
            row_ns = SimpleNamespace(**row)
            context["target"] = row_ns
            try:
                result = SafeExpressionEvaluator.evaluate_boolean(condition, context)
                return bool(result)
            except Exception:
                return False

        # Normalize WHERE condition if present
        normalized_condition = None
        if where_conditions:
            where_expr = where_conditions[0]
            # Normalize SQL expression to Python-compatible syntax
            # Handle IS NOT NULL first (before NOT normalization)
            normalized_condition = re.sub(
                r"\bIS\s+NOT\s+NULL\b", "is not None", where_expr, flags=re.IGNORECASE
            )
            # Handle IS NULL
            normalized_condition = re.sub(
                r"\bIS\s+NULL\b", "is None", normalized_condition, flags=re.IGNORECASE
            )
            # Normalize logical operators
            normalized_condition = re.sub(
                r"\bAND\b", "and", normalized_condition, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"\bOR\b", "or", normalized_condition, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"\bNOT\b", "not", normalized_condition, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"(?<![<>!=])=(?!=)", "==", normalized_condition
            )

        # Helper function to parse and evaluate SET value
        def evaluate_set_value(value_expr: Any, row: Dict[str, Any]) -> Any:
            """Parse and evaluate SET clause value."""
            if isinstance(value_expr, str):
                expr = value_expr.strip()
                # Handle string literals
                if (expr.startswith("'") and expr.endswith("'")) or (
                    expr.startswith('"') and expr.endswith('"')
                ):
                    return expr[1:-1]
                # Handle NULL
                elif expr.upper() == "NULL":
                    return None
                # Handle booleans
                elif expr.upper() == "TRUE":
                    return True
                elif expr.upper() == "FALSE":
                    return False
                # Handle numbers
                elif expr.replace(".", "", 1).replace("-", "", 1).isdigit():
                    if "." in expr:
                        return float(expr)
                    else:
                        return int(expr)
                # Try to evaluate as expression (column reference or simple expression)
                else:
                    from ...core.safe_evaluator import SafeExpressionEvaluator

                    context = dict(row)
                    row_ns = SimpleNamespace(**row)
                    context["target"] = row_ns
                    # Normalize expression
                    normalized = re.sub(r"(?<![<>!=])=(?!=)", "==", expr)
                    try:
                        result = SafeExpressionEvaluator.evaluate(normalized, context)
                        return result if result is not None else expr
                    except Exception:
                        # If evaluation fails, return as string
                        return expr
            return value_expr

        # Apply UPDATE to rows
        updated_rows: List[Dict[str, Any]] = []
        for row in rows:
            # Check if row matches WHERE condition
            if normalized_condition:
                should_update = evaluate_condition(row, normalized_condition)
            else:
                # No WHERE clause - update all rows
                should_update = True

            if not should_update:
                # Keep row unchanged
                updated_rows.append(row)
                continue

            # Update row with SET clauses
            new_row = dict(row)
            for set_clause in set_clauses:
                column = set_clause.get("column")
                value_expr = set_clause.get("value")

                if not column:
                    continue

                # Evaluate and set new value
                new_value = evaluate_set_value(value_expr, row)
                new_row[column] = new_value

            updated_rows.append(new_row)

        # Overwrite table with updated data
        if updated_rows:
            # Create DataFrame with updated data
            updated_dataframe = self.session.createDataFrame(updated_rows, table_schema)
            # Overwrite table
            updated_dataframe.write.format("delta").mode("overwrite").saveAsTable(
                qualified_name
            )
        else:
            # Empty result - clear table
            empty_df = self.session.createDataFrame([], table_schema)
            empty_df.write.format("delta").mode("overwrite").saveAsTable(qualified_name)

        # Return empty DataFrame to indicate success
        from ...dataframe import DataFrame
        from ...spark_types import StructType

        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_delete(self, ast: SQLAST) -> IDataFrame:
        """Execute DELETE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.

        Raises:
            AnalysisException: If table does not exist.
        """
        from ...errors import AnalysisException
        import re
        from types import SimpleNamespace

        components = ast.components
        table_name = components.get("table_name", "unknown")
        schema_name = components.get("schema_name")
        where_conditions = components.get("where_conditions", [])

        # Access storage through catalog (ISession protocol doesn't expose _storage)
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        if schema_name is None:
            schema_name = storage.get_current_schema()

        # Build qualified table name
        qualified_name = f"{schema_name}.{table_name}" if schema_name else table_name

        # Check if table exists
        if not storage.table_exists(schema_name, table_name):
            raise AnalysisException(f"Table {qualified_name} does not exist")

        # Get table data and schema
        rows = storage.get_data(schema_name, table_name)
        table_schema = storage.get_table_schema(schema_name, table_name)

        # Normalize WHERE condition if present
        normalized_condition = None
        if where_conditions:
            where_expr = where_conditions[0]
            # Normalize SQL expression to Python-compatible syntax
            normalized_condition = re.sub(
                r"\bAND\b", "and", where_expr, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"\bOR\b", "or", normalized_condition, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"\bNOT\b", "not", normalized_condition, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"(?<![<>!=])=(?!=)", "==", normalized_condition
            )

        # Helper function to evaluate condition for a row
        def evaluate_condition(row: Dict[str, Any], condition: str) -> bool:
            """Evaluate WHERE condition for a single row."""
            from ...core.safe_evaluator import SafeExpressionEvaluator

            context = dict(row)
            row_ns = SimpleNamespace(**row)
            context["target"] = row_ns
            try:
                result = SafeExpressionEvaluator.evaluate_boolean(condition, context)
                return bool(result)
            except Exception:
                return False

        # Filter rows - keep rows that DON'T match WHERE condition
        if normalized_condition:
            remaining_rows = [
                row for row in rows if not evaluate_condition(row, normalized_condition)
            ]
        else:
            # No WHERE clause - delete all rows (truncate table)
            remaining_rows = []

        # Overwrite table with remaining data, preserving schema
        # Get table format from metadata (default to parquet if not specified)
        table_metadata = storage.get_table_metadata(schema_name, table_name)
        table_format = "parquet"  # Default format
        if isinstance(table_metadata, dict) and "format" in table_metadata:
            table_format = table_metadata["format"]

        if remaining_rows:
            # Create DataFrame with remaining data
            remaining_dataframe = self.session.createDataFrame(
                remaining_rows, table_schema
            )
            # Overwrite table with same format, preserving schema
            remaining_dataframe.write.format(table_format).mode(
                "overwrite"
            ).saveAsTable(qualified_name)
        else:
            # Empty result - clear table but preserve schema
            empty_df = self.session.createDataFrame([], table_schema)
            empty_df.write.format(table_format).mode("overwrite").saveAsTable(
                qualified_name
            )

        # Return empty DataFrame to indicate success
        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_refresh(self, ast: SQLAST) -> IDataFrame:
        """Execute REFRESH TABLE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.

        Raises:
            AnalysisException: If table does not exist.
        """
        from ...errors import AnalysisException

        components = ast.components
        table_name = components.get("table_name", "unknown")
        schema_name = components.get("schema_name")

        # Access storage through catalog
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        if schema_name is None:
            schema_name = storage.get_current_schema()

        # Build qualified table name
        qualified_name = f"{schema_name}.{table_name}" if schema_name else table_name

        # Check if table exists
        if not storage.table_exists(schema_name, table_name):
            raise AnalysisException(f"Table {qualified_name} does not exist")

        # Refresh table metadata - for sparkless, this is a no-op
        # but we verify the table exists and is accessible
        # In a real implementation, this would reload metadata from storage
        # For sparkless, tables are already immediately accessible after writes

        # Return empty DataFrame to indicate success
        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_show(self, ast: SQLAST) -> IDataFrame:
        """Execute SHOW query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            DataFrame with SHOW results that reflect the real catalog and
            match PySpark's formats where applicable.
        """
        from typing import cast
        from ...dataframe import DataFrame
        from ...spark_types import StructType, StructField, StringType, BooleanType

        original_query = ast.components.get("original_query", "") or ""
        query_lower = original_query.lower()

        # SHOW DATABASES
        if "show databases" in query_lower:
            databases = self.session.catalog.listDatabases()
            data = [{"databaseName": db.name} for db in databases]
            schema = StructType([StructField("databaseName", StringType())])
            return cast("IDataFrame", DataFrame(data, schema))

        # SHOW TABLES [IN db] or SHOW TABLES
        if "show tables" in query_lower:
            db_name = None
            match = re.search(
                r"show\s+tables\s+in\s+([`\w]+)", original_query, re.IGNORECASE
            )
            if match:
                db_name = match.group(1).strip("`")

            if db_name is None:
                db_name = self.session.catalog.currentDatabase()

            tables = self.session.catalog.listTables(dbName=db_name)
            # PySpark SHOW TABLES returns columns: database, tableName, isTemporary
            data = [
                {
                    "database": tbl.database,
                    "tableName": tbl.name,
                    "isTemporary": False,
                }
                for tbl in tables
            ]
            schema = StructType(
                [
                    StructField("database", StringType()),
                    StructField("tableName", StringType()),
                    StructField("isTemporary", BooleanType()),
                ]
            )
            return cast("IDataFrame", DataFrame(data, schema))

        # Fallback: unsupported SHOW variant
        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_describe(self, ast: SQLAST) -> IDataFrame:
        """Execute DESCRIBE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            DataFrame with DESCRIBE results.
        """
        # Get original query string for parsing
        original_query = ast.components.get("original_query", "")
        if not original_query and hasattr(ast, "query"):
            original_query = ast.query

        query = original_query.upper() if original_query else ""

        if "DETAIL" in query:
            # DESCRIBE DETAIL table_name
            match = re.search(
                r"DESCRIBE\s+DETAIL\s+(\w+(?:\.\w+)?)", original_query, re.IGNORECASE
            )
            if match:
                table_name = match.group(1)

                # Parse schema and table
                if "." in table_name:
                    schema_name, table_only = table_name.split(".", 1)
                else:
                    schema_name, table_only = "default", table_name

                # Get table metadata
                storage = getattr(self.session, "_storage", None)
                if storage is None:
                    storage = self.session.catalog.get_storage_backend()
                meta = storage.get_table_metadata(schema_name, table_only)

                if not meta or meta.get("format") != "delta":
                    from ...errors import AnalysisException

                    raise AnalysisException(
                        f"Table {table_name} is not a Delta table. "
                        "DESCRIBE DETAIL can only be used with Delta format tables."
                    )

                # Create mock table details matching Delta Lake schema
                from ...spark_types import (
                    StructType,
                    StructField,
                    StringType,
                    LongType,
                    ArrayType,
                    MapType,
                )

                details = [
                    {
                        "format": "delta",
                        "id": f"mock-table-{hash(table_name)}",
                        "name": table_name,
                        "description": meta.get("description"),
                        "location": meta.get(
                            "location", f"/mock/delta/{table_name.replace('.', '/')}"
                        ),
                        "createdAt": meta.get(
                            "created_at", "2024-01-01T00:00:00.000+0000"
                        ),
                        "lastModified": meta.get(
                            "last_modified", "2024-01-01T00:00:00.000+0000"
                        ),
                        "partitionColumns": meta.get("partition_columns", []),
                        "numFiles": meta.get("num_files", 1),
                        "sizeInBytes": meta.get("size_in_bytes", 1024),
                        "properties": meta.get("properties", {}),
                        "minReaderVersion": meta.get("min_reader_version", 1),
                        "minWriterVersion": meta.get("min_writer_version", 2),
                    }
                ]

                detail_schema = StructType(
                    [
                        StructField("format", StringType()),
                        StructField("id", StringType()),
                        StructField("name", StringType()),
                        StructField("description", StringType()),
                        StructField("location", StringType()),
                        StructField("createdAt", StringType()),
                        StructField("lastModified", StringType()),
                        StructField("partitionColumns", ArrayType(StringType())),
                        StructField("numFiles", LongType()),
                        StructField("sizeInBytes", LongType()),
                        StructField("properties", MapType(StringType(), StringType())),
                        StructField("minReaderVersion", LongType()),
                        StructField("minWriterVersion", LongType()),
                    ]
                )

                from typing import cast

                from ...dataframe import DataFrame

                return cast("IDataFrame", DataFrame(details, detail_schema, storage))

        if "HISTORY" in query:
            # DESCRIBE HISTORY table_name
            match = re.search(
                r"DESCRIBE\s+HISTORY\s+(\w+(?:\.\w+)?)", original_query, re.IGNORECASE
            )
            if match:
                table_name = match.group(1)

                # Parse schema and table
                if "." in table_name:
                    schema_name, table_only = table_name.split(".", 1)
                else:
                    schema_name, table_only = "default", table_name

                # Get table metadata
                # Access storage through catalog (ISession protocol doesn't expose _storage)
                storage = getattr(self.session, "_storage", None)
                if storage is None:
                    storage = self.session.catalog.get_storage_backend()
                meta = storage.get_table_metadata(schema_name, table_only)

                if not meta or meta.get("format") != "delta":
                    from ...errors import AnalysisException

                    raise AnalysisException(
                        f"Table {table_name} is not a Delta table. "
                        "DESCRIBE HISTORY can only be used with Delta format tables."
                    )

                version_history = meta.get("version_history", [])

                # Create DataFrame with history
                from ...dataframe import DataFrame
                from ...spark_types import (
                    StructType,
                )
                from typing import cast

                # Build history rows
                history_data = []
                for v in version_history:
                    # Handle both MockDeltaVersion objects and dicts
                    if hasattr(v, "version"):
                        row = {
                            "version": v.version,
                            "timestamp": v.timestamp,
                            "operation": v.operation,
                        }
                    else:
                        row = {
                            "version": v.get("version"),
                            "timestamp": v.get("timestamp"),
                            "operation": v.get("operation"),
                        }
                    history_data.append(row)

                # Return DataFrame using session's createDataFrame
                return self.session.createDataFrame(history_data)

        # Default DESCRIBE implementation
        from ...dataframe import DataFrame
        from ...spark_types import StructType, StructField, StringType
        from typing import cast

        # Parse DESCRIBE TABLE [table_name] [column_name]
        # Formats: DESCRIBE table_name, DESCRIBE EXTENDED table_name, DESCRIBE table_name column_name
        # Check for EXTENDED keyword
        is_extended = "EXTENDED" in query or "FORMATTED" in query
        # Match: DESCRIBE [EXTENDED|FORMATTED] table_name [column_name]
        # Need to explicitly match EXTENDED/FORMATTED followed by whitespace and table name
        table_match = re.search(
            r"DESCRIBE\s+(?:EXTENDED|FORMATTED)\s+(\w+(?:\.\w+)?)",
            original_query,
            re.IGNORECASE,
        )
        if not table_match:
            # Try without EXTENDED/FORMATTED
            table_match = re.search(
                r"DESCRIBE\s+(\w+(?:\.\w+)?)", original_query, re.IGNORECASE
            )
        if not table_match:
            return cast("IDataFrame", DataFrame([], StructType([])))

        table_name = table_match.group(1)

        # Check if specific column is requested: DESCRIBE [EXTENDED] table_name column_name
        # Only match if there's a word after the table name (not the table name itself)
        # First check if EXTENDED/FORMATTED is present - if so, only match column if there's something after table
        if is_extended:
            # For EXTENDED/FORMATTED, column must come after table name
            col_match = re.search(
                rf"DESCRIBE\s+(?:EXTENDED|FORMATTED)\s+{re.escape(table_name)}\s+(\w+)",
                original_query,
                re.IGNORECASE,
            )
        else:
            # For regular DESCRIBE, match column name after table
            col_match = re.search(
                rf"DESCRIBE\s+{re.escape(table_name)}\s+(\w+)",
                original_query,
                re.IGNORECASE,
            )
        column_name = col_match.group(1) if col_match else None

        # Get table
        try:
            table_df = self.session.table(table_name)
        except Exception:
            from ...errors import AnalysisException

            raise AnalysisException(f"Table or view not found: {table_name}")

        table_schema: StructType = table_df.schema  # type: ignore[assignment]

        if column_name:
            # DESCRIBE specific column
            field = None
            for f in table_schema.fields:
                if f.name.lower() == column_name.lower():
                    field = f
                    break
            if not field:
                return cast("IDataFrame", DataFrame([], StructType([])))

            comment = ""
            if field.metadata is not None and hasattr(field.metadata, "get"):
                comment = field.metadata.get("comment", "")
            data = [
                {
                    "col_name": field.name,
                    "data_type": str(field.dataType),
                    "comment": comment,
                }
            ]
            result_schema = StructType(
                [
                    StructField("col_name", StringType()),
                    StructField("data_type", StringType()),
                    StructField("comment", StringType()),
                ]
            )
        else:
            # DESCRIBE table (all columns)
            data = []
            for f in table_schema.fields:
                row = {
                    "col_name": f.name,
                    "data_type": str(f.dataType),
                }
                if is_extended:
                    # Add extended info
                    comment = ""
                    if f.metadata is not None and hasattr(f.metadata, "get"):
                        comment = f.metadata.get("comment", "")
                    row["comment"] = comment
                    # Add nullable info (basic)
                    row["nullable"] = "true" if f.nullable else "false"
                data.append(row)

            if is_extended:
                # Add extended metadata rows after column info (PySpark format)
                # Add separator row
                data.append(
                    {"col_name": "", "data_type": "", "comment": "", "nullable": ""}
                )
                # Add table metadata rows
                data.append(
                    {
                        "col_name": "# Detailed Table Information",
                        "data_type": "",
                        "comment": "",
                        "nullable": "",
                    }
                )
                data.append(
                    {
                        "col_name": "Name",
                        "data_type": table_name,
                        "comment": "",
                        "nullable": "",
                    }
                )
                data.append(
                    {
                        "col_name": "Type",
                        "data_type": "MANAGED",
                        "comment": "",
                        "nullable": "",
                    }
                )
                data.append(
                    {
                        "col_name": "Provider",
                        "data_type": "sparkless",
                        "comment": "",
                        "nullable": "",
                    }
                )

                result_schema = StructType(
                    [
                        StructField("col_name", StringType()),
                        StructField("data_type", StringType()),
                        StructField("comment", StringType()),
                        StructField("nullable", StringType()),
                    ]
                )
            else:
                result_schema = StructType(
                    [
                        StructField("col_name", StringType()),
                        StructField("data_type", StringType()),
                    ]
                )

        return cast("IDataFrame", DataFrame(data, result_schema))

    def _execute_merge(self, ast: SQLAST) -> IDataFrame:
        """Execute MERGE INTO query with complex pattern support.

        Supports:
        - Multiple WHEN MATCHED clauses with conditions (first match wins)
        - WHEN NOT MATCHED BY SOURCE clause
        - Complex expressions in SET clauses

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame (MERGE returns no results).
        """
        from ...dataframe import DataFrame
        from ...spark_types import StructType
        from typing import cast, Set

        # Extract components
        target_table = ast.components.get("target_table", "")
        source_table = ast.components.get("source_table", "")
        on_condition = ast.components.get("on_condition", "")
        target_alias: str = ast.components.get("target_alias") or ""
        source_alias: str = ast.components.get("source_alias") or ""
        when_matched = ast.components.get("when_matched", [])
        when_not_matched = ast.components.get("when_not_matched", [])
        when_not_matched_by_source = ast.components.get(
            "when_not_matched_by_source", []
        )

        # Parse table names (schema.table)
        if "." in target_table:
            target_schema, target_name = target_table.split(".", 1)
        else:
            target_schema, target_name = "default", target_table

        # Get target and source data
        target_df = self.session.table(target_table)
        target_data = target_df.collect()

        source_df = self.session.table(source_table)
        source_data = source_df.collect()
        source_data_list = [row.asDict() for row in source_data]

        # Parse ON condition - simple equality for now
        # Example: "t.id = s.id" or "t.id = s.id AND t.category = s.category"
        condition_parts = []
        for part in on_condition.split(" AND "):
            part = part.strip()
            match = re.match(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", part)
            if match:
                condition_parts.append(
                    {
                        "left_alias": match.group(1),
                        "left_col": match.group(2),
                        "right_alias": match.group(3),
                        "right_col": match.group(4),
                    }
                )

        # Track which rows were processed
        matched_target_ids: Set[int] = set()
        deleted_target_ids: Set[int] = set()
        matched_source_indices: Set[int] = set()
        updated_rows: List[Dict[str, Any]] = []

        # Process WHEN MATCHED clauses (first matching clause wins)
        if when_matched:
            for target_row in target_data:
                target_dict = target_row.asDict()
                row_processed = False

                # Check if this target row matches any source row
                for source_idx, source_dict in enumerate(source_data_list):
                    if row_processed:
                        break

                    matches = all(
                        target_dict.get(cond["left_col"])
                        == source_dict.get(cond["right_col"])
                        for cond in condition_parts
                    )

                    if matches:
                        matched_target_ids.add(id(target_row))
                        matched_source_indices.add(source_idx)

                        # Find first matching WHEN MATCHED clause (first match wins)
                        for clause in when_matched:
                            clause_condition = clause.get("condition")

                            # Check if this clause's condition matches
                            if clause_condition:
                                condition_matches = self._evaluate_merge_condition(
                                    clause_condition,
                                    target_dict,
                                    source_dict,
                                    target_alias,
                                    source_alias,
                                )
                                if not condition_matches:
                                    continue

                            # This clause matches - execute its action
                            if clause["action"] == "UPDATE":
                                updated_row = self._apply_merge_update(
                                    clause,
                                    target_dict,
                                    source_dict,
                                    target_alias,
                                    source_alias,
                                )
                                updated_rows.append(updated_row)
                                row_processed = True
                                break
                            elif clause["action"] == "DELETE":
                                deleted_target_ids.add(id(target_row))
                                row_processed = True
                                break

                        # If no clause matched, row is unchanged
                        if not row_processed:
                            updated_rows.append(target_dict.copy())
                            row_processed = True

                        break  # Only match first source row for WHEN MATCHED

        # Process WHEN NOT MATCHED BY SOURCE clauses (rows in target not in source)
        if when_not_matched_by_source:
            for target_row in target_data:
                if id(target_row) in matched_target_ids:
                    continue  # Skip already matched rows

                target_dict = target_row.asDict()
                row_processed = False

                # Find first matching clause
                for clause in when_not_matched_by_source:
                    clause_condition = clause.get("condition")

                    # Check if this clause's condition matches
                    if clause_condition:
                        condition_matches = self._evaluate_merge_condition(
                            clause_condition,
                            target_dict,
                            {},  # No source row
                            target_alias,
                            source_alias,
                        )
                        if not condition_matches:
                            continue

                    # This clause matches - execute its action
                    if clause["action"] == "DELETE":
                        deleted_target_ids.add(id(target_row))
                        row_processed = True
                        break
                    elif clause["action"] == "UPDATE":
                        updated_row = self._apply_merge_update(
                            clause,
                            target_dict,
                            {},  # No source row
                            target_alias,
                            source_alias,
                        )
                        updated_rows.append(updated_row)
                        row_processed = True
                        break

                # If no clause matched, add unchanged row
                # Only add if not already in updated_rows from WHEN MATCHED
                if not row_processed and id(target_row) not in matched_target_ids:
                    updated_rows.append(target_dict.copy())

        # Add unmatched target rows (unchanged) if not processed by NOT MATCHED BY SOURCE
        for target_row in target_data:
            row_id = id(target_row)
            if (
                row_id not in matched_target_ids
                and row_id not in deleted_target_ids
                and not when_not_matched_by_source
            ):
                updated_rows.append(target_row.asDict())

        # Process WHEN NOT MATCHED clauses (inserts for source rows not in target)
        if when_not_matched:
            for source_idx, source_dict in enumerate(source_data_list):
                if source_idx in matched_source_indices:
                    continue  # Skip already matched source rows

                # Check if this source row matches any target row
                matched = False
                for target_row in target_data:
                    target_dict = target_row.asDict()
                    matches = all(
                        target_dict.get(cond["left_col"])
                        == source_dict.get(cond["right_col"])
                        for cond in condition_parts
                    )
                    if matches:
                        matched = True
                        break

                if not matched:
                    # Find first matching WHEN NOT MATCHED clause
                    for clause in when_not_matched:
                        clause_condition = clause.get("condition")

                        # Check if this clause's condition matches
                        if clause_condition:
                            condition_matches = self._evaluate_merge_condition(
                                clause_condition,
                                {},  # No target row
                                source_dict,
                                target_alias,
                                source_alias,
                            )
                            if not condition_matches:
                                continue

                        if clause["action"] == "INSERT":
                            # Simple parsing: just insert all source columns
                            updated_rows.append(source_dict.copy())
                            break

        # Write merged data back to target table
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        storage.drop_table(target_schema, target_name)
        storage.create_table(target_schema, target_name, target_df.schema.fields)
        if updated_rows:
            storage.insert_data(target_schema, target_name, updated_rows)

        # MERGE returns empty DataFrame
        return cast("IDataFrame", DataFrame([], StructType([])))

    def _evaluate_merge_condition(
        self,
        condition: str,
        target_dict: Dict[str, Any],
        source_dict: Dict[str, Any],
        target_alias: str,
        source_alias: str,
    ) -> bool:
        """Evaluate a MERGE clause condition.

        Args:
            condition: The condition string (e.g., "s.updated_at > t.updated_at").
            target_dict: Target row as dictionary.
            source_dict: Source row as dictionary.
            target_alias: Alias for target table (e.g., 't').
            source_alias: Alias for source table (e.g., 's').

        Returns:
            True if condition matches, False otherwise.
        """
        # Build context for evaluation
        context: Dict[str, Any] = {}

        # Add target columns with alias prefix
        if target_alias:
            for col, val in target_dict.items():
                context[f"{target_alias}_{col}"] = val
                context[f"{target_alias}.{col}"] = val
        context.update(target_dict)

        # Add source columns with alias prefix
        if source_alias:
            for col, val in source_dict.items():
                context[f"{source_alias}_{col}"] = val
                context[f"{source_alias}.{col}"] = val
        context.update(source_dict)

        # Replace alias.column with actual values for comparison
        # Handle patterns like s.updated_at > t.updated_at or s.status = 'deleted'

        # Simple comparison patterns
        # Pattern 1: alias.col op alias.col (e.g., s.updated_at > t.updated_at)
        compare_match = re.match(
            r"(\w+)\.(\w+)\s*(=|!=|<>|>|<|>=|<=)\s*(\w+)\.(\w+)",
            condition.strip(),
            re.IGNORECASE,
        )
        if compare_match:
            left_alias, left_col, op, right_alias, right_col = compare_match.groups()
            left_val = None
            right_val = None

            # Get left value
            if left_alias == target_alias:
                left_val = target_dict.get(left_col)
            elif left_alias == source_alias:
                left_val = source_dict.get(left_col)

            # Get right value
            if right_alias == target_alias:
                right_val = target_dict.get(right_col)
            elif right_alias == source_alias:
                right_val = source_dict.get(right_col)

            if left_val is None or right_val is None:
                return False

            return self._compare_values(left_val, op, right_val)

        # Pattern 2: alias.col op 'string' (e.g., s.status = 'deleted')
        string_match = re.match(
            r"(\w+)\.(\w+)\s*(=|!=|<>)\s*['\"]([^'\"]*)['\"]",
            condition.strip(),
            re.IGNORECASE,
        )
        if string_match:
            alias, col, op, value = string_match.groups()
            col_val = None

            if alias == target_alias:
                col_val = target_dict.get(col)
            elif alias == source_alias:
                col_val = source_dict.get(col)

            if col_val is None:
                return False

            return self._compare_values(str(col_val), op, value)

        # Pattern 3: col = 'string' (without alias, for NOT MATCHED BY SOURCE)
        simple_string_match = re.match(
            r"(\w+)\s*(=|!=|<>)\s*['\"]([^'\"]*)['\"]",
            condition.strip(),
            re.IGNORECASE,
        )
        if simple_string_match:
            col, op, value = simple_string_match.groups()
            col_val = target_dict.get(col) or source_dict.get(col)

            if col_val is None:
                return False

            return self._compare_values(str(col_val), op, value)

        # Default: try basic evaluation (fallback)
        return True

    def _compare_values(self, left: Any, op: str, right: Any) -> bool:
        """Compare two values with the given operator.

        Args:
            left: Left value.
            op: Comparison operator.
            right: Right value.

        Returns:
            Comparison result.
        """
        try:
            if op == "=":
                return bool(left == right)
            elif op in ("!=", "<>"):
                return bool(left != right)
            elif op == ">":
                return bool(left > right)
            elif op == "<":
                return bool(left < right)
            elif op == ">=":
                return bool(left >= right)
            elif op == "<=":
                return bool(left <= right)
        except (TypeError, ValueError):
            pass
        return False

    def _apply_merge_update(
        self,
        clause: Dict[str, Any],
        target_dict: Dict[str, Any],
        source_dict: Dict[str, Any],
        target_alias: str,
        source_alias: str,
    ) -> Dict[str, Any]:
        """Apply UPDATE SET clause to a target row.

        Handles:
        - Simple column assignment: t.col = s.col
        - Expressions: t.version = t.version + 1
        - Function calls: t.updated_at = current_timestamp()

        Args:
            clause: The WHEN MATCHED UPDATE clause.
            target_dict: Target row as dictionary.
            source_dict: Source row as dictionary.
            target_alias: Alias for target table.
            source_alias: Alias for source table.

        Returns:
            Updated row dictionary.
        """
        updated_row = target_dict.copy()

        # Get assignments from parsed clause
        assignments = clause.get("assignments", [])

        if not assignments:
            # Fallback: parse set_clause directly
            set_clause = clause.get("set_clause", "")
            for assignment in set_clause.split(","):
                assignment = assignment.strip()
                self._apply_single_assignment(
                    assignment,
                    updated_row,
                    target_dict,
                    source_dict,
                    target_alias,
                    source_alias,
                )
        else:
            for assignment in assignments:
                target_col = assignment.get("target", "")
                value_expr = assignment.get("value", "")
                self._apply_assignment_value(
                    target_col,
                    value_expr,
                    updated_row,
                    target_dict,
                    source_dict,
                    target_alias,
                    source_alias,
                )

        return updated_row

    def _apply_single_assignment(
        self,
        assignment: str,
        updated_row: Dict[str, Any],
        target_dict: Dict[str, Any],
        source_dict: Dict[str, Any],
        target_alias: str,
        source_alias: str,
    ) -> None:
        """Apply a single SET assignment.

        Args:
            assignment: Assignment string like 't.col = s.col'.
            updated_row: Row to update (modified in place).
            target_dict: Original target row.
            source_dict: Source row.
            target_alias: Target alias.
            source_alias: Source alias.
        """
        # Parse assignment
        parts = assignment.split("=", 1)
        if len(parts) != 2:
            return

        target_col_expr = parts[0].strip()
        value_expr = parts[1].strip()

        self._apply_assignment_value(
            target_col_expr,
            value_expr,
            updated_row,
            target_dict,
            source_dict,
            target_alias,
            source_alias,
        )

    def _apply_assignment_value(
        self,
        target_col_expr: str,
        value_expr: str,
        updated_row: Dict[str, Any],
        target_dict: Dict[str, Any],
        source_dict: Dict[str, Any],
        target_alias: str,
        source_alias: str,
    ) -> None:
        """Apply an assignment value to the updated row.

        Args:
            target_col_expr: Target column expression (e.g., 't.col').
            value_expr: Value expression (e.g., 's.col', 't.col + 1').
            updated_row: Row to update (modified in place).
            target_dict: Original target row.
            source_dict: Source row.
            target_alias: Target alias.
            source_alias: Source alias.
        """
        # Extract target column name
        target_col = target_col_expr
        if "." in target_col_expr:
            target_col = target_col_expr.split(".", 1)[1]

        # Evaluate value expression
        value = self._evaluate_merge_expression(
            value_expr, target_dict, source_dict, target_alias, source_alias
        )

        updated_row[target_col] = value

    def _evaluate_merge_expression(
        self,
        expr: str,
        target_dict: Dict[str, Any],
        source_dict: Dict[str, Any],
        target_alias: str,
        source_alias: str,
    ) -> Any:
        """Evaluate a MERGE expression.

        Handles:
        - Simple column reference: s.col, t.col
        - Arithmetic expressions: t.version + 1
        - Function calls: current_timestamp()

        Args:
            expr: Expression string.
            target_dict: Target row.
            source_dict: Source row.
            target_alias: Target alias.
            source_alias: Source alias.

        Returns:
            Evaluated value.
        """
        expr = expr.strip()

        # Pattern 1: Simple column reference (alias.col)
        col_match = re.match(r"(\w+)\.(\w+)$", expr)
        if col_match:
            alias, col = col_match.groups()
            if alias == source_alias:
                return source_dict.get(col)
            elif alias == target_alias:
                return target_dict.get(col)
            return None

        # Pattern 2: Arithmetic expression (alias.col + number or alias.col - number)
        arith_match = re.match(r"(\w+)\.(\w+)\s*([+\-*/])\s*(\d+)$", expr)
        if arith_match:
            alias, col, op, num_str = arith_match.groups()
            num = int(num_str)

            val = None
            if alias == source_alias:
                val = source_dict.get(col)
            elif alias == target_alias:
                val = target_dict.get(col)

            if val is not None:
                try:
                    if op == "+":
                        return val + num
                    elif op == "-":
                        return val - num
                    elif op == "*":
                        return val * num
                    elif op == "/":
                        return val / num if num != 0 else val
                except (TypeError, ValueError):
                    pass
            return val

        # Pattern 3: Function call (e.g., current_timestamp())
        func_match = re.match(r"(\w+)\(\)$", expr, re.IGNORECASE)
        if func_match:
            func_name = func_match.group(1).lower()
            if func_name == "current_timestamp":
                from datetime import datetime

                return datetime.now().isoformat()
            elif func_name == "current_date":
                from datetime import date

                return date.today().isoformat()
            return None

        # Pattern 4: Literal string
        string_match = re.match(r"['\"]([^'\"]*)['\"]$", expr)
        if string_match:
            return string_match.group(1)

        # Pattern 5: Literal number
        try:
            if "." in expr:
                return float(expr)
            return int(expr)
        except ValueError:
            pass

        # Fallback: return the expression as-is
        return expr
