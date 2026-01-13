"""
Expression functions for Sparkless.

This module provides the F namespace functions and expression utilities
for creating column expressions and transformations.
"""

from typing import Any, List, TYPE_CHECKING, Union
from .column import Column, ColumnOperation
from .literals import Literal

if TYPE_CHECKING:
    from ..conditional import CaseWhen
else:
    CaseWhen = Any


class ExpressionFunctions:
    """Expression functions for creating column expressions."""

    @staticmethod
    def _require_active_session(operation_name: str) -> None:
        """Require an active SparkSession for the operation.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        from sparkless.session.core.session import SparkSession

        # Use getActiveSession() for PySpark compatibility
        if SparkSession.getActiveSession() is None:
            raise RuntimeError(
                f"Cannot perform {operation_name}: "
                "No active SparkSession found. "
                "This operation requires an active SparkSession, similar to PySpark. "
                "Create a SparkSession first: spark = SparkSession('app_name')"
            )

    @staticmethod
    def col(name: str) -> Column:
        """Create a column reference.

        Delegates to canonical Column constructor.

        Args:
            name: Column name.

        Returns:
            Column instance.

        Note:
            In PySpark, col() can be called without an active SparkSession.
            The column expression is evaluated later when used with a DataFrame.
        """
        return Column(name)

    @staticmethod
    def lit(value: Any) -> Literal:
        """Create a literal value.

        Delegates to canonical Literal constructor.

        Args:
            value: Literal value.

        Returns:
            Literal instance.

        Note:
            In PySpark, lit() can be called without an active SparkSession.
            The literal expression is evaluated later when used with a DataFrame.
        """
        return Literal(value)

    @staticmethod
    def when(condition: ColumnOperation, value: Any) -> "CaseWhen":
        """Start a CASE WHEN expression.

        Delegates to canonical CaseWhen constructor.

        Args:
            condition: Condition to evaluate.
            value: Value if condition is true.

        Returns:
            CaseWhen instance.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        ExpressionFunctions._require_active_session("CASE WHEN expression")
        from ..conditional import CaseWhen

        return CaseWhen(None, condition, value)

    @staticmethod
    def coalesce(
        *columns: Union[Column, ColumnOperation, str],
    ) -> ColumnOperation:
        """Return the first non-null value from a list of columns.

        Args:
            *columns: Columns to check for non-null values.

        Returns:
            ColumnOperation for coalesce.
        """
        col_refs: List[Union[Column, ColumnOperation]] = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(Column(col))
            else:
                col_refs.append(col)

        return ColumnOperation(None, "coalesce", col_refs)

    @staticmethod
    def isnull(column: Union[Column, str]) -> ColumnOperation:
        """Check if column value is null.

        Args:
            column: Column to check.

        Returns:
            ColumnOperation for isnull.
        """
        if isinstance(column, str):
            column = Column(column)
        return column.isnull()

    @staticmethod
    def isnotnull(column: Union[Column, str]) -> ColumnOperation:
        """Check if column value is not null.

        Args:
            column: Column to check.

        Returns:
            ColumnOperation for isnotnull.
        """
        if isinstance(column, str):
            column = Column(column)
        return column.isnotnull()

    @staticmethod
    def isnan(column: Union[Column, str]) -> ColumnOperation:
        """Check if column value is NaN.

        Args:
            column: Column to check.

        Returns:
            ColumnOperation for isnan.
        """
        if isinstance(column, str):
            column = Column(column)
        return ColumnOperation(column, "isnan", None)

    @staticmethod
    def isnotnan(column: Union[Column, str]) -> ColumnOperation:
        """Check if column value is not NaN.

        Args:
            column: Column to check.

        Returns:
            ColumnOperation for isnotnan.
        """
        if isinstance(column, str):
            column = Column(column)
        return ColumnOperation(column, "isnotnan", None)

    @staticmethod
    def expr(expr: str) -> Union[ColumnOperation, Column, "CaseWhen", "Literal"]:
        """Create a column expression from SQL string.

        Args:
            expr: SQL expression string (e.g., "id IS NOT NULL", "age > 18").
                  Must use SQL syntax, not Python expressions.

        Returns:
            ColumnOperation for the expression.

        Raises:
            RuntimeError: If no active SparkSession is available
            ParseException: If SQL syntax is invalid
        """
        ExpressionFunctions._require_active_session(f"expression '{expr}'")

        # Parse SQL expression instead of storing as raw string
        from .sql_expr_parser import SQLExprParser

        try:
            parsed = SQLExprParser.parse(expr)
            # If parsed result is a Column or ColumnOperation, return it
            if isinstance(parsed, (ColumnOperation, Column)):
                # Mark as coming from F.expr() for detection in materialization
                if isinstance(parsed, ColumnOperation):
                    setattr(parsed, "_from_expr", True)
                return parsed
            else:
                # Literal value or CaseWhen - wrap in ColumnOperation if needed
                from .literals import Literal

                if isinstance(parsed, Literal):
                    result = ColumnOperation(None, "lit", parsed)
                    setattr(result, "_from_expr", True)
                    return result
                # CaseWhen or other types - return as-is
                return parsed
        except Exception as e:
            from sparkless.core.exceptions.analysis import ParseException

            if isinstance(e, ParseException):
                raise
            # Fallback to old behavior if parsing fails (for backward compatibility)
            # But warn that this might not work correctly
            import warnings

            warnings.warn(
                f"Failed to parse SQL expression '{expr}'. "
                f"F.expr() should use SQL syntax (e.g., 'id IS NOT NULL'), "
                f"not Python expressions (e.g., \"col('id').isNotNull()\"). "
                f"Error: {str(e)}",
                UserWarning,
                stacklevel=2,
            )
            return ColumnOperation(None, "expr", expr)

    @staticmethod
    def array(*columns: Union[Column, str]) -> ColumnOperation:
        """Create an array from columns.

        Args:
            *columns: Columns to include in array.

        Returns:
            ColumnOperation for array.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(Column(col))
            else:
                col_refs.append(col)

        return ColumnOperation(None, "array", col_refs)

    @staticmethod
    def struct(*columns: Union[Column, str]) -> ColumnOperation:
        """Create a struct from columns.

        Args:
            *columns: Columns to include in struct.

        Returns:
            ColumnOperation for struct.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(Column(col))
            else:
                col_refs.append(col)

        return ColumnOperation(None, "struct", col_refs)

    @staticmethod
    def greatest(*columns: Union[Column, str]) -> ColumnOperation:
        """Return the greatest value among columns.

        Args:
            *columns: Columns to compare.

        Returns:
            ColumnOperation for greatest.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(Column(col))
            else:
                col_refs.append(col)

        return ColumnOperation(None, "greatest", col_refs)

    @staticmethod
    def least(*columns: Union[Column, str]) -> ColumnOperation:
        """Return the least value among columns.

        Args:
            *columns: Columns to compare.

        Returns:
            ColumnOperation for least.
        """
        col_refs = []
        for col in columns:
            if isinstance(col, str):
                col_refs.append(Column(col))
            else:
                col_refs.append(col)

        return ColumnOperation(None, "least", col_refs)

    @staticmethod
    def when_otherwise(
        condition: ColumnOperation, value: Any, otherwise: Any
    ) -> "CaseWhen":
        """Create a complete CASE WHEN expression.

        Args:
            condition: Condition to evaluate.
            value: Value if condition is true.
            otherwise: Default value.

        Returns:
            CaseWhen instance.
        """
        from ..conditional import CaseWhen

        case_when = CaseWhen(None, condition, value)
        return case_when.otherwise(otherwise)
