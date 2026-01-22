"""
Conditional functions for Sparkless.

This module contains conditional functions including CASE WHEN expressions.
"""

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, Union, cast
from sparkless.functions.base import Column, ColumnOperation
from sparkless.core.condition_evaluator import ConditionEvaluator
from sparkless.core.type_utils import get_expression_name

if TYPE_CHECKING:
    from sparkless.spark_types import DataType
    from sparkless.functions.aggregate import AggregateFunction


def validate_rule(
    column: Union[Column, str], rule: Union[str, List[Any]]
) -> ColumnOperation:
    """Convert validation rule to column expression.

    Args:
        column: The column to validate.
        rule: Validation rule as string or list.

    Returns:
        Column expression for the validation rule.

    Raises:
        ValueError: If rule is not recognized.
    """
    if isinstance(column, str):
        column = Column(column)

    if isinstance(rule, str):
        # String rules
        if rule == "not_null":
            return column.isNotNull()
        elif rule == "positive":
            return column > 0
        elif rule == "non_negative":
            return column >= 0
        elif rule == "negative":
            return column < 0
        elif rule == "non_positive":
            return column <= 0
        elif rule == "non_zero":
            return column != 0
        elif rule == "zero":
            return column == 0
        else:
            raise ValueError(f"Unknown string validation rule: {rule}")
    elif isinstance(rule, list):
        # List rules: ["operator", arg1, arg2, ...]
        if not rule:
            raise ValueError("Empty rule list")

        op = rule[0]
        if op == "gt":
            if len(rule) < 2:
                raise ValueError("gt rule requires a value")
            return cast("ColumnOperation", column > rule[1])
        elif op == "gte":
            if len(rule) < 2:
                raise ValueError("gte rule requires a value")
            return cast("ColumnOperation", column >= rule[1])
        elif op == "lt":
            if len(rule) < 2:
                raise ValueError("lt rule requires a value")
            return cast("ColumnOperation", column < rule[1])
        elif op == "lte":
            if len(rule) < 2:
                raise ValueError("lte rule requires a value")
            return cast("ColumnOperation", column <= rule[1])
        elif op == "eq":
            if len(rule) < 2:
                raise ValueError("eq rule requires a value")
            return cast("ColumnOperation", column == rule[1])
        elif op == "ne":
            if len(rule) < 2:
                raise ValueError("ne rule requires a value")
            return cast("ColumnOperation", column != rule[1])
        elif op == "between":
            if len(rule) < 3:
                raise ValueError("between rule requires two values")
            return column.between(rule[1], rule[2])
        elif op == "in":
            if len(rule) < 2:
                raise ValueError("in rule requires a list of values")
            return column.isin(rule[1])
        elif op == "not_in":
            if len(rule) < 2:
                raise ValueError("not_in rule requires a list of values")
            return ~column.isin(rule[1])
        elif op == "contains":
            if len(rule) < 2:
                raise ValueError("contains rule requires a value")
            return column.contains(rule[1])
        elif op == "starts_with":
            if len(rule) < 2:
                raise ValueError("starts_with rule requires a value")
            return column.startswith(rule[1])
        elif op == "ends_with":
            if len(rule) < 2:
                raise ValueError("ends_with rule requires a value")
            return column.endswith(rule[1])
        elif op == "regex":
            if len(rule) < 2:
                raise ValueError("regex rule requires a pattern")
            return column.rlike(rule[1])
        else:
            raise ValueError(f"Unknown list validation rule: {op}")
    else:
        raise ValueError(f"Unknown validation rule type: {type(rule)}")


class CaseWhen:
    """Represents a CASE WHEN expression.

    This class handles complex conditional logic with multiple conditions
    and default values, similar to SQL CASE WHEN statements.
    """

    def __init__(self, column: Any = None, condition: Any = None, value: Any = None):
        """Initialize CaseWhen.

        Args:
            column: The column or expression being evaluated.
            condition: The condition for this case.
            value: The value to return if condition is true.
        """
        self.column = column
        self.conditions: List[Tuple[Any, Any]] = []
        self.default_value: Any = None

        if condition is not None and value is not None:
            self.conditions.append((condition, value))

        # Generate a meaningful name from the condition and value
        # This will be updated later when otherwise() is called
        self.name = "CASE WHEN"

    @property
    def else_value(self) -> Any:
        """Get the else value (alias for default_value for compatibility)."""
        return self.default_value

    @else_value.setter
    def else_value(self, value: Any) -> None:
        """Set the else value (alias for default_value for compatibility)."""
        self.default_value = value

    def when(self, condition: Any, value: Any) -> "CaseWhen":
        """Add another WHEN condition.

        Args:
            condition: The condition to check.
            value: The value to return if condition is true.

        Returns:
            Self for method chaining.
        """
        self.conditions.append((condition, value))
        return self

    def otherwise(self, value: Any) -> "CaseWhen":
        """Set the default value for the CASE WHEN expression.

        Args:
            value: The default value to return if no conditions match.

        Returns:
            Self for method chaining.
        """
        self.default_value = value

        # Generate full SQL expression for the name
        # Format: CASE WHEN (condition) THEN value ELSE otherwise END
        if self.conditions:
            condition, then_value = self.conditions[0]
            condition_str = (
                str(condition) if hasattr(condition, "__str__") else str(condition)
            )
            name = f"CASE WHEN ({condition_str}) THEN {then_value} ELSE {value} END"
            self.name = name

        return self

    def alias(self, name: str) -> "CaseWhen":
        """Create an alias for the CASE WHEN expression.

        Args:
            name: The alias name.

        Returns:
            Self for method chaining.
        """
        self.name = name
        return self

    def cast(self, data_type: Any) -> ColumnOperation:
        """Cast the CASE WHEN expression to a different data type.

        Args:
            data_type: The target data type (DataType instance or string type name).

        Returns:
            ColumnOperation representing the cast operation.

        Example:
            >>> F.when(F.col("value") == "A", F.lit(100)).otherwise(F.lit(200)).cast("long")
        """
        return ColumnOperation(self, "cast", data_type)

    def _create_operation(self, operation: str, other: Any) -> ColumnOperation:
        """Create a ColumnOperation with the given operation and other operand.

        Args:
            operation: The operation to perform (e.g., "+", "-", "|", etc.)
            other: The other operand

        Returns:
            ColumnOperation instance
        """
        return ColumnOperation(self, operation, other)

    def __add__(self, other: Any) -> ColumnOperation:
        """Addition operation (PySpark-compatible)."""
        return self._create_operation("+", other)

    def __sub__(self, other: Any) -> ColumnOperation:
        """Subtraction operation (PySpark-compatible)."""
        return self._create_operation("-", other)

    def __mul__(self, other: Any) -> ColumnOperation:
        """Multiplication operation (PySpark-compatible)."""
        return self._create_operation("*", other)

    def __truediv__(self, other: Any) -> ColumnOperation:
        """Division operation (PySpark-compatible)."""
        return self._create_operation("/", other)

    def __mod__(self, other: Any) -> ColumnOperation:
        """Modulo operation (PySpark-compatible)."""
        return self._create_operation("%", other)

    def __radd__(self, other: Any) -> ColumnOperation:
        """Reverse addition operation (for `2 + case_when`)."""
        # For commutative operations, we can just swap operands
        return self._create_operation("+", other)

    def __rsub__(self, other: Any) -> ColumnOperation:
        """Reverse subtraction operation (for `2 - case_when`)."""
        # For non-commutative operations, create ColumnOperation with literal as left operand
        return ColumnOperation(other, "-", self)

    def __rmul__(self, other: Any) -> ColumnOperation:
        """Reverse multiplication operation (for `2 * case_when`)."""
        # For commutative operations, we can just swap operands
        return self._create_operation("*", other)

    def __rtruediv__(self, other: Any) -> ColumnOperation:
        """Reverse division operation (for `2 / case_when`)."""
        # For non-commutative operations, create ColumnOperation with literal as left operand
        return ColumnOperation(other, "/", self)

    def __rmod__(self, other: Any) -> ColumnOperation:
        """Reverse modulo operation (for `2 % case_when`)."""
        # For non-commutative operations, create ColumnOperation with literal as left operand
        return ColumnOperation(other, "%", self)

    def __or__(self, other: Any) -> ColumnOperation:
        """Bitwise OR operation (PySpark-compatible)."""
        return self._create_operation("|", other)

    def __and__(self, other: Any) -> ColumnOperation:
        """Bitwise AND operation (PySpark-compatible)."""
        return self._create_operation("&", other)

    def __invert__(self) -> ColumnOperation:
        """Bitwise NOT operation (unary ~, PySpark-compatible)."""
        return ColumnOperation(self, "~", None)

    def evaluate(self, row: Dict[str, Any]) -> Any:
        """Evaluate the CASE WHEN expression for a given row.

        Args:
            row: The data row to evaluate against.

        Returns:
            The evaluated result.
        """
        # Evaluate conditions in order
        for condition, value in self.conditions:
            if self._evaluate_condition(row, condition):
                return self._evaluate_value(row, value)

        # Return default value if no condition matches
        return self._evaluate_value(row, self.default_value)

    def get_result_type(self) -> "DataType":
        """Infer the result type from condition values."""
        from ..spark_types import (
            BooleanType,
            IntegerType,
            StringType,
            DoubleType,
            LongType,
        )
        from .core.literals import Literal

        # Check all condition values and default value
        all_values = [v for _, v in self.conditions]
        if self.default_value is not None:
            all_values.append(self.default_value)

        # Check if all values are literals (which are never nullable)

        all_literals = all(
            isinstance(val, Literal) or val is None for val in all_values
        )

        for val in all_values:
            if val is not None:
                if isinstance(val, Literal):
                    # For Literal, create a new instance with correct nullable
                    data_type = val.data_type
                    if isinstance(data_type, BooleanType):
                        return BooleanType(
                            nullable=False
                        )  # Literals are never nullable
                    elif isinstance(data_type, IntegerType):
                        return IntegerType(
                            nullable=False
                        )  # Literals are never nullable
                    elif isinstance(data_type, DoubleType):
                        return DoubleType(nullable=False)  # Literals are never nullable
                    elif isinstance(data_type, StringType):
                        return StringType(nullable=False)  # Literals are never nullable
                    else:
                        # For other types, create with correct nullable
                        return data_type.__class__(
                            nullable=False
                        )  # Literals are never nullable
                elif isinstance(val, bool):
                    return BooleanType(nullable=False)  # Literals are never nullable
                elif isinstance(val, int):
                    return IntegerType(nullable=False)  # Literals are never nullable
                elif isinstance(val, float):
                    return DoubleType(nullable=False)  # Literals are never nullable
                elif isinstance(val, str):
                    return StringType(nullable=False)  # Literals are never nullable
                elif hasattr(val, "operation") and hasattr(val, "column"):
                    # Handle ColumnOperation - check the operation type
                    if val.operation in ["+", "-", "*", "/", "%", "abs"]:
                        # Arithmetic operations return LongType
                        return LongType(nullable=False)
                    elif val.operation in ["round"]:
                        # Round operations return DoubleType
                        return DoubleType(nullable=False)
                    else:
                        # Default to StringType for other operations
                        return StringType(nullable=False)

        # Default to LongType for arithmetic operations, not BooleanType
        return LongType(nullable=not all_literals)

    def _evaluate_condition(self, row: Dict[str, Any], condition: Any) -> bool:
        """Evaluate a condition for a given row.

        Delegates to shared ConditionEvaluator for consistency.

        Args:
            row: The data row to evaluate against.
            condition: The condition to evaluate.

        Returns:
            True if condition is met, False otherwise.
        """
        from sparkless.core.condition_evaluator import ConditionEvaluator

        result = ConditionEvaluator.evaluate_condition(row, condition)
        return bool(result)

    def _evaluate_value(self, row: Dict[str, Any], value: Any) -> Any:
        """Evaluate a value for a given row.

        Args:
            row: The data row to evaluate against.
            value: The value to evaluate.

        Returns:
            The evaluated value.
        """
        from .core.literals import Literal

        if isinstance(value, Literal):
            # For Literal, return the actual value
            return value.value
        elif hasattr(value, "operation") and hasattr(value, "column"):
            # Handle ColumnOperation (e.g., unary minus, arithmetic operations)
            from sparkless.functions.base import ColumnOperation

            if isinstance(value, ColumnOperation):
                return self._evaluate_column_operation_value(row, value)
        elif hasattr(value, "name"):
            return row.get(value.name)
        elif hasattr(value, "value"):
            return value.value
        else:
            return value

    def _evaluate_column_operation_value(
        self, row: Dict[str, Any], operation: Any
    ) -> Any:
        """Evaluate a column operation for a value.

        Args:
            row: The data row.
            operation: The column operation to evaluate.

        Returns:
            The evaluated result.
        """
        if operation.operation == "-" and operation.value is None:
            # Unary minus operation
            left_value = ConditionEvaluator._get_column_value(row, operation.column)
            if left_value is None:
                return None
            return -left_value
        elif operation.operation == "+" and operation.value is None:
            # Unary plus operation (just return the value)
            return ConditionEvaluator._get_column_value(row, operation.column)
        elif operation.operation in ["+", "-", "*", "/", "%"]:
            # Binary arithmetic operations
            left_value = ConditionEvaluator._get_column_value(row, operation.column)
            right_value = ConditionEvaluator._get_column_value(row, operation.value)

            if left_value is None or right_value is None:
                return None

            if operation.operation == "+":
                return left_value + right_value
            elif operation.operation == "-":
                return left_value - right_value
            elif operation.operation == "*":
                return left_value * right_value
            elif operation.operation == "/":
                return left_value / right_value if right_value != 0 else None
            elif operation.operation == "%":
                return left_value % right_value if right_value != 0 else None
        else:
            # For other operations, try to get the column value
            return ConditionEvaluator._get_column_value(row, operation.column)


class ConditionalFunctions:
    """Collection of conditional functions."""

    @staticmethod
    def coalesce(*columns: Union[Column, str, Any]) -> ColumnOperation:
        """Return the first non-null value from a list of columns.

        Args:
            *columns: Variable number of columns or values to check.

        Returns:
            ColumnOperation representing the coalesce function.
        """
        # Convert string columns to Column objects
        mock_columns = []
        for col in columns:
            if isinstance(col, str):
                mock_columns.append(Column(col))
            else:
                mock_columns.append(col)

        # Create operation with first column as base
        operation = ColumnOperation(mock_columns[0], "coalesce", mock_columns[1:])
        # Generate column name, handling Literals specially
        # get_expression_name is imported at module level

        name_parts = [get_expression_name(c) for c in mock_columns]
        operation.name = f"coalesce({', '.join(name_parts)})"
        return operation

    @staticmethod
    def isnull(column: Union[Column, str]) -> ColumnOperation:
        """Check if a column is null.

        Args:
            column: The column to check.

        Returns:
            ColumnOperation representing the isnull function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "isnull", name=f"({column.name} IS NULL)")
        return operation

    @staticmethod
    def isnotnull(column: Union[Column, str]) -> ColumnOperation:
        """Check if a column is not null.

        Args:
            column: The column to check.

        Returns:
            ColumnOperation representing the isnotnull function.
        """
        if isinstance(column, str):
            column = Column(column)

        # PySpark's isnotnull is implemented as ~isnull, so it generates (NOT (column IS NULL))
        operation = ColumnOperation(
            column, "isnotnull", name=f"(NOT ({column.name} IS NULL))"
        )
        return operation

    @staticmethod
    def isnan(column: Union[Column, str]) -> ColumnOperation:
        """Check if a column is NaN (Not a Number).

        Args:
            column: The column to check.

        Returns:
            ColumnOperation representing the isnan function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "isnan")
        operation.name = f"isnan({column.name})"
        return operation

    @staticmethod
    def when(condition: Any, value: Any = None) -> CaseWhen:
        """Start a CASE WHEN expression.

        Args:
            condition: The initial condition.
            value: Optional value for the condition.

        Returns:
            CaseWhen object for chaining.
        """
        if value is not None:
            return CaseWhen(condition=condition, value=value)
        return CaseWhen(condition=condition)

    @staticmethod
    def assert_true(
        condition: Union[Column, ColumnOperation, str],  # str may be passed at runtime
    ) -> ColumnOperation:
        """Assert that a condition is true, raises error if false.

        Args:
            condition: Boolean condition to assert.

        Returns:
            ColumnOperation representing the assert_true function.

        Example:
            >>> df.select(F.assert_true(F.col("value") > 0))
        """
        from sparkless.functions import Column, ColumnOperation
        from sparkless.core.type_utils import (
            is_column,
            is_column_operation,
            get_expression_name,
        )

        if is_column(condition):
            col = condition
            value: Optional[ColumnOperation] = None
        elif is_column_operation(condition):
            # Type guard narrows condition to ColumnOperation
            # Cast to help mypy understand the type narrowing in Python 3.9
            col_op = cast("ColumnOperation", condition)  # type: ignore[redundant-cast,unused-ignore]
            col = col_op.column
            value = col_op
        elif isinstance(condition, str):
            col = Column(condition)
            value = None
        else:
            # This branch should not be reached due to type annotation
            # Union[Column, ColumnOperation, str] is exhaustive
            assert False, f"Unexpected condition type: {type(condition)}"

        name_str = (
            get_expression_name(condition)
            if not isinstance(condition, str)
            else condition
        )

        # col is guaranteed to be a Column after the if/elif/elif branches
        # No need for additional isinstance check

        return ColumnOperation(
            col,
            "assert_true",
            value,
            name=f"assert_true({name_str})",
        )

    # Priority 2: Conditional/Null Functions
    @staticmethod
    def ifnull(col1: Union[Column, str], col2: Union[Column, str]) -> ColumnOperation:
        """Alias for coalesce(col1, col2) - Returns col2 if col1 is null (PySpark 3.5+).

        Args:
            col1: First column.
            col2: Second column (replacement for null).

        Returns:
            ColumnOperation representing the ifnull function.
        """
        return ConditionalFunctions.coalesce(col1, col2)

    @staticmethod
    def equal_null(
        col1: Union[Column, str], col2: Union[Column, str, Any]
    ) -> ColumnOperation:
        """Equality check that treats NULL as equal.

        Args:
            col1: First column or value.
            col2: Second column or value.

        Returns:
            ColumnOperation representing the equal_null function.
        """
        if isinstance(col1, str):
            col1 = Column(col1)
        if isinstance(col2, str):
            col2 = Column(col2)

        operation = ColumnOperation(
            col1,
            "equal_null",
            col2,
            name=f"equal_null({col1.name}, {col2.name if hasattr(col2, 'name') else col2})",
        )
        return operation

    @staticmethod
    def nullif(col1: Union[Column, str], col2: Any) -> ColumnOperation:
        """Returns null if col1 equals col2, otherwise returns col1 (PySpark 3.5+).

        Args:
            col1: First column.
            col2: Column, column name, or literal value to compare.

        Returns:
            ColumnOperation representing the nullif function.
        """
        from typing import Union, Any
        from ..functions.core.literals import Literal

        column1 = Column(col1) if isinstance(col1, str) else col1

        # col2 can be a column, column name, or literal value
        column2: Union[Literal, Column, Any]
        if isinstance(col2, (int, float, bool, type(None))):
            # It's a literal value
            column2 = Literal(col2)
        elif isinstance(col2, str):
            # It's a column name (str not in literal tuple above)
            column2 = Column(col2)
        else:
            # It's already a Column or ColumnOperation
            column2 = col2

        # Get proper name for the column expression
        col2_name = column2.name if hasattr(column2, "name") else str(column2)

        # Use NULLIF function for DuckDB (DuckDB backend only)
        return ColumnOperation(
            column1,
            "nullif",
            value=column2,
            name=f"nullif({column1.name}, {col2_name})",
        )

    @staticmethod
    def case_when(*conditions: Tuple[Any, Any], else_value: Any = None) -> CaseWhen:
        """Create CASE WHEN expression with multiple conditions.

        Args:
            *conditions: Variable number of (condition, value) tuples.
            else_value: Default value if no conditions match.

        Returns:
            CaseWhen object representing the CASE WHEN expression.

        Example:
            >>> F.case_when(
            ...     (F.col("age") > 18, "adult"),
            ...     (F.col("age") > 12, "teen"),
            ...     else_value="child"
            ... )
        """
        if not conditions:
            raise ValueError("At least one condition must be provided")

        # Create CaseWhen with the first condition
        first_condition, first_value = conditions[0]
        case_when = CaseWhen(condition=first_condition, value=first_value)

        # Add remaining conditions
        for condition, value in conditions[1:]:
            case_when.when(condition, value)

        # Set default value if provided
        if else_value is not None:
            case_when.otherwise(else_value)

        return case_when

    @staticmethod
    def try_add(
        left: Union[Column, str, int, float], right: Union[Column, str, int, float]
    ) -> ColumnOperation:
        """Null-safe addition - returns NULL on error (PySpark 3.5+).

        Args:
            left: Left operand (column or literal).
            right: Right operand (column or literal).

        Returns:
            ColumnOperation representing the try_add function.
        """
        from sparkless.functions.base import Column

        if isinstance(left, (str, int, float)):
            left = Column(str(left)) if isinstance(left, (int, float)) else Column(left)
        if isinstance(right, (str, int, float)):
            right = (
                Column(str(right)) if isinstance(right, (int, float)) else Column(right)
            )

        operation = ColumnOperation(
            left,
            "try_add",
            value=right,
            name=f"try_add({left.name}, {right.name if hasattr(right, 'name') else right})",
        )
        return operation

    @staticmethod
    def try_subtract(
        left: Union[Column, str, int, float], right: Union[Column, str, int, float]
    ) -> ColumnOperation:
        """Null-safe subtraction - returns NULL on error (PySpark 3.5+).

        Args:
            left: Left operand (column or literal).
            right: Right operand (column or literal).

        Returns:
            ColumnOperation representing the try_subtract function.
        """
        from sparkless.functions.base import Column

        if isinstance(left, (str, int, float)):
            left = Column(str(left)) if isinstance(left, (int, float)) else Column(left)
        if isinstance(right, (str, int, float)):
            right = (
                Column(str(right)) if isinstance(right, (int, float)) else Column(right)
            )

        operation = ColumnOperation(
            left,
            "try_subtract",
            value=right,
            name=f"try_subtract({left.name}, {right.name if hasattr(right, 'name') else right})",
        )
        return operation

    @staticmethod
    def try_multiply(
        left: Union[Column, str, int, float], right: Union[Column, str, int, float]
    ) -> ColumnOperation:
        """Null-safe multiplication - returns NULL on error (PySpark 3.5+).

        Args:
            left: Left operand (column or literal).
            right: Right operand (column or literal).

        Returns:
            ColumnOperation representing the try_multiply function.
        """
        from sparkless.functions.base import Column

        if isinstance(left, (str, int, float)):
            left = Column(str(left)) if isinstance(left, (int, float)) else Column(left)
        if isinstance(right, (str, int, float)):
            right = (
                Column(str(right)) if isinstance(right, (int, float)) else Column(right)
            )

        operation = ColumnOperation(
            left,
            "try_multiply",
            value=right,
            name=f"try_multiply({left.name}, {right.name if hasattr(right, 'name') else right})",
        )
        return operation

    @staticmethod
    def try_divide(
        left: Union[Column, str, int, float], right: Union[Column, str, int, float]
    ) -> ColumnOperation:
        """Null-safe division - returns NULL on error (PySpark 3.5+).

        Args:
            left: Left operand (column or literal).
            right: Right operand (column or literal).

        Returns:
            ColumnOperation representing the try_divide function.
        """
        from sparkless.functions.base import Column

        if isinstance(left, (str, int, float)):
            left = Column(str(left)) if isinstance(left, (int, float)) else Column(left)
        if isinstance(right, (str, int, float)):
            right = (
                Column(str(right)) if isinstance(right, (int, float)) else Column(right)
            )

        operation = ColumnOperation(
            left,
            "try_divide",
            value=right,
            name=f"try_divide({left.name}, {right.name if hasattr(right, 'name') else right})",
        )
        return operation

    @staticmethod
    def try_sum(column: Union[Column, str]) -> "AggregateFunction":
        """Null-safe sum aggregate - returns NULL on error (PySpark 3.5+).

        Args:
            column: The column to sum.

        Returns:
            AggregateFunction representing the try_sum function.
        """
        from sparkless.functions.base import AggregateFunction, Column
        from sparkless.spark_types import DoubleType

        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "try_sum", name=f"try_sum({column.name})")
        return AggregateFunction(operation, "try_sum", DoubleType())

    @staticmethod
    def try_avg(column: Union[Column, str]) -> "AggregateFunction":
        """Null-safe average aggregate - returns NULL on error (PySpark 3.5+).

        Args:
            column: The column to average.

        Returns:
            AggregateFunction representing the try_avg function.
        """
        from sparkless.functions.base import AggregateFunction, Column
        from sparkless.spark_types import DoubleType

        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "try_avg", name=f"try_avg({column.name})")
        return AggregateFunction(operation, "try_avg", DoubleType())

    @staticmethod
    def try_element_at(
        column: Union[Column, str], index: Union[Column, str, int]
    ) -> ColumnOperation:
        """Null-safe element_at - returns NULL on error (PySpark 3.5+).

        Args:
            column: The column containing array or map.
            index: The index or key to access.

        Returns:
            ColumnOperation representing the try_element_at function.
        """
        from sparkless.functions.base import Column

        if isinstance(column, str):
            column = Column(column)
        if isinstance(index, (str, int)):
            index = Column(str(index)) if isinstance(index, int) else Column(index)

        operation = ColumnOperation(
            column,
            "try_element_at",
            value=index,
            name=f"try_element_at({column.name}, {index.name if hasattr(index, 'name') else index})",
        )
        return operation

    @staticmethod
    def try_to_binary(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Null-safe to_binary - returns NULL on error (PySpark 3.5+).

        Args:
            column: The column to convert to binary.
            format: Optional format ('hex', 'base64', 'utf-8').

        Returns:
            ColumnOperation representing the try_to_binary function.
        """
        from sparkless.functions.base import Column

        if isinstance(column, str):
            column = Column(column)

        if format is not None:
            operation = ColumnOperation(
                column,
                "try_to_binary",
                value=format,
                name=f"try_to_binary({column.name}, '{format}')",
            )
        else:
            operation = ColumnOperation(
                column, "try_to_binary", name=f"try_to_binary({column.name})"
            )
        return operation

    @staticmethod
    def try_to_number(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Null-safe to_number - returns NULL on error (PySpark 3.5+).

        Args:
            column: The column to convert to number.
            format: Optional format string.

        Returns:
            ColumnOperation representing the try_to_number function.
        """
        from sparkless.functions.base import Column

        if isinstance(column, str):
            column = Column(column)

        if format is not None:
            operation = ColumnOperation(
                column,
                "try_to_number",
                value=format,
                name=f"try_to_number({column.name}, '{format}')",
            )
        else:
            operation = ColumnOperation(
                column, "try_to_number", name=f"try_to_number({column.name})"
            )
        return operation

    @staticmethod
    def try_to_timestamp(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Null-safe to_timestamp - returns NULL on error (PySpark 3.5+).

        Args:
            column: The column to convert to timestamp.
            format: Optional format string.

        Returns:
            ColumnOperation representing the try_to_timestamp function.
        """
        from sparkless.functions.base import Column

        if isinstance(column, str):
            column = Column(column)

        if format is not None:
            operation = ColumnOperation(
                column,
                "try_to_timestamp",
                value=format,
                name=f"try_to_timestamp({column.name}, '{format}')",
            )
        else:
            operation = ColumnOperation(
                column, "try_to_timestamp", name=f"try_to_timestamp({column.name})"
            )
        return operation
