"""
Conditional expression evaluator for DataFrame operations.

This module handles evaluation of CASE WHEN expressions and other conditional logic.
"""

from typing import Any, Dict
from sparkless.functions import Column, ColumnOperation
from sparkless.functions.conditional import CaseWhen


class ConditionalEvaluator:
    """Evaluates conditional expressions like CASE WHEN."""

    def __init__(self, base_evaluator: Any):
        """Initialize ConditionalEvaluator.

        Args:
            base_evaluator: Reference to the main ExpressionEvaluator for
                           delegating expression evaluation.
        """
        self._base_evaluator = base_evaluator

    def evaluate_case_when(self, row: Dict[str, Any], case_when: CaseWhen) -> Any:
        """Evaluate when/otherwise expressions.

        Args:
            row: Row data dictionary
            case_when: CaseWhen expression to evaluate

        Returns:
            Evaluated value from matching condition, or default value
        """
        # Evaluate each condition in order
        for condition, value in case_when.conditions:
            condition_result = self._base_evaluator.evaluate_expression(row, condition)
            if condition_result:
                # Return the value (evaluate if it's an expression)
                # Check for Literal, Column, or ColumnOperation
                if hasattr(value, "value") and hasattr(value, "name"):
                    # It's a Literal - evaluate it
                    return self._base_evaluator._evaluate_value(row, value)
                elif isinstance(value, (Column, ColumnOperation)):
                    return self._base_evaluator.evaluate_expression(row, value)
                return value

        # No condition matched, return default value
        if case_when.default_value is not None:
            # Check for Literal, Column, or ColumnOperation
            if hasattr(case_when.default_value, "value") and hasattr(
                case_when.default_value, "name"
            ):
                # It's a Literal - evaluate it
                return self._base_evaluator._evaluate_value(
                    row, case_when.default_value
                )
            elif isinstance(case_when.default_value, (Column, ColumnOperation)):
                return self._base_evaluator.evaluate_expression(
                    row, case_when.default_value
                )
            return case_when.default_value

        return None
