"""
Expression evaluator modules for DataFrame operations.

This package contains specialized evaluators split from the monolithic
expression_evaluator.py for better maintainability and organization.

Modules:
    - function_evaluator: Function call evaluation
    - conditional_evaluator: CASE WHEN and conditional logic evaluation
    - aggregate_evaluator: Aggregate function evaluation
"""

__all__ = [
    "FunctionEvaluator",
    "ConditionalEvaluator",
    "AggregateEvaluator",
]
