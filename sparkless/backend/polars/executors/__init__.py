"""
Operation executor modules for Polars backend.

This package contains specialized executors split from the monolithic
operation_executor.py for better maintainability and organization.

Modules:
    - join_executor: Join operation execution
    - aggregation_executor: Aggregation operation execution
    - transformation_executor: DataFrame transformation execution
"""

__all__ = [
    "JoinExecutor",
    "AggregationExecutor",
    "TransformationExecutor",
]
