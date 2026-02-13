"""
DataFrame module for Sparkless.

Sparkless v4 re-exports Robin's DataFrame and related types. All DataFrame
operations use Robin (robin-sparkless) as the execution engine.
"""

from ..sql import (
    DataFrame,
    DataFrameWriter,
    DataFrameReader,
    GroupedData,
)

__all__ = [
    "DataFrame",
    "DataFrameWriter",
    "DataFrameReader",
    "GroupedData",
]
