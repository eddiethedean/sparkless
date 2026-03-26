"""
Sparkless SQL module - PySpark-compatible SQL interface.

Exports SparkSession, DataFrame, Column, and functions from the pure Python engine.
"""

from ._backend import (
    SparkSession,
    DataFrame,
    DataFrameWriter,
    GroupedData,
    Column,
    ColumnOperation,
    F,
    Functions,
)

from ..spark_types import Row
from ..core.exceptions import PySparkTypeError, PySparkValueError

# Import types submodule
from . import types

# Import functions submodule
from . import functions

# Import utils submodule (PySpark-compatible exception exports)
from . import utils

from ..window import Window, WindowSpec

__all__ = [
    "SparkSession",
    "DataFrame",
    "DataFrameWriter",
    "GroupedData",
    "Column",
    "ColumnOperation",
    "Row",
    "Window",
    "WindowSpec",
    "Functions",
    "F",
    "functions",
    "types",
    "utils",
    "PySparkTypeError",
    "PySparkValueError",
]
