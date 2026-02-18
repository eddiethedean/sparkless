"""
Sparkless SQL module - PySpark-compatible SQL interface.
"""

from ..session import SparkSession
from ..dataframe import (
    DataFrame,
    DataFrameWriter,
    GroupedData,
)
from ..functions import Column, ColumnOperation, F, Functions
from ..spark_types import Row
from ..window import Window, WindowSpec

# Import exceptions (PySpark 3.5+ compatibility)
from ..core.exceptions import PySparkTypeError, PySparkValueError

# Import types submodule
from . import types

# Import functions submodule
from . import functions

# Import utils submodule (PySpark-compatible exception exports)
from . import utils

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
