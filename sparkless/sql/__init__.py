"""
Sparkless SQL module - PySpark-compatible SQL interface.

This module re-exports Robin (robin-sparkless) as the execution engine so that
existing imports like `from sparkless.sql import SparkSession, DataFrame, functions as F`
work without change. SparkSession is a thin wrapper that adds createDataFrame(data, schema=None).
"""

from __future__ import annotations

import robin_sparkless as _robin

from sparkless.sql._session import SparkSession, SparkSessionBuilder
from sparkless.spark_types import Row

# Re-export from Robin
DataFrame = _robin.DataFrame
Column = _robin.Column
DataFrameWriter = _robin.DataFrameWriter
DataFrameReader = _robin.DataFrameReader
GroupedData = _robin.GroupedData
Window = _robin.Window
# Robin may use WindowSpec or similar; expose if present
WindowSpec = getattr(_robin, "WindowSpec", Window)
# Functions namespace (Robin module is the F namespace)
Functions = _robin
F = _robin
functions = _robin

# ColumnOperation: Robin may not expose this name; keep for compatibility if code references it
ColumnOperation = getattr(_robin, "ColumnOperation", Column)

# Types submodule (Sparkless types for PySpark compatibility)
from sparkless.sql import types  # noqa: E402

# Utils submodule (exceptions)
from sparkless.sql import utils  # noqa: E402

# Exceptions (PySpark 3.5+ compatibility)
from sparkless.core.exceptions import PySparkTypeError, PySparkValueError  # noqa: E402

__all__ = [
    "SparkSession",
    "SparkSessionBuilder",
    "DataFrame",
    "DataFrameWriter",
    "DataFrameReader",
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
