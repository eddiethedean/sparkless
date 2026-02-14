"""
Sparkless SQL module - PySpark-compatible SQL interface.

This module re-exports Robin (robin-sparkless) as the execution engine so that
existing imports like `from sparkless.sql import SparkSession, DataFrame, functions as F`
work without change. SparkSession is a thin wrapper that adds createDataFrame(data, schema=None).

When SPARKLESS_BACKEND or SPARKLESS_TEST_BACKEND is "robin", F/Column/ColumnOperation
are taken from sparkless.functions so that expressions are Sparkless types and
_robin_compat can convert them to Robin Column for Robin APIs.
"""

from __future__ import annotations

import os

import robin_sparkless as _robin

from sparkless.sql._session import SparkSession, SparkSessionBuilder
from sparkless.sql._robin_compat import (
    _PySparkCompatDataFrame as DataFrame,
    _PySparkCompatGroupedData as GroupedData,
)
from sparkless.spark_types import Row

# Backend: when robin, use Sparkless F/Column/ColumnOperation so _to_robin_column can convert
_backend = (os.environ.get("SPARKLESS_BACKEND") or os.environ.get("SPARKLESS_TEST_BACKEND") or "").strip().lower()

if _backend == "robin":
    from sparkless.functions import F as _sparkless_F
    from sparkless.functions.core.column import Column as _sparkless_Column, ColumnOperation as _sparkless_ColumnOperation
    F = _sparkless_F
    functions = _sparkless_F
    Functions = _sparkless_F
    Column = _sparkless_Column
    ColumnOperation = _sparkless_ColumnOperation
else:
    Column = _robin.Column
    Functions = _robin
    F = _robin
    functions = _robin
    ColumnOperation = getattr(_robin, "ColumnOperation", Column)

# Re-export from Robin (Writer, Reader, Window) regardless of backend
DataFrameWriter = _robin.DataFrameWriter
DataFrameReader = _robin.DataFrameReader
Window = _robin.Window
# Robin may use WindowSpec or similar; expose if present
WindowSpec = getattr(_robin, "WindowSpec", Window)

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
