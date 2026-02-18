"""
Backend selection for sparkless.sql.

Exports SparkSession, DataFrame, Column, F, Functions from either the Robin
(PyO3) backend or the pure-Python backend. Used by sql/__init__.py and
sql/functions.py to avoid circular imports.
"""

from __future__ import annotations

from typing import Any

try:
    from ._robin_sql import RobinSparkSession, RobinDataFrame
    from ._robin_functions import get_robin_functions

    _robin = __import__("sparkless_robin", fromlist=[])
    _ROBIN_AVAILABLE = True

    SparkSession = RobinSparkSession
    DataFrame = RobinDataFrame
    Column = _robin.PyColumn
    F = get_robin_functions()
    Functions = type(F)
    ColumnOperation = None
except ImportError:
    _ROBIN_AVAILABLE = False

    from ..session import SparkSession
    from ..dataframe import DataFrame
    from ..functions import Column, ColumnOperation, F, Functions
