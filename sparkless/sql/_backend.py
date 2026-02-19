"""
Backend selection for sparkless.sql.

Exports SparkSession, DataFrame, Column, F, Functions from the Robin (PyO3)
backend. Sparkless requires sparkless_robin (build with: maturin develop).
"""

from __future__ import annotations

from typing import Any

try:
    _robin = __import__("sparkless_robin", fromlist=[])
except ImportError as e:
    raise ImportError(
        "sparkless_robin native extension is required. "
        "Build with: maturin develop"
    ) from e

from ._robin_sql import RobinSparkSession, RobinDataFrame, RobinDataFrameWriter, RobinGroupedData
from ._robin_functions import get_robin_functions
from ._robin_column import RobinColumn

SparkSession = RobinSparkSession
DataFrame = RobinDataFrame
DataFrameWriter = RobinDataFrameWriter
GroupedData = RobinGroupedData
Column = RobinColumn
F = get_robin_functions()
Functions = type(F)
ColumnOperation = None
