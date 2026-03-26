"""
Backend selection for sparkless.sql.

Exports SparkSession, DataFrame, Column, F, Functions from the pure Python
engine (no Rust/Robin dependency).
"""

from __future__ import annotations

# Use direct module paths to avoid circular imports through session/__init__.py
from sparkless.session.core.session import SparkSession  # noqa: F401
from sparkless.dataframe.dataframe import DataFrame  # noqa: F401
from sparkless.dataframe.writer import DataFrameWriter  # noqa: F401
from sparkless.dataframe.grouped.base import GroupedData  # noqa: F401
from sparkless.functions.core.column import Column, ColumnOperation  # noqa: F401
from sparkless.functions.functions import Functions  # noqa: F401

F = Functions()
