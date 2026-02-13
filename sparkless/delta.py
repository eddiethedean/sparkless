"""
Delta Lake compatibility for Sparkless.

Sparkless v4 provides stubs for DeltaTable and DeltaMergeBuilder for API
compatibility. For real Delta operations, use Robin's Delta support or
PySpark with delta-spark.
"""

from sparkless import DeltaTable, DeltaMergeBuilder

__all__ = ["DeltaTable", "DeltaMergeBuilder"]
