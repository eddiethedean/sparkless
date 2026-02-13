"""Robin (robin-sparkless) backend for Sparkless.

Sparkless re-exports Robin's SparkSession and DataFrame via sparkless.sql.
This package provides RobinStorageManager for code that still needs
file-based catalog/table storage (e.g. saveAsTable, catalog).
"""

from .storage import RobinStorageManager

__all__ = ["RobinStorageManager"]
