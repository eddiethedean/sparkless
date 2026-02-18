"""
Session management module for Sparkless.

Robin backend: SparkSession comes from sql. This module provides SparkContext,
Catalog, and configuration without loading the legacy Python execution stack.
"""

from .context import SparkContext, JVMContext
from .catalog import Catalog, Database, Table
from .config import Configuration

# Re-export from sql for backward compatibility
from ..sql import SparkSession

__all__ = [
    "SparkSession",
    "SparkContext",
    "JVMContext",
    "Catalog",
    "Database",
    "Table",
    "Configuration",
]
