"""
Session management module for Sparkless.

Provides SparkContext, Catalog, and configuration.
SparkSession is re-exported from the core module directly to avoid circular
imports with sparkless.sql.
"""

from .context import SparkContext, JVMContext
from .catalog import Catalog, Database, Table
from .config import Configuration
from .core.session import SparkSession

__all__ = [
    "SparkSession",
    "SparkContext",
    "JVMContext",
    "Catalog",
    "Database",
    "Table",
    "Configuration",
]
