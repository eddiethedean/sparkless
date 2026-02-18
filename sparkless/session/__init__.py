"""
Session management module for Sparkless.

This module provides session management, SQL processing, and configuration
management for Sparkless, following the Single Responsibility Principle
and enabling better testability and modularity.

Components:
    - SparkSession: Main session class
    - SparkContext: Spark context management
    - Catalog: Database and table catalog operations
    - Configuration management
    - SQL processing pipeline
"""

from .core import SparkSession
from .core import (
    SparkSessionBuilder,
    SparkContext,
    JVMContext,
)
from .catalog import Catalog, Database, Table
from .config import Configuration

__all__ = [
    "SparkSession",
    "SparkSessionBuilder",
    "SparkContext",
    "JVMContext",
    "Catalog",
    "Database",
    "Table",
    "Configuration",
]
