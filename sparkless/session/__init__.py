"""
Session management module for Sparkless.

Sparkless v4 re-exports Robin via sparkless.sql. This module re-exports
SparkSession (Robin-backed) and SparkSessionBuilder from sql; SparkContext
and JVMContext remain for compatibility.
"""

from ..sql import SparkSession, SparkSessionBuilder
from .context import SparkContext, JVMContext
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
