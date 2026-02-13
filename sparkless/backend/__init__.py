"""
Backend module for Sparkless.

Sparkless v4 re-exports Robin via sparkless.sql. This module provides
BackendFactory.create_storage_backend("robin") for code that needs
file-based catalog/table storage.
"""

from .protocols import QueryExecutor, StorageBackend
from .factory import BackendFactory

__all__ = [
    "QueryExecutor",
    "StorageBackend",
    "BackendFactory",
]
