"""
Backend module for Sparkless.

This module provides backend implementations for storage, query execution,
and data materialization. It decouples backend-specific logic from the
core DataFrame and Session modules.

Architecture:
    - protocols.py: Protocol definitions for backend interfaces
    - factory.py: Factory for creating backend instances
    - robin/: Robin backend implementation (only backend in v4)

Example:
    >>> from sparkless.backend.factory import BackendFactory
    >>> storage = BackendFactory.create_storage_backend("robin")
    >>> materializer = BackendFactory.create_materializer("robin")
"""

from .protocols import (
    QueryExecutor,
    DataMaterializer,
    StorageBackend,
    ExportBackend,
)
from .factory import BackendFactory

__all__ = [
    "QueryExecutor",
    "DataMaterializer",
    "StorageBackend",
    "ExportBackend",
    "BackendFactory",
]
