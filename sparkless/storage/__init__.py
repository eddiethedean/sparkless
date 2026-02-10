"""
Storage module for Sparkless.

This module provides storage interfaces and backends. In v4 the default
persistent backend is Robin (via BackendFactory); file and memory backends
are available for testing. Use BackendFactory.create_storage_backend("robin")
for the v4 default.

Key Features:
    - Robin backend (v4 default) via sparkless.backend.factory
    - In-memory and file-based storage for testing
    - Unified storage interface
    - Schema management and table operations
"""

# Import interfaces from canonical location
from ..core.interfaces.storage import IStorageManager, ITable
from ..core.types.schema import ISchema

# Import backends
from .backends.memory import MemoryStorageManager, MemoryTable, MemorySchema
from .backends.file import FileStorageManager, FileTable, FileSchema

# Robin backend (v4 default) - use BackendFactory.create_storage_backend("robin") in practice
from sparkless.backend.robin.storage import RobinStorageManager

from .models import (
    MockTableMetadata,
    ColumnDefinition,
    StorageMode,
    StorageOperationResult,
    QueryResult,
)

# Import serialization
from .serialization.json import JSONSerializer
from .serialization.csv import CSVSerializer

# Import managers
from .manager import StorageManagerFactory, UnifiedStorageManager

__all__ = [
    # Interfaces
    "IStorageManager",
    "ITable",
    "ISchema",
    # Memory backend
    "MemoryStorageManager",
    "MemoryTable",
    "MemorySchema",
    # File backend
    "FileStorageManager",
    "FileTable",
    "FileSchema",
    # Robin backend (v4 default)
    "RobinStorageManager",
    # Storage models (dataclasses)
    "MockTableMetadata",
    "ColumnDefinition",
    "StorageMode",
    "StorageOperationResult",
    "QueryResult",
    # Serialization
    "JSONSerializer",
    "CSVSerializer",
    # Storage managers
    "StorageManagerFactory",
    "UnifiedStorageManager",
]
