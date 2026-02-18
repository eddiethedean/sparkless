"""
Storage module for Sparkless.

v4 Robin-only: session uses RobinCatalogStorage (Robin's built-in catalog).
File storage and serialization utilities remain for reader/writer and tests.
"""

# Import interfaces from canonical location
from ..core.interfaces.storage import IStorageManager, ITable
from ..core.types.schema import ISchema

# Import backends (v4 Robin-only: memory is the single catalog backing)
from .backends.memory import MemoryStorageManager, MemoryTable, MemorySchema
from .models import (
    MockTableMetadata,
    ColumnDefinition,
    StorageMode,
    StorageOperationResult,
    QueryResult,
)
from .backends.file import FileStorageManager, FileTable, FileSchema
from .backends.robin import RobinCatalogStorage

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
    # Memory backend (v4 Robin-only catalog)
    "MemoryStorageManager",
    "MemoryTable",
    "MemorySchema",
    # Storage models (dataclasses)
    "MockTableMetadata",
    "ColumnDefinition",
    "StorageMode",
    "StorageOperationResult",
    "QueryResult",
    # Robin catalog backend (v4 default)
    "RobinCatalogStorage",
    # File backend
    "FileStorageManager",
    "FileTable",
    "FileSchema",
    # Serialization
    "JSONSerializer",
    "CSVSerializer",
    # Storage managers
    "StorageManagerFactory",
    "UnifiedStorageManager",
]
