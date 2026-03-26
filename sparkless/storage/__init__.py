"""
Storage module for Sparkless.

Provides in-memory and file-based storage backends for catalog operations.
"""

# Import interfaces from canonical location
from ..core.interfaces.storage import IStorageManager, ITable
from ..core.types.schema import ISchema

# Import backends
from .backends.memory import MemoryStorageManager, MemoryTable, MemorySchema
from .models import (
    MockTableMetadata,
    ColumnDefinition,
    StorageMode,
    StorageOperationResult,
    QueryResult,
)
from .backends.file import FileStorageManager, FileTable, FileSchema

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
    # Storage models (dataclasses)
    "MockTableMetadata",
    "ColumnDefinition",
    "StorageMode",
    "StorageOperationResult",
    "QueryResult",
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
