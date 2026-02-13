"""
Protocol definitions for backend interfaces.

This module defines the protocols (interfaces) that backend implementations
must satisfy. The materializer and export backends have been removed; only
storage remains for code that needs catalog/table persistence.
"""

from typing import Any, Dict, List, Protocol
from sparkless.spark_types import StructType
from sparkless.core.interfaces.storage import IStorageManager


class QueryExecutor(Protocol):
    """Protocol for executing queries on data. Kept for type references."""

    def execute_query(self, query: str) -> List[Dict[str, Any]]: ...
    def create_table(
        self, name: str, schema: StructType, data: List[Dict[str, Any]]
    ) -> None: ...
    def close(self) -> None: ...


# StorageBackend protocol is now an alias for IStorageManager
StorageBackend = IStorageManager
