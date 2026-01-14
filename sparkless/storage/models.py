"""
Dataclass models for type-safe DuckDB storage operations.

This module provides dataclass-based models for Sparkless's storage layer,
ensuring type safety for all database operations.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from enum import Enum


class StorageMode(str, Enum):
    """Storage operation modes with type safety."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"


@dataclass
class MockDeltaVersion:
    """Represents a single version of a Delta table for time travel."""

    version: int
    timestamp: datetime
    operation: str  # "WRITE", "APPEND", "OVERWRITE", "MERGE", etc.
    data_snapshot: List[Dict[str, Any]]  # Snapshot of data at this version


@dataclass
class MockTableMetadata:
    """Type-safe table metadata model for DuckDB storage."""

    table_name: str
    schema_name: str = "default"
    id: Optional[int] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = None
    row_count: int = 0
    schema_version: str = "1.0"
    storage_format: str = "columnar"
    is_temporary: bool = False
    # Delta Lake specific fields
    format: Optional[str] = None  # "delta", "parquet", "json", etc.
    version: int = 0  # Current Delta table version
    properties: Dict[str, Any] = field(default_factory=dict)  # Delta properties
    version_history: List["MockDeltaVersion"] = field(
        default_factory=list
    )  # Version history for time travel


@dataclass
class ColumnDefinition:
    """Type-safe column definition model for DuckDB tables."""

    column_name: str
    column_type: str
    table_id: Optional[int] = None
    id: Optional[int] = None
    is_nullable: bool = True
    is_primary_key: bool = False
    default_value: Optional[str] = None
    column_order: int = 0


@dataclass
class DuckDBTableModel:
    """Base model for DuckDB table operations with type safety."""

    table_name: str
    schema_name: str = "default"

    def get_full_name(self) -> str:
        """Get fully qualified table name."""
        return (
            f"{self.schema_name}.{self.table_name}"
            if self.schema_name != "default"
            else self.table_name
        )


@dataclass
class DuckDBConnectionConfig:
    """Type-safe configuration for DuckDB connections."""

    database_path: str = "sparkless.duckdb"
    read_only: bool = False
    memory_limit: Optional[str] = None
    thread_count: Optional[int] = None
    enable_extensions: bool = True


@dataclass
class StorageOperationResult:
    """Type-safe result model for storage operations."""

    success: bool
    rows_affected: int
    operation_type: str
    table_name: str
    error_message: Optional[str] = None
    execution_time_ms: Optional[float] = None


@dataclass
class QueryResult:
    """Type-safe model for query results."""

    data: List[Dict[str, Any]]
    row_count: int
    column_count: int
    query: str
    execution_time_ms: Optional[float] = None
