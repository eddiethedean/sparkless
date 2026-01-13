"""
Polars storage backend with Parquet file persistence.

This module provides a Polars-based storage implementation using Parquet files
for persistence and in-memory DataFrames for active operations.
"""

import os
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone
import polars as pl

from sparkless.core.interfaces.storage import IStorageManager, ITable
from sparkless.spark_types import StructType, StructField
from .schema_registry import SchemaRegistry
from .type_mapper import mock_type_to_polars_dtype
from .schema_utils import align_frame_to_schema


class PolarsTable(ITable):
    """Table implementation using Polars DataFrame."""

    def __init__(
        self,
        name: str,
        schema: StructType,
        schema_name: str = "default",
        db_path: Optional[str] = None,
    ):
        """Initialize Polars table.

        Args:
            name: Table name
            schema: StructType schema
            schema_name: Schema name (default: "default")
            db_path: Optional database path for persistence
        """
        self._name = name
        self._schema = schema
        self._schema_name = schema_name
        self._db_path = db_path

        # Initialize empty DataFrame with schema
        self._df: Optional[pl.DataFrame] = None
        self._initialize_empty_dataframe()

        # Metadata
        self._metadata = {
            "table_name": name,
            "schema_name": schema_name,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": None,
            "row_count": 0,
            "schema_version": "1.0",
            "storage_format": "parquet",
            "is_temporary": False,
        }

        # Load from Parquet if exists (only if not in-memory)
        if db_path and db_path != ":memory:":
            self._load_from_parquet()

    def _initialize_empty_dataframe(self) -> None:
        """Initialize empty DataFrame with proper schema."""
        # Create empty DataFrame with schema
        if not self._schema.fields:
            # Empty schema - create empty DataFrame
            self._df = pl.DataFrame()
            return

        schema_dict = {}
        for field in self._schema.fields:
            polars_dtype = mock_type_to_polars_dtype(field.dataType)
            # Create empty list with correct dtype
            schema_dict[field.name] = pl.Series(field.name, [], dtype=polars_dtype)
        self._df = pl.DataFrame(schema_dict)

    def _get_parquet_path(self) -> Optional[str]:
        """Get path to Parquet file for this table."""
        if not self._db_path or self._db_path == ":memory:":
            return None
        schema_dir = os.path.join(self._db_path, self._schema_name)
        return os.path.join(schema_dir, f"{self._name}.parquet")

    def _load_from_parquet(self) -> None:
        """Load table data from Parquet file if it exists."""
        parquet_path = self._get_parquet_path()
        if parquet_path and os.path.exists(parquet_path):
            try:
                self._df = pl.read_parquet(parquet_path)
                self._metadata["row_count"] = len(self._df)
            except Exception:
                # If loading fails, keep empty DataFrame
                pass

    def _save_to_parquet(self) -> None:
        """Save table data to Parquet file."""
        if not self._db_path or self._df is None:
            return
        parquet_path = self._get_parquet_path()
        if parquet_path:
            os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
            self._df.write_parquet(parquet_path, compression="snappy")

    @property
    def name(self) -> str:
        """Get table name."""
        return self._name

    @property
    def schema(self) -> StructType:
        """Get table schema."""
        return self._schema

    @property
    def metadata(self) -> Dict[str, Any]:
        """Get table metadata."""
        return self._metadata

    def insert(self, data: List[Dict[str, Any]]) -> None:
        """Insert data into table.

        Args:
            data: List of dictionaries representing rows
        """
        if not data:
            return

        try:
            # Convert to Polars DataFrame and align to table schema
            new_df = pl.DataFrame(data)
            new_df = align_frame_to_schema(new_df, self._schema)

            if self._df is None or self._df.height == 0:
                self._df = new_df
            else:
                aligned_existing = align_frame_to_schema(self._df, self._schema)
                self._df = pl.concat([aligned_existing, new_df], how="vertical")

            # Ensure stored frame remains aligned to schema after concatenation
            self._df = align_frame_to_schema(self._df, self._schema)

            # Update metadata
            self._metadata["row_count"] = len(self._df)
            self._metadata["updated_at"] = datetime.now(timezone.utc).isoformat()

            # Save to Parquet (only if not in-memory)
            if self._db_path and self._db_path != ":memory:":
                self._save_to_parquet()

        except Exception as e:
            raise ValueError(f"Failed to insert data: {e}") from e

    def query(self, **filters: Any) -> List[Dict[str, Any]]:
        """Query data from table.

        Args:
            **filters: Column filters (e.g., col_name=value)

        Returns:
            List of dictionaries representing rows
        """
        if self._df is None or len(self._df) == 0:
            return []

        df = self._df

        # Apply filters
        for col_name, value in filters.items():
            if col_name in df.columns:
                df = df.filter(pl.col(col_name) == value)

        from typing import cast

        return cast("List[Dict[str, Any]]", df.to_dicts())

    def count(self) -> int:
        """Count rows in table.

        Returns:
            Number of rows
        """
        if self._df is None:
            return 0
        return len(self._df)

    def truncate(self) -> None:
        """Truncate table (remove all data)."""
        self._initialize_empty_dataframe()
        self._metadata["row_count"] = 0
        self._metadata["updated_at"] = datetime.now(timezone.utc).isoformat()

        # Delete Parquet file if exists
        parquet_path = self._get_parquet_path()
        if parquet_path and os.path.exists(parquet_path):
            os.remove(parquet_path)

    def drop(self) -> None:
        """Drop table (remove data and metadata)."""
        self._df = None
        self._metadata.clear()

        # Delete Parquet file if exists
        parquet_path = self._get_parquet_path()
        if parquet_path and os.path.exists(parquet_path):
            os.remove(parquet_path)


class PolarsSchema:
    """Schema implementation for Polars storage."""

    def __init__(self, name: str, db_path: Optional[str] = None):
        """Initialize Polars schema.

        Args:
            name: Schema name
            db_path: Optional database path
        """
        self.name = name
        self.db_path = db_path
        self.tables: Dict[str, PolarsTable] = {}

        # Create schema directory if db_path is provided and not in-memory
        if db_path and db_path != ":memory:":
            schema_dir = os.path.join(db_path, name)
            os.makedirs(schema_dir, exist_ok=True)

    def create_table(
        self, table: str, columns: Union[List[StructField], StructType]
    ) -> Optional[PolarsTable]:
        """Create a new table in this schema.

        Args:
            table: Table name
            columns: Table schema (StructType or list of StructField)

        Returns:
            PolarsTable instance
        """
        # If table already exists, return existing instance to avoid overwriting data
        if table in self.tables:
            return self.tables[table]

        schema = StructType(columns) if isinstance(columns, list) else columns

        polars_table = PolarsTable(
            table, schema, schema_name=self.name, db_path=self.db_path
        )
        self.tables[table] = polars_table
        return polars_table


class PolarsStorageManager(IStorageManager):
    """Polars-based storage manager implementing IStorageManager protocol."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize Polars storage manager.

        Args:
            db_path: Optional database path for persistent storage.
                    If None, uses in-memory storage only.
        """
        self.db_path = db_path or ":memory:"
        self.schemas: Dict[str, PolarsSchema] = {}
        self._current_schema = "default"
        self._syncing: bool = False
        # Use ":memory:" for schema registry when using in-memory storage
        # The schema registry already handles ":memory:" by skipping file operations
        schema_storage_path = self.db_path
        self.schema_registry = SchemaRegistry(schema_storage_path)

        # Create default schema
        if self.db_path != ":memory:":
            os.makedirs(os.path.join(self.db_path, "default"), exist_ok=True)
        self.schemas["default"] = PolarsSchema("default", self.db_path)

        self._rehydrate_from_disk()

    def create_schema(self, schema_name: str) -> None:
        """Create a new schema.

        Args:
            schema_name: Schema name
        """
        if schema_name in self.schemas:
            return

        if self.db_path != ":memory:":
            schema_dir = os.path.join(self.db_path, schema_name)
            os.makedirs(schema_dir, exist_ok=True)

        self.schemas[schema_name] = PolarsSchema(schema_name, self.db_path)

    def drop_schema(self, schema_name: str, cascade: bool = False) -> None:
        """Drop a schema.

        Args:
            schema_name: Schema name
            cascade: If True, drop all tables in schema
        """
        if schema_name not in self.schemas:
            return

        if cascade:
            # Drop all tables
            for table_name in list(self.schemas[schema_name].tables.keys()):
                self.drop_table(schema_name, table_name)

        # Remove schema directory
        if self.db_path != ":memory:":
            schema_dir = os.path.join(self.db_path, schema_name)
            if os.path.exists(schema_dir):
                import shutil

                shutil.rmtree(schema_dir)

        del self.schemas[schema_name]
        if self._current_schema == schema_name:
            self._current_schema = "default"

    def _rehydrate_from_disk(self) -> None:
        """Hydrate schemas and tables from disk for persistent storage."""
        if self.db_path == ":memory:":
            return
        if not os.path.exists(self.db_path):
            return

        for schema_name in sorted(os.listdir(self.db_path)):
            schema_dir = os.path.join(self.db_path, schema_name)
            if not os.path.isdir(schema_dir):
                continue

            self.create_schema(schema_name)
            registry_files = [
                filename
                for filename in os.listdir(schema_dir)
                if filename.endswith(".schema.json")
            ]

            schema_container = self.schemas[schema_name]
            for registry_file in registry_files:
                table_name = registry_file[: -len(".schema.json")]
                schema = self.schema_registry.load_schema(schema_name, table_name)
                if schema is None:
                    continue
                if table_name in schema_container.tables:
                    continue

                schema_container.tables[table_name] = PolarsTable(
                    table_name,
                    schema,
                    schema_name=schema_name,
                    db_path=self.db_path,
                )

    def schema_exists(self, schema_name: str) -> bool:
        """Check if schema exists.

        Args:
            schema_name: Schema name

        Returns:
            True if schema exists, False otherwise
        """
        if schema_name == "default":
            return True
        return schema_name in self.schemas

    def get_current_schema(self) -> str:
        """Return the schema used for unqualified table references."""
        return self._current_schema

    def set_current_schema(self, schema_name: str) -> None:
        """Set the schema used for unqualified table references."""
        if not self.schema_exists(schema_name):
            raise ValueError(f"Schema '{schema_name}' does not exist")
        self._current_schema = schema_name

    def list_schemas(self) -> List[str]:
        """List all schemas.

        Returns:
            List of schema names
        """
        if self.db_path == ":memory:":
            return list(self.schemas.keys())

        # Discover schemas from directory structure
        if os.path.exists(self.db_path):
            schemas = [
                d
                for d in os.listdir(self.db_path)
                if os.path.isdir(os.path.join(self.db_path, d))
            ]
            # Add any discovered schemas
            for schema_name in schemas:
                if schema_name not in self.schemas:
                    self.schemas[schema_name] = PolarsSchema(schema_name, self.db_path)
            return list(self.schemas.keys())
        return ["default"]

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        fields: Union[List[StructField], StructType],
    ) -> Optional[PolarsTable]:
        """Create a new table.

        Args:
            schema_name: Schema name
            table_name: Table name
            fields: Table schema

        Returns:
            PolarsTable instance
        """
        # Ensure schema exists
        if schema_name not in self.schemas:
            self.create_schema(schema_name)

        schema = self.schemas[schema_name]

        # Create table
        table = schema.create_table(table_name, fields)

        # Save schema to registry
        mock_schema = StructType(fields) if isinstance(fields, list) else fields
        self.schema_registry.save_schema(schema_name, table_name, mock_schema)

        return table

    def drop_table(self, schema_name: str, table_name: str) -> None:
        """Drop a table.

        Args:
            schema_name: Schema name
            table_name: Table name
        """
        if schema_name not in self.schemas:
            return

        schema = self.schemas[schema_name]
        if table_name in schema.tables:
            schema.tables[table_name].drop()
            del schema.tables[table_name]

        # Delete schema file
        self.schema_registry.delete_schema(schema_name, table_name)

        # Delete Parquet file
        if self.db_path != ":memory:":
            parquet_path = os.path.join(
                self.db_path, schema_name, f"{table_name}.parquet"
            )
            if os.path.exists(parquet_path):
                os.remove(parquet_path)

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """Check if table exists.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            True if table exists, False otherwise
        """
        # Check in-memory tables first
        if schema_name in self.schemas:
            schema = self.schemas[schema_name]
            if table_name in schema.tables:
                return True

        # Check if Parquet file exists
        if self.db_path != ":memory:":
            parquet_path = os.path.join(
                self.db_path, schema_name, f"{table_name}.parquet"
            )
            if os.path.exists(parquet_path):
                return True

        return False

    def list_tables(self, schema_name: Optional[str] = None) -> List[str]:
        """List tables in schema.

        Args:
            schema_name: Schema name (None for all schemas)

        Returns:
            List of table names
        """
        if schema_name:
            if schema_name not in self.schemas:
                # Discover tables from Parquet files if persistent storage
                if self.db_path != ":memory:":
                    schema_dir = os.path.join(self.db_path, schema_name)
                    if os.path.exists(schema_dir):
                        # Find all .parquet files
                        tables = []
                        for file in os.listdir(schema_dir):
                            if file.endswith(".parquet"):
                                table_name = file[:-8]  # Remove .parquet extension
                                tables.append(table_name)
                        return tables
                return []
            # Get from in-memory tables
            tables = list(self.schemas[schema_name].tables.keys())
            # Also check for Parquet files if persistent storage
            if self.db_path != ":memory:":
                schema_dir = os.path.join(self.db_path, schema_name)
                if os.path.exists(schema_dir):
                    for file in os.listdir(schema_dir):
                        if file.endswith(".parquet"):
                            table_name = file[:-8]
                            if table_name not in tables:
                                tables.append(table_name)
            return tables

        # List all tables across all schemas
        all_tables: List[str] = []
        for schema in self.schemas.values():
            all_tables.extend(schema.tables.keys())

        # Also discover from persistent storage
        if self.db_path != ":memory:" and os.path.exists(self.db_path):
            for schema_dir_name in os.listdir(self.db_path):
                schema_path = os.path.join(self.db_path, schema_dir_name)
                if os.path.isdir(schema_path):
                    for file in os.listdir(schema_path):
                        if file.endswith(".parquet"):
                            table_name = file[:-8]
                            qualified_name = f"{schema_dir_name}.{table_name}"
                            if qualified_name not in all_tables:
                                all_tables.append(qualified_name)

        return all_tables

    def get_table_schema(self, schema_name: str, table_name: str) -> StructType:
        """Get table schema.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            StructType schema
        """
        # Try to load from registry
        schema = self.schema_registry.load_schema(schema_name, table_name)
        if schema:
            return schema

        # Fallback: get from table if it exists
        if schema_name in self.schemas:
            schema_obj = self.schemas[schema_name]
            if table_name in schema_obj.tables:
                return schema_obj.tables[table_name].schema

        raise ValueError(f"Table {schema_name}.{table_name} not found")

    def insert_data(
        self, schema_name: str, table_name: str, data: List[Dict[str, Any]]
    ) -> None:
        """Insert data into table.

        Args:
            schema_name: Schema name
            table_name: Table name
            data: List of dictionaries representing rows
        """
        if schema_name not in self.schemas:
            raise ValueError(f"Schema {schema_name} not found")

        schema = self.schemas[schema_name]
        if table_name not in schema.tables:
            raise ValueError(f"Table {schema_name}.{table_name} not found")

        table = schema.tables[table_name]
        table.insert(data)

        # Synchronize writes to other active sessions if this storage is not shared.
        # Guard with _syncing to avoid recursion when propagating.
        if not getattr(self, "_syncing", False):
            self._syncing = True
            try:
                table_schema = table.schema
                self._sync_active_sessions(schema_name, table_name, table_schema, data)
            finally:
                self._syncing = False

    def query_data(
        self, schema_name: str, table_name: str, **filters: Any
    ) -> List[Dict[str, Any]]:
        """Query data from table.

        Args:
            schema_name: Schema name
            table_name: Table name
            **filters: Column filters

        Returns:
            List of dictionaries representing rows
        """
        if schema_name not in self.schemas:
            raise ValueError(f"Schema {schema_name} not found")

        schema = self.schemas[schema_name]
        if table_name not in schema.tables:
            raise ValueError(f"Table {schema_name}.{table_name} not found")

        table = schema.tables[table_name]
        return table.query(**filters)

    def get_table_metadata(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get table metadata.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            Dictionary with table metadata
        """
        if schema_name not in self.schemas:
            raise ValueError(f"Schema {schema_name} not found")

        schema = self.schemas[schema_name]
        if table_name not in schema.tables:
            raise ValueError(f"Table {schema_name}.{table_name} not found")

        table = schema.tables[table_name]
        return table.metadata

    def get_data(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get all data from table (optional method).

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of dictionaries representing all rows
        """
        return self.query_data(schema_name, table_name)

    def query_table(
        self, schema_name: str, table_name: str, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query table with filter expression (optional method).

        Args:
            schema_name: Schema name
            table_name: Table name
            filter_expr: Optional filter expression (ignored for Polars backend)

        Returns:
            List of dictionaries representing rows

        Note:
            Filter expressions are not supported at the storage level for Polars backend.
            Filtering should be done at the DataFrame level using DataFrame.filter() or
            by loading data into a DataFrame and applying filters there. This method
            returns all data from the table regardless of filter_expr parameter.
        """
        # Polars backend handles filtering at DataFrame level, not storage level
        # Filter expressions are ignored here - use DataFrame.filter() instead
        return self.query_data(schema_name, table_name)

    def update_table_metadata(
        self, schema_name: str, table_name: str, metadata_updates: Dict[str, Any]
    ) -> None:
        """Update table metadata (optional method).

        Args:
            schema_name: Schema name
            table_name: Table name
            metadata_updates: Dictionary of metadata updates
        """
        if schema_name not in self.schemas:
            raise ValueError(f"Schema {schema_name} not found")

        schema = self.schemas[schema_name]
        if table_name not in schema.tables:
            raise ValueError(f"Table {schema_name}.{table_name} not found")

        table = schema.tables[table_name]
        table._metadata.update(metadata_updates)
        table._metadata["updated_at"] = datetime.now(timezone.utc).isoformat()

    def _sync_active_sessions(
        self,
        schema_name: str,
        table_name: str,
        schema: StructType,
        data: List[Dict[str, Any]],
    ) -> None:
        """Replicate writes into other active SparkSession storages (best effort)."""
        try:
            from sparkless.session.core.session import SparkSession
        except Exception:
            return

        active_sessions = getattr(SparkSession, "_active_sessions", [])
        if not active_sessions:
            return

        target_storages = [
            getattr(session, "_storage", None) for session in active_sessions
        ]
        target_storages = [s for s in target_storages if s is not None]
        if not target_storages:
            return

        # Only sync when this storage is detached from all active sessions.
        if self in target_storages:
            return

        for session in list(active_sessions):
            try:
                target_storage = getattr(session, "_storage", None)
                if target_storage is None or target_storage is self:
                    continue

                # Ensure schema and table exist on the target storage
                if not target_storage.schema_exists(schema_name):
                    target_storage.create_schema(schema_name)
                if not target_storage.table_exists(schema_name, table_name):
                    target_storage.create_table(schema_name, table_name, schema.fields)

                # Insert data into target storage without re-synchronizing
                reset_sync = hasattr(target_storage, "_syncing")
                if reset_sync:
                    target_storage._syncing = True
                try:
                    target_storage.insert_data(schema_name, table_name, data)
                finally:
                    if reset_sync:
                        target_storage._syncing = False
            except Exception:
                # Best effort: ignore propagation failures to avoid breaking the writer
                continue

    def create_temp_view(self, name: str, dataframe: Any) -> None:
        """Create a temporary view from a DataFrame.

        Args:
            name: Name of the temporary view.
            dataframe: DataFrame to create view from.
        """
        # Create a schema and table for the temporary view
        schema = "default"
        self.create_schema(schema)

        # Convert DataFrame data to table format
        # Materialize the DataFrame if it has lazy operations
        if hasattr(dataframe, "_materialize_if_lazy"):
            dataframe = dataframe._materialize_if_lazy()

        data = list(dataframe.data)  # Ensure it's a list
        schema_obj = dataframe.schema

        # Create the table - pass fields, not the schema object
        fields = schema_obj.fields if hasattr(schema_obj, "fields") else schema_obj

        # Create the table using the public API
        self.create_table(schema, name, fields)

        # Insert the data using the public API
        self.insert_data(schema, name, data)
