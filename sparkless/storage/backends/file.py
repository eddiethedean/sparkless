"""
File-based storage backend.

This module provides a file-based storage implementation using JSON files.
"""

import json
import os
from typing import Any, Dict, List, Optional, Union
from ...core.interfaces.storage import IStorageManager, ITable
from ...core.types.schema import ISchema
from sparkless.spark_types import StructType, StructField


class FileTable(ITable):
    """File-based table implementation."""

    def __init__(self, name: str, schema: StructType, file_path: str):
        """Initialize file table.

        Args:
            name: Table name.
            schema: Table schema.
            file_path: Path to table data file.
        """
        self._name = name
        self._schema = schema
        self.file_path = file_path
        self._metadata = {
            "created_at": "2024-01-01T00:00:00Z",
            "row_count": 0,
            "schema_version": "1.0",
        }
        self._ensure_file_exists()

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

    def _ensure_file_exists(self) -> None:
        """Ensure the table data file exists."""
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        if not os.path.exists(self.file_path):
            with open(self.file_path, "w") as f:
                json.dump([], f)

    def _load_data(self) -> List[Dict[str, Any]]:
        """Load data from file.

        Returns:
            List of data rows.
        """
        try:
            with open(self.file_path) as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _save_data(self, data: List[Dict[str, Any]]) -> None:
        """Save data to file.

        Args:
            data: Data to save.
        """
        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=2)

    def insert_data(self, data: List[Dict[str, Any]], mode: str = "append") -> None:
        """Insert data into table.

        Args:
            data: Data to insert.
            mode: Insert mode ("append", "overwrite", "ignore").
        """
        if not data:
            return

        current_data = self._load_data()

        if mode == "overwrite":
            current_data = data
        elif mode == "append":
            current_data.extend(data)
        elif mode == "ignore" and not current_data:
            # Only insert if table is empty
            current_data = data

        self._save_data(current_data)
        self._metadata["row_count"] = len(current_data)

    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query data from table.

        Args:
            filter_expr: Optional filter expression.

        Returns:
            List of data rows.
        """
        data = self._load_data()

        if filter_expr is None:
            return data

        # Simple filter implementation
        # In a real implementation, this would parse and evaluate the filter expression
        return data

    def get_schema(self) -> StructType:
        """Get table schema.

        Returns:
            Table schema.
        """
        return self.schema

    def get_metadata(self) -> Dict[str, Any]:
        """Get table metadata.

        Returns:
            Table metadata.
        """
        data = self._load_data()
        metadata = self._metadata.copy()
        metadata["row_count"] = len(data)
        return metadata

    def insert(self, data: List[Dict[str, Any]]) -> None:
        """Insert data into table."""
        self.insert_data(data)

    def query(self, **filters: Any) -> List[Dict[str, Any]]:
        """Query data from table."""
        return self.query_data()

    def count(self) -> int:
        """Count rows in table."""
        return len(self._load_data())

    def truncate(self) -> None:
        """Truncate table."""
        self._save_data([])
        self._metadata["row_count"] = 0

    def drop(self) -> None:
        """Drop table."""
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
        self._metadata.clear()


class FileSchema(ISchema):
    """File-based schema implementation."""

    def __init__(self, name: str, base_path: str):
        """Initialize file schema.

        Args:
            name: Schema name.
            base_path: Base path for schema files.
        """
        self.name = name
        self.base_path = os.path.join(base_path, name)
        self.tables: Dict[str, FileTable] = {}
        os.makedirs(self.base_path, exist_ok=True)

    def create_table(
        self, table: str, columns: Union[List[StructField], StructType]
    ) -> None:
        """Create a new table in this schema.

        Args:
            table: Name of the table.
            columns: Table columns definition.
        """
        schema = StructType(columns) if isinstance(columns, list) else columns

        table_path = os.path.join(self.base_path, f"{table}.json")
        self.tables[table] = FileTable(table, schema, table_path)

    def table_exists(self, table: str) -> bool:
        """Check if table exists in this schema.

        Args:
            table: Name of the table.

        Returns:
            True if table exists, False otherwise.
        """
        table_path = os.path.join(self.base_path, f"{table}.json")
        return os.path.exists(table_path)

    def drop_table(self, table: str) -> None:
        """Drop a table from this schema.

        Args:
            table: Name of the table.
        """
        table_path = os.path.join(self.base_path, f"{table}.json")
        if os.path.exists(table_path):
            os.remove(table_path)

        if table in self.tables:
            del self.tables[table]

    def list_tables(self) -> List[str]:
        """List all tables in this schema.

        Returns:
            List of table names.
        """
        if not os.path.exists(self.base_path):
            return []

        tables = []
        for filename in os.listdir(self.base_path):
            if filename.endswith(".json"):
                tables.append(filename[:-5])  # Remove .json extension

        return tables

    # ISchema interface implementation
    @property
    def fields(self) -> List[Any]:
        """Get schema fields."""
        return []

    def add_field(self, field: Any) -> None:
        """Add field to schema."""
        pass

    def remove_field(self, field_name: str) -> None:
        """Remove field from schema."""
        pass

    def get_field(self, field_name: str) -> Optional[Any]:
        """Get field by name."""
        return None

    def field_names(self) -> List[str]:
        """Get field names."""
        return []

    def field_types(self) -> Dict[str, Any]:
        """Get field types."""
        return {}

    def __eq__(self, other: Any) -> bool:
        """Check equality with another schema."""
        if not isinstance(other, FileSchema):
            return False
        return self.name == other.name

    def __hash__(self) -> int:
        """Get hash for schema."""
        return hash(self.name)

    def __str__(self) -> str:
        """Get string representation."""
        return f"FileSchema(name={self.name})"

    def __repr__(self) -> str:
        """Get representation."""
        return f"FileSchema(name={self.name})"


class FileStorageManager(IStorageManager):
    """File-based storage manager implementation."""

    def __init__(self, base_path: str = "sparkless_storage"):
        """Initialize file storage manager.

        Args:
            base_path: Base path for storage files.
        """
        self.base_path = base_path
        self.schemas: Dict[str, FileSchema] = {}
        # Create default schema
        self.schemas["default"] = FileSchema("default", base_path)

    def create_schema(self, schema: str) -> None:
        """Create a new schema.

        Args:
            schema: Name of the schema to create.
        """
        if schema not in self.schemas:
            self.schemas[schema] = FileSchema(schema, self.base_path)

    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists.

        Args:
            schema: Name of the schema to check.

        Returns:
            True if schema exists, False otherwise.
        """
        return schema in self.schemas

    def drop_schema(self, schema_name: str, cascade: bool = False) -> None:
        """Drop a schema.

        Args:
            schema_name: Name of the schema to drop.
            cascade: Whether to cascade the drop operation.
        """
        if schema_name in self.schemas and schema_name != "default":
            # Remove schema directory
            schema_path = os.path.join(self.base_path, schema_name)
            if os.path.exists(schema_path):
                import shutil

                shutil.rmtree(schema_path)
            del self.schemas[schema_name]

    def list_schemas(self) -> List[str]:
        """List all schemas.

        Returns:
            List of schema names.
        """
        return list(self.schemas.keys())

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            True if table exists, False otherwise.
        """
        if schema not in self.schemas:
            return False
        return self.schemas[schema].table_exists(table)

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        fields: Union[List[StructField], StructType],
    ) -> None:
        """Create a new table.

        Args:
            schema_name: Name of the schema.
            table_name: Name of the table.
            fields: Table fields definition.
        """
        if schema_name not in self.schemas:
            self.create_schema(schema_name)

        self.schemas[schema_name].create_table(table_name, fields)

    def drop_table(self, schema_name: str, table_name: str) -> None:
        """Drop a table.

        Args:
            schema_name: Name of the schema.
            table_name: Name of the table.
        """
        if schema_name in self.schemas:
            self.schemas[schema_name].drop_table(table_name)

    def insert_data(
        self, schema_name: str, table_name: str, data: List[Dict[str, Any]]
    ) -> None:
        """Insert data into table.

        Args:
            schema_name: Name of the schema.
            table_name: Name of the table.
            data: Data to insert.
        """
        if (
            schema_name in self.schemas
            and table_name in self.schemas[schema_name].tables
        ):
            self.schemas[schema_name].tables[table_name].insert_data(data)

    def query_data(
        self, schema_name: str, table_name: str, **filters: Any
    ) -> List[Dict[str, Any]]:
        """Query data from table.

        Args:
            schema_name: Name of the schema.
            table_name: Name of the table.
            **filters: Optional filter parameters.

        Returns:
            List of data rows.
        """
        if (
            schema_name in self.schemas
            and table_name in self.schemas[schema_name].tables
        ):
            return self.schemas[schema_name].tables[table_name].query_data()
        return []

    def query_table(
        self, schema: str, table: str, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query data from table.

        Args:
            schema: Name of the schema.
            table: Name of the table.
            filter_expr: Optional filter expression.

        Returns:
            List of data rows.
        """
        if schema in self.schemas and table in self.schemas[schema].tables:
            return self.schemas[schema].tables[table].query_data(filter_expr)
        return []

    def get_table_schema(self, schema_name: str, table_name: str) -> StructType:
        """Get table schema.

        Args:
            schema_name: Name of the schema.
            table_name: Name of the table.

        Returns:
            Table schema.
        """
        if (
            schema_name in self.schemas
            and table_name in self.schemas[schema_name].tables
        ):
            return self.schemas[schema_name].tables[table_name].get_schema()
        # Return empty schema if table doesn't exist
        return StructType([])

    def get_data(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all data from table.

        Args:
            schema: Name of the schema.
            table: Name of the table.

        Returns:
            List of data rows.
        """
        return self.query_table(schema, table)

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
        data = dataframe.data
        schema_obj = dataframe.schema

        # Create the table
        self.create_table(schema, name, schema_obj)

        # Insert the data
        self.insert_data(schema, name, data)

    def list_tables(self, schema_name: Optional[str] = None) -> List[str]:
        """List tables in schema.

        Args:
            schema_name: Name of the schema. If None, list tables in all schemas.

        Returns:
            List of table names.
        """
        if schema_name is None:
            # List tables from all schemas
            all_tables = []
            for schema in self.schemas.values():
                all_tables.extend(schema.list_tables())
            return all_tables

        if schema_name not in self.schemas:
            return []
        return self.schemas[schema_name].list_tables()

    def get_table_metadata(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get table metadata including Delta-specific fields."""
        if schema_name not in self.schemas:
            return {}
        if table_name not in self.schemas[schema_name].tables:
            return {}
        return self.schemas[schema_name].tables[table_name].get_metadata()

    def update_table_metadata(
        self, schema_name: str, table_name: str, metadata_updates: Dict[str, Any]
    ) -> None:
        """Update table metadata fields."""
        if (
            schema_name in self.schemas
            and table_name in self.schemas[schema_name].tables
        ):
            table_obj = self.schemas[schema_name].tables[table_name]
            table_obj._metadata.update(metadata_updates)

    def close(self) -> None:
        """Close storage backend and clean up resources.

        For file-based storage, this is a no-op as files are managed per operation.
        """
        pass
