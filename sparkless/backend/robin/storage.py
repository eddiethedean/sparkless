"""Robin backend storage: file-based catalog and table persistence.

In v4 the Robin backend uses robin_sparkless for DataFrame materialization and
a file-based storage backend (no Polars) for catalog/table operations so
saveAsTable, catalog.tableExists, etc. continue to work.
"""

import os
import tempfile
from typing import Any, Dict, List, Optional, Union

from sparkless.core.interfaces.storage import (
    ITableMetadataLegacy,
)
from sparkless.spark_types import StructType
from sparkless.storage.backends.file import FileStorageManager


class RobinStorageManager:
    """Storage manager for Robin backend; uses file-based storage (no Polars)."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._db_path = db_path or ":memory:"
        if self._db_path == ":memory:":
            base_path = os.path.join(tempfile.gettempdir(), "sparkless_robin")
        else:
            base_path = self._db_path
        self._file = FileStorageManager(base_path=base_path)

    @property
    def db_path(self) -> str:
        """Database path for persistence; ':memory:' uses a temp directory."""
        return self._db_path

    def create_schema(self, schema_name: str) -> None:
        self._file.create_schema(schema_name)

    def drop_schema(self, schema_name: str, cascade: bool = False) -> None:
        self._file.drop_schema(schema_name, cascade=cascade)

    def schema_exists(self, schema_name: str) -> bool:
        return self._file.schema_exists(schema_name)

    def list_schemas(self) -> List[str]:
        return self._file.list_schemas()

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        fields: Union[List[Any], StructType],
    ) -> Optional[Any]:
        self._file.create_table(schema_name, table_name, fields)
        return None

    def drop_table(self, schema_name: str, table_name: str) -> None:
        self._file.drop_table(schema_name, table_name)

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        return self._file.table_exists(schema_name, table_name)

    def list_tables(self, schema_name: Optional[str] = None) -> List[str]:
        return self._file.list_tables(schema_name)

    def get_table_schema(
        self, schema_name: str, table_name: str
    ) -> Union[Any, StructType]:
        return self._file.get_table_schema(schema_name, table_name)

    def insert_data(
        self, schema_name: str, table_name: str, data: List[Dict[str, Any]]
    ) -> None:
        self._file.insert_data(schema_name, table_name, data)

    def query_data(
        self, schema_name: str, table_name: str, **filters: Any
    ) -> List[Dict[str, Any]]:
        return self._file.query_data(schema_name, table_name, **filters)

    def get_table_metadata(
        self, schema_name: str, table_name: str
    ) -> Union[ITableMetadataLegacy, Dict[str, Any]]:
        return self._file.get_table_metadata(schema_name, table_name)

    def get_current_schema(self) -> str:
        return self._file.get_current_schema()

    def set_current_schema(self, schema_name: str) -> None:
        self._file.set_current_schema(schema_name)

    def get_data(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        return self._file.get_data(schema_name, table_name)

    def create_temp_view(self, name: str, dataframe: Any) -> None:
        self._file.create_temp_view(name, dataframe)

    def query_table(
        self, schema_name: str, table_name: str, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        return self._file.query_table(schema_name, table_name, filter_expr)

    def update_table_metadata(
        self, schema_name: str, table_name: str, metadata_updates: Dict[str, Any]
    ) -> None:
        self._file.update_table_metadata(schema_name, table_name, metadata_updates)
