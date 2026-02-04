"""Robin backend storage: delegates to Polars for catalog/table operations.

The robin backend uses robin_sparkless only for DataFrame materialization.
Catalog and table persistence use the same Polars storage so saveAsTable,
catalog.tableExists, etc. continue to work. A future full Robin backend could
implement IStorageManager using robin session temp views and file paths.
"""

from typing import Any, Dict, List, Optional, Union

from sparkless.core.interfaces.storage import (
    ITableMetadataLegacy,
)
from sparkless.spark_types import StructType


class RobinStorageManager:
    """Storage manager for robin backend; delegates to Polars storage."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        from sparkless.backend.polars.storage import PolarsStorageManager

        self._polars = PolarsStorageManager(db_path=db_path)

    def create_schema(self, schema_name: str) -> None:
        self._polars.create_schema(schema_name)

    def drop_schema(self, schema_name: str, cascade: bool = False) -> None:
        self._polars.drop_schema(schema_name, cascade=cascade)

    def schema_exists(self, schema_name: str) -> bool:
        return self._polars.schema_exists(schema_name)

    def list_schemas(self) -> List[str]:
        return self._polars.list_schemas()

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        fields: Union[List[Any], StructType],
    ) -> Optional[Any]:
        return self._polars.create_table(schema_name, table_name, fields)

    def drop_table(self, schema_name: str, table_name: str) -> None:
        self._polars.drop_table(schema_name, table_name)

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        return self._polars.table_exists(schema_name, table_name)

    def list_tables(self, schema_name: Optional[str] = None) -> List[str]:
        return self._polars.list_tables(schema_name)

    def get_table_schema(
        self, schema_name: str, table_name: str
    ) -> Union[Any, StructType]:
        return self._polars.get_table_schema(schema_name, table_name)

    def insert_data(
        self, schema_name: str, table_name: str, data: List[Dict[str, Any]]
    ) -> None:
        self._polars.insert_data(schema_name, table_name, data)

    def query_data(
        self, schema_name: str, table_name: str, **filters: Any
    ) -> List[Dict[str, Any]]:
        return self._polars.query_data(schema_name, table_name, **filters)

    def get_table_metadata(
        self, schema_name: str, table_name: str
    ) -> Union[ITableMetadataLegacy, Dict[str, Any]]:
        return self._polars.get_table_metadata(schema_name, table_name)

    def get_current_schema(self) -> str:
        return self._polars.get_current_schema()

    def set_current_schema(self, schema_name: str) -> None:
        self._polars.set_current_schema(schema_name)

    def get_data(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        return self._polars.get_data(schema_name, table_name)

    def create_temp_view(self, name: str, dataframe: Any) -> None:
        self._polars.create_temp_view(name, dataframe)

    def query_table(
        self, schema_name: str, table_name: str, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        return self._polars.query_table(schema_name, table_name, filter_expr)

    def update_table_metadata(
        self, schema_name: str, table_name: str, metadata_updates: Dict[str, Any]
    ) -> None:
        self._polars.update_table_metadata(schema_name, table_name, metadata_updates)
