"""
Robin catalog storage backend.

Delegates table/temp view storage to the robin-sparkless crate's session catalog
instead of managing tables in Python.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from ...core.interfaces.storage import IStorageManager
from ...spark_types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _schema_list_to_struct_type(schema_list: List[Dict[str, str]]) -> StructType:
    """Convert list of {name, type} from Robin to Sparkless StructType."""
    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "bigint": LongType(),
        "long": LongType(),
        "double": DoubleType(),
        "float": DoubleType(),
        "boolean": BooleanType(),
        "bool": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "datetime": TimestampType(),
    }
    fields = []
    for f in schema_list:
        name = f.get("name", "")
        type_str = (f.get("type") or "string").lower().strip()
        dtype = type_map.get(type_str, StringType())
        fields.append(StructField(name, dtype, nullable=True))
    return StructType(fields)


def _qualified_name(schema_name: str, table_name: str) -> str:
    if schema_name and schema_name != "default":
        return f"{schema_name}.{table_name}"
    return table_name


class RobinCatalogStorage(IStorageManager):
    """
    Storage manager that uses Robin's built-in session catalog for tables and temp views.

    create_temp_view, get_data, get_table_schema, table_exists, and saveAsTable
    (via create_table + insert_data) delegate to the robin-sparkless crate.
    """

    def __init__(self) -> None:
        self._pending_table: Optional[tuple[str, str, Any]] = (
            None  # (schema, table, fields)
        )

    def create_schema(self, schema_name: str) -> None:
        # Robin does not expose create_schema; no-op (catalog is session-scoped).
        pass

    def drop_schema(self, schema_name: str, cascade: bool = False) -> None:
        pass

    def schema_exists(self, schema_name: str) -> bool:
        return schema_name in ("default", "global_temp")

    def list_schemas(self) -> List[str]:
        return ["default", "global_temp"]

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        fields: Union[List[Any], StructType],
    ) -> Optional[Any]:
        # Store for insert_data (saveAsTable path)
        fields_list = fields.fields if isinstance(fields, StructType) else fields
        self._pending_table = (schema_name, table_name, fields_list)
        return None

    def drop_table(self, schema_name: str, table_name: str) -> None:
        # Robin crate has drop_temp_view / drop_saved_table; not exposed in FFI yet.
        pass

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        try:
            from sparkless.robin import get_table_via_robin

            get_table_via_robin(_qualified_name(schema_name, table_name))
            return True
        except Exception:
            return False

    def list_tables(self, schema_name: Optional[str] = None) -> List[str]:
        # Robin does not expose list_temp_view_names in our FFI yet.
        return []

    def get_table_schema(
        self, schema_name: str, table_name: str
    ) -> Union[Any, StructType]:
        from sparkless.robin import get_table_via_robin

        _, schema_list = get_table_via_robin(_qualified_name(schema_name, table_name))
        return _schema_list_to_struct_type(schema_list)

    def _fields_to_schema_list(self, fields: Any) -> List[Dict[str, str]]:
        schema_list: List[Dict[str, str]] = []
        for f in fields:
            if hasattr(f, "name") and hasattr(f, "dataType"):
                schema_list.append(
                    {
                        "name": getattr(f, "name", ""),
                        "type": getattr(f.dataType, "simpleString", lambda: "string")(),
                    }
                )
            elif isinstance(f, (list, tuple)) and len(f) >= 2:
                schema_list.append({"name": str(f[0]), "type": "string"})
            elif isinstance(f, dict):
                schema_list.append(
                    {
                        "name": str(f.get("name", "")),
                        "type": str(f.get("type", "string")),
                    }
                )
            else:
                schema_list.append({"name": "", "type": "string"})
        return schema_list

    def _data_to_dicts(self, data: List[Any]) -> List[Dict[str, Any]]:
        from sparkless.spark_types import get_row_value, row_keys

        if not data:
            return []
        out = []
        for row in data:
            if hasattr(row, "asDict"):
                out.append(dict(row.asDict()))
            elif isinstance(row, dict):
                out.append(dict(row))
            else:
                keys = row_keys(row)
                out.append({k: get_row_value(row, k) for k in keys})
        return out

    def insert_data(
        self, schema_name: str, table_name: str, data: List[Dict[str, Any]]
    ) -> None:
        from sparkless.robin import (
            register_global_temp_view_via_robin,
            save_as_table_via_robin,
        )

        data_dicts = self._data_to_dicts(data)
        if self._pending_table:
            pend_schema, pend_table, fields = self._pending_table
            self._pending_table = None
            if pend_schema == schema_name and pend_table == table_name and fields:
                schema_list = self._fields_to_schema_list(fields)
                if schema_name == "global_temp":
                    register_global_temp_view_via_robin(
                        table_name, data_dicts, schema_list
                    )
                else:
                    qualified = _qualified_name(schema_name, table_name)
                    save_as_table_via_robin(
                        qualified, data_dicts, schema_list, "overwrite"
                    )
                return
        qualified = _qualified_name(schema_name, table_name)
        if schema_name == "global_temp":
            schema_list = [
                {"name": k, "type": "string"}
                for k in (data_dicts[0] if data_dicts else [])
            ]
            register_global_temp_view_via_robin(table_name, data_dicts, schema_list)
            return
        if data_dicts:
            schema_list = [{"name": k, "type": "string"} for k in data_dicts[0]]
        else:
            schema_list = []
        save_as_table_via_robin(qualified, data_dicts, schema_list, "overwrite")

    def query_data(
        self, schema_name: str, table_name: str, **filters: Any
    ) -> List[Dict[str, Any]]:
        return self.get_data(schema_name, table_name)

    def get_table_metadata(
        self, schema_name: str, table_name: str
    ) -> Union[Any, Dict[str, Any]]:
        return {"name": table_name, "schema": schema_name}

    def get_data(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        from sparkless.robin import get_table_via_robin

        rows, _ = get_table_via_robin(_qualified_name(schema_name, table_name))
        return rows

    def create_temp_view(self, name: str, dataframe: Any) -> None:
        from sparkless.robin.schema_ser import serialize_data, serialize_schema
        from sparkless.robin import register_temp_view_via_robin

        if hasattr(dataframe, "_materialize_if_lazy"):
            dataframe = dataframe._materialize_if_lazy()
        data = list(dataframe.data)
        schema_obj = dataframe.schema
        schema_list = serialize_schema(schema_obj)
        data_serialized = serialize_data(data, schema_obj)
        register_temp_view_via_robin(name, data_serialized, schema_list)
