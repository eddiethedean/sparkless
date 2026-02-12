"""
Schema registry for managing schemas as JSON metadata files.

This module handles saving and loading table schemas as JSON files,
replacing SQL schema management with file-based storage.
"""

import json
import os
from typing import Optional
from sparkless.spark_types import (
    StructType,
    StructField,
    ArrayType,
    MapType,
    DecimalType,
    DataType,
)


class SchemaRegistry:
    """Manages schema storage as JSON metadata files."""

    def __init__(self, storage_path: str):
        """Initialize schema registry.

        Args:
            storage_path: Base path for storing schemas
        """
        self.storage_path = storage_path

    def _get_schema_path(self, schema_name: str, table_name: str) -> str:
        """Get path to schema JSON file.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            Path to schema file
        """
        schema_dir = os.path.join(self.storage_path, schema_name)
        return os.path.join(schema_dir, f"{table_name}.schema.json")

    def save_schema(
        self, schema_name: str, table_name: str, schema: StructType
    ) -> None:
        """Save schema to JSON file.

        Args:
            schema_name: Schema name
            table_name: Table name
            schema: StructType to save
        """
        # Skip saving if using in-memory storage
        if self.storage_path == ":memory:":
            return

        schema_path = self._get_schema_path(schema_name, table_name)
        schema_dir = os.path.dirname(schema_path)
        os.makedirs(schema_dir, exist_ok=True)

        # Convert schema to JSON-serializable format
        schema_data = {
            "fields": [
                {
                    "name": field.name,
                    "type": field.dataType.__class__.__name__,
                    "nullable": field.nullable,
                    "metadata": getattr(field, "metadata", {}),
                }
                for field in schema.fields
            ]
        }

        # Handle special types
        for i, field in enumerate(schema.fields):
            if isinstance(field.dataType, ArrayType):
                schema_data["fields"][i]["elementType"] = (
                    field.dataType.element_type.__class__.__name__
                )
            elif isinstance(field.dataType, MapType):
                schema_data["fields"][i]["keyType"] = (
                    field.dataType.key_type.__class__.__name__
                )
                schema_data["fields"][i]["valueType"] = (
                    field.dataType.value_type.__class__.__name__
                )
            elif isinstance(field.dataType, DecimalType):
                schema_data["fields"][i]["precision"] = field.dataType.precision
                schema_data["fields"][i]["scale"] = field.dataType.scale

        with open(schema_path, "w") as f:
            json.dump(schema_data, f, indent=2, default=str)

    def load_schema(self, schema_name: str, table_name: str) -> Optional[StructType]:
        """Load schema from JSON file.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            StructType if schema exists, None otherwise
        """
        # Skip loading if using in-memory storage
        if self.storage_path == ":memory:":
            return None

        schema_path = self._get_schema_path(schema_name, table_name)
        if not os.path.exists(schema_path):
            return None

        with open(schema_path) as f:
            schema_data = json.load(f)

        # Import types dynamically
        from sparkless.spark_types import (
            StringType,
            IntegerType,
            LongType,
            DoubleType,
            FloatType,
            BooleanType,
            DateType,
            TimestampType,
            TimestampNTZType,
            DecimalType,
            BinaryType,
            ArrayType,
            MapType,
            ShortType,
            ByteType,
            NullType,
        )

        type_map = {
            "StringType": StringType,
            "IntegerType": IntegerType,
            "LongType": LongType,
            "DoubleType": DoubleType,
            "FloatType": FloatType,
            "BooleanType": BooleanType,
            "DateType": DateType,
            "TimestampType": TimestampType,
            "TimestampNTZType": TimestampNTZType,
            "DecimalType": DecimalType,
            "BinaryType": BinaryType,
            "ArrayType": ArrayType,
            "MapType": MapType,
            "ShortType": ShortType,
            "ByteType": ByteType,
            "NullType": NullType,
        }

        fields = []
        for field_data in schema_data["fields"]:
            type_name = field_data["type"]
            type_class = type_map.get(type_name)
            if not type_class:
                raise ValueError(f"Unknown type: {type_name}")

            # Handle special types
            data_type: DataType
            if type_name == "ArrayType":
                element_type_name = field_data.get("elementType", "StringType")
                element_type = type_map.get(element_type_name, StringType)()
                data_type = ArrayType(element_type=element_type)
            elif type_name == "MapType":
                key_type_name = field_data.get("keyType", "StringType")
                value_type_name = field_data.get("valueType", "StringType")
                key_type = type_map.get(key_type_name, StringType)()
                value_type = type_map.get(value_type_name, StringType)()
                data_type = MapType(key_type=key_type, value_type=value_type)
            elif type_name == "DecimalType":
                precision = field_data.get("precision", 10)
                scale = field_data.get("scale", 0)
                data_type = DecimalType(precision=precision, scale=scale)
            else:
                data_type = type_class()

            field = StructField(
                field_data["name"],
                data_type,
                nullable=field_data.get("nullable", True),
                metadata=field_data.get("metadata", {}),
            )
            fields.append(field)

        return StructType(fields)

    def schema_exists(self, schema_name: str, table_name: str) -> bool:
        """Check if schema file exists.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            True if schema file exists, False otherwise
        """
        # In-memory storage doesn't have schema files
        if self.storage_path == ":memory:":
            return False

        schema_path = self._get_schema_path(schema_name, table_name)
        return os.path.exists(schema_path)

    def delete_schema(self, schema_name: str, table_name: str) -> None:
        """Delete schema file.

        Args:
            schema_name: Schema name
            table_name: Table name
        """
        # Skip deletion if using in-memory storage
        if self.storage_path == ":memory:":
            return

        schema_path = self._get_schema_path(schema_name, table_name)
        if os.path.exists(schema_path):
            os.remove(schema_path)
