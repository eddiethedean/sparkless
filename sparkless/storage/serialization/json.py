"""
JSON serialization module.

This module provides JSON serialization and deserialization for storage.
"""

import json
from typing import Any, Dict, List
from sparkless.spark_types import StructType, StructField


class JSONSerializer:
    """JSON serializer for storage operations."""

    @staticmethod
    def serialize_data(data: List[Dict[str, Any]], file_path: str) -> None:
        """Serialize data to JSON file.

        Args:
            data: Data to serialize.
            file_path: Path to output file.
        """
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    @staticmethod
    def deserialize_data(file_path: str) -> List[Dict[str, Any]]:
        """Deserialize data from JSON file.

        Args:
            file_path: Path to input file.

        Returns:
            Deserialized data.
        """
        try:
            with open(file_path) as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    @staticmethod
    def serialize_schema(schema: StructType, file_path: str) -> None:
        """Serialize schema to JSON file.

        Args:
            schema: Schema to serialize.
            file_path: Path to output file.
        """
        schema_data = {
            "fields": [
                {
                    "name": field.name,
                    "data_type": type(field.dataType).__name__,
                    "nullable": field.nullable,
                }
                for field in schema.fields
            ]
        }

        with open(file_path, "w") as f:
            json.dump(schema_data, f, indent=2)

    @staticmethod
    def deserialize_schema(file_path: str) -> StructType:
        """Deserialize schema from JSON file.

        Args:
            file_path: Path to input file.

        Returns:
            Deserialized schema.
        """
        try:
            with open(file_path) as f:
                schema_data = json.load(f)

            fields = []
            for field_data in schema_data.get("fields", []):
                # Create appropriate data type based on type name
                data_type = JSONSerializer._create_data_type(field_data["data_type"])
                field = StructField(
                    field_data["name"], data_type, field_data.get("nullable", True)
                )
                fields.append(field)

            return StructType(fields)
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            return StructType([])

    @staticmethod
    def _create_data_type(type_name: str) -> Any:
        """Create data type from type name.

        Args:
            type_name: Name of the data type.

        Returns:
            Data type instance.
        """
        from ...spark_types import (
            StringType,
            IntegerType,
            LongType,
            DoubleType,
            BooleanType,
        )

        type_mapping = {
            "StringType": StringType(),
            "IntegerType": IntegerType(),
            "LongType": LongType(),
            "DoubleType": DoubleType(),
            "BooleanType": BooleanType(),
        }

        return type_mapping.get(type_name, StringType())
