"""
CSV serialization module.

This module provides CSV serialization and deserialization for storage.
"""

import csv
from typing import Any, Dict, List
from sparkless.spark_types import StructType, StructField


class CSVSerializer:
    """CSV serializer for storage operations."""

    @staticmethod
    def serialize_data(data: List[Dict[str, Any]], file_path: str) -> None:
        """Serialize data to CSV file.

        Args:
            data: Data to serialize.
            file_path: Path to output file.
        """
        if not data:
            return

        with open(file_path, "w", newline="") as f:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    @staticmethod
    def deserialize_data(file_path: str) -> List[Dict[str, Any]]:
        """Deserialize data from CSV file.

        Args:
            file_path: Path to input file.

        Returns:
            Deserialized data.
        """
        try:
            with open(file_path, newline="") as f:
                reader = csv.DictReader(f)
                return list(reader)
        except FileNotFoundError:
            return []

    @staticmethod
    def serialize_schema(schema: StructType, file_path: str) -> None:
        """Serialize schema to CSV file.

        Args:
            schema: Schema to serialize.
            file_path: Path to output file.
        """
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "data_type", "nullable"])

            for field in schema.fields:
                writer.writerow(
                    [field.name, type(field.dataType).__name__, field.nullable]
                )

    @staticmethod
    def deserialize_schema(file_path: str) -> StructType:
        """Deserialize schema from CSV file.

        Args:
            file_path: Path to input file.

        Returns:
            Deserialized schema.
        """
        try:
            with open(file_path, newline="") as f:
                reader = csv.DictReader(f)
                fields = []

                for row in reader:
                    data_type = CSVSerializer._create_data_type(row["data_type"])
                    field = StructField(
                        row["name"],
                        data_type,
                        row.get("nullable", "True").lower() == "true",
                    )
                    fields.append(field)

                return StructType(fields)
        except (FileNotFoundError, KeyError):
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
