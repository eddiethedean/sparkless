"""
Convenience functions for data generation in Sparkless.

This module provides convenient functions for common data generation
scenarios without requiring manual class instantiation.
"""

from typing import Any, Dict, List
from ..spark_types import StructType
from .generator import MockDataGenerator


def create_test_data(
    schema: StructType, num_rows: int = 100, seed: int = 42
) -> List[Dict[str, Any]]:
    """Create test data based on schema.

    Args:
        schema: StructType defining the data structure.
        num_rows: Number of rows to generate.
        seed: Random seed for reproducible data.

    Returns:
        List of dictionaries representing the generated data.
    """
    return MockDataGenerator.create_test_data(schema, num_rows, seed)


def create_corrupted_data(
    schema: StructType,
    corruption_rate: float = 0.1,
    num_rows: int = 100,
    seed: int = 42,
) -> List[Dict[str, Any]]:
    """Create corrupted data for error testing.

    Args:
        schema: StructType defining the data structure.
        corruption_rate: Fraction of data to corrupt.
        num_rows: Number of rows to generate.
        seed: Random seed for reproducible data.

    Returns:
        List of dictionaries representing the corrupted data.
    """
    return MockDataGenerator.create_corrupted_data(
        schema, corruption_rate, num_rows, seed
    )


def create_realistic_data(
    schema: StructType, num_rows: int = 100, seed: int = 42
) -> List[Dict[str, Any]]:
    """Create realistic data with proper distributions.

    Args:
        schema: StructType defining the data structure.
        num_rows: Number of rows to generate.
        seed: Random seed for reproducible data.

    Returns:
        List of dictionaries representing the realistic data.
    """
    return MockDataGenerator.create_realistic_data(schema, num_rows, seed)
