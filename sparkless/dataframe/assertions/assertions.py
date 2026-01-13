"""
DataFrame Assertion Utilities

This module provides assertion methods for testing DataFrames.
Extracted from dataframe.py to improve organization and maintainability.
"""

from typing import Any, Dict, List, TYPE_CHECKING

from ..protocols import SupportsDataFrameOps

if TYPE_CHECKING:
    from ...spark_types import StructType


class DataFrameAssertions:
    """Provides assertion methods for DataFrame testing."""

    @staticmethod
    def assert_has_columns(
        df: SupportsDataFrameOps, expected_columns: List[str]
    ) -> None:
        """Assert that DataFrame has the expected columns.

        Args:
            df: DataFrame to check
            expected_columns: List of expected column names

        Raises:
            AssertionError: If any expected columns are missing
        """
        actual_columns = df.columns
        missing = set(expected_columns) - set(actual_columns)
        if missing:
            raise AssertionError(f"Missing columns: {sorted(missing)}")

    @staticmethod
    def assert_row_count(df: SupportsDataFrameOps, expected_count: int) -> None:
        """Assert that DataFrame has the expected row count.

        Args:
            df: DataFrame to check
            expected_count: Expected number of rows

        Raises:
            AssertionError: If row count doesn't match
        """
        actual_count = df.count()
        if actual_count != expected_count:
            raise AssertionError(f"Expected {expected_count} rows, got {actual_count}")

    @staticmethod
    def assert_schema_matches(
        df: SupportsDataFrameOps, expected_schema: "StructType"
    ) -> None:
        """Assert that DataFrame schema matches the expected schema.

        Args:
            df: DataFrame to check
            expected_schema: Expected schema

        Raises:
            AssertionError: If schemas don't match
        """
        if len(df.schema.fields) != len(expected_schema.fields):
            raise AssertionError(
                f"Schema field count mismatch: {len(df.schema.fields)} != {len(expected_schema.fields)}"
            )
        for a, b in zip(df.schema.fields, expected_schema.fields):
            if a.name != b.name or a.dataType.__class__ != b.dataType.__class__:
                raise AssertionError(f"Schema mismatch: {a} != {b}")

    @staticmethod
    def assert_data_equals(
        df: SupportsDataFrameOps, expected_data: List[Dict[str, Any]]
    ) -> None:
        """Assert that DataFrame data equals the expected data.

        Args:
            df: DataFrame to check
            expected_data: Expected data as list of dictionaries

        Raises:
            AssertionError: If data doesn't match
        """
        actual = [r.asDict() for r in df.collect()]
        if actual != expected_data:
            raise AssertionError(f"Data mismatch: {actual} != {expected_data}")
