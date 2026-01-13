"""
Assertion service for DataFrame operations.

This service provides assertion methods using composition instead of mixin inheritance.
"""

from typing import Any, Dict, List, TYPE_CHECKING, cast

from ...spark_types import StructType

if TYPE_CHECKING:
    from ..dataframe import DataFrame
    from ..protocols import SupportsDataFrameOps


class AssertionService:
    """Service providing assertion operations for DataFrame testing."""

    def __init__(self, df: "DataFrame"):
        """Initialize assertion service with DataFrame instance."""
        self._df = df

    def assert_has_columns(self, expected_columns: List[str]) -> None:
        """Assert that DataFrame has the expected columns."""
        from ..assertions.assertions import DataFrameAssertions

        return DataFrameAssertions.assert_has_columns(
            cast("SupportsDataFrameOps", self._df), expected_columns
        )

    def assert_row_count(self, expected_count: int) -> None:
        """Assert that DataFrame has the expected row count."""
        from ..assertions.assertions import DataFrameAssertions

        return DataFrameAssertions.assert_row_count(
            cast("SupportsDataFrameOps", self._df), expected_count
        )

    def assert_schema_matches(self, expected_schema: StructType) -> None:
        """Assert that DataFrame schema matches the expected schema."""
        from ..assertions.assertions import DataFrameAssertions

        return DataFrameAssertions.assert_schema_matches(
            cast("SupportsDataFrameOps", self._df), expected_schema
        )

    def assert_data_equals(self, expected_data: List[Dict[str, Any]]) -> None:
        """Assert that DataFrame data equals the expected data."""
        from ..assertions.assertions import DataFrameAssertions

        return DataFrameAssertions.assert_data_equals(
            cast("SupportsDataFrameOps", self._df), expected_data
        )
