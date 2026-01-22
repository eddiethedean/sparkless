"""Tests for DataFrame.first() method.

This module tests the first() method which returns the first row of a DataFrame.
"""

import pytest

from sparkless import SparkSession
from sparkless.spark_types import StructType, StructField, StringType


@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder.appName("test_first").getOrCreate()


class TestDataFrameFirst:
    """Test suite for DataFrame.first() method."""

    def test_first_returns_single_row(self, spark):
        """Test that first() returns a single Row object, not a list."""
        df = spark.createDataFrame([{"name": "Alice"}, {"name": "Bob"}])
        result = df.first()

        # Should be a single Row, not a list
        assert result is not None
        assert not isinstance(result, list)
        assert result["name"] == "Alice"

    def test_first_empty_dataframe_returns_none(self, spark):
        """Test that first() returns None for empty DataFrame."""
        schema = StructType([StructField("name", StringType())])
        df = spark.createDataFrame([], schema=schema)
        result = df.first()

        assert result is None

    def test_first_with_multiple_columns(self, spark):
        """Test first() with multiple columns."""
        data = [
            {"name": "Alice", "age": 25, "city": "NYC"},
            {"name": "Bob", "age": 30, "city": "LA"},
        ]
        df = spark.createDataFrame(data)
        result = df.first()

        assert result["name"] == "Alice"
        assert result["age"] == 25
        assert result["city"] == "NYC"

    def test_first_after_filter(self, spark):
        """Test first() after a filter operation."""
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35},
        ]
        df = spark.createDataFrame(data)
        filtered = df.filter("age > 25")
        result = filtered.first()

        assert result["name"] == "Bob"
        assert result["age"] == 30

    def test_first_after_orderby(self, spark):
        """Test first() after orderBy operation."""
        data = [
            {"name": "Charlie", "value": 3},
            {"name": "Alice", "value": 1},
            {"name": "Bob", "value": 2},
        ]
        df = spark.createDataFrame(data)
        ordered = df.orderBy("value")
        result = ordered.first()

        assert result["name"] == "Alice"
        assert result["value"] == 1

    def test_first_after_select(self, spark):
        """Test first() after select operation."""
        data = [{"name": "Alice", "age": 25}]
        df = spark.createDataFrame(data)
        selected = df.select("name")
        result = selected.first()

        assert result["name"] == "Alice"
        # age should not be in the result
        with pytest.raises(KeyError):
            _ = result["age"]

    def test_first_vs_head_difference(self, spark):
        """Test that first() and head() have different return types.

        first() returns a single Row (or None)
        head() without argument returns a single Row
        head(n) returns a list
        """
        data = [{"name": "Alice"}, {"name": "Bob"}]
        df = spark.createDataFrame(data)

        first_result = df.first()
        head_result = df.head()
        head_n_result = df.head(1)

        # first() returns single Row
        assert not isinstance(first_result, list)
        assert first_result["name"] == "Alice"

        # head() without arg returns single Row
        assert not isinstance(head_result, list)
        assert head_result["name"] == "Alice"

        # head(n) returns list
        assert isinstance(head_n_result, list)
        assert len(head_n_result) == 1
        assert head_n_result[0]["name"] == "Alice"

    def test_first_with_null_values(self, spark):
        """Test first() works correctly with null values in data."""
        data = [
            {"name": None, "value": 1},
            {"name": "Bob", "value": 2},
        ]
        df = spark.createDataFrame(data)
        result = df.first()

        assert result["name"] is None
        assert result["value"] == 1

    def test_first_single_row_dataframe(self, spark):
        """Test first() on DataFrame with exactly one row."""
        df = spark.createDataFrame([{"value": 42}])
        result = df.first()

        assert result is not None
        assert result["value"] == 42
