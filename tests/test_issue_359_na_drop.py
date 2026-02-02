"""
Tests for issue #359: NAHandler.drop method.

PySpark supports df.na.drop() for dropping rows with null values.
This test verifies that Sparkless supports the same operation.

Works with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""

import pytest


class TestIssue359NADrop:
    """Test NAHandler.drop method (issue #359)."""

    def test_na_drop_with_subset_exact_issue_scenario(self, spark):
        """Exact scenario from issue #359: drop rows with None in specified column."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": None},
            ]
        )
        result = df.na.drop(subset=["Value"])
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"
        assert rows[0]["Value"] == 1

    def test_na_drop_no_subset_drops_any_null(self, spark):
        """na.drop() with no subset drops rows with any null (how='any' default)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": None},
                {"Name": "Charlie", "Value": 3},
            ]
        )
        result = df.na.drop()
        rows = result.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"Alice", "Charlie"}

    def test_na_drop_subset_as_string(self, spark):
        """na.drop(subset='Value') with single column as string."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": None},
            ]
        )
        result = df.na.drop(subset="Value")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_na_drop_subset_as_tuple(self, spark):
        """na.drop(subset=(col1, col2)) with tuple of columns."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": None, "b": 2, "c": 3},
                {"a": 1, "b": None, "c": 3},
                {"a": 1, "b": 2, "c": None},
            ]
        )
        # Drop rows with null in a or b only
        result = df.na.drop(subset=("a", "b"))
        rows = result.collect()
        assert len(rows) == 2  # rows 1 and 2 have null in a or b

    def test_na_drop_how_all(self, spark):
        """na.drop(how='all') drops only rows where all values are null."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": 2},
                {"a": None, "b": 2},
                {"a": None, "b": None},
            ]
        )
        result = df.na.drop(how="all")
        rows = result.collect()
        assert len(rows) == 2  # third row is all null

    def test_na_drop_thresh(self, spark):
        """na.drop(thresh=n) keeps rows with at least n non-null values."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": 1, "b": None, "c": 3},
                {"a": None, "b": None, "c": 3},
            ]
        )
        result = df.na.drop(thresh=2)  # keep rows with >= 2 non-null
        rows = result.collect()
        assert len(rows) == 2  # third row has only 1 non-null

    def test_na_drop_no_nulls_returns_same(self, spark):
        """na.drop() on DataFrame with no nulls returns same row count."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": 2},
            ]
        )
        result = df.na.drop()
        rows = result.collect()
        assert len(rows) == 2

    def test_na_drop_equivalent_to_dropna(self, spark):
        """na.drop() produces same result as dropna()."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": None},
            ]
        )
        result_na = df.na.drop(subset=["Value"]).collect()
        result_direct = df.dropna(subset=["Value"]).collect()
        assert len(result_na) == len(result_direct) == 1
        assert result_na[0]["Name"] == result_direct[0]["Name"]
