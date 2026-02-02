"""
Tests for issue #359: NAHandler.drop method.

PySpark supports df.na.drop() for dropping rows with null values.
This test verifies that Sparkless supports the same operation.
"""

import pytest
from sparkless.sql import SparkSession


class TestIssue359NADrop:
    """Test NAHandler.drop method (issue #359)."""

    def test_na_drop_with_subset_exact_issue_scenario(self):
        """Exact scenario from issue #359: drop rows with None in specified column."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
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
        finally:
            spark.stop()

    def test_na_drop_no_subset_drops_any_null(self):
        """na.drop() with no subset drops rows with any null (how='any' default)."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
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
        finally:
            spark.stop()

    def test_na_drop_subset_as_string(self):
        """na.drop(subset='Value') with single column as string."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
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
        finally:
            spark.stop()

    def test_na_drop_subset_as_tuple(self):
        """na.drop(subset=(col1, col2)) with tuple of columns."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
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
        finally:
            spark.stop()

    def test_na_drop_how_all(self):
        """na.drop(how='all') drops only rows where all values are null."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
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
        finally:
            spark.stop()

    def test_na_drop_thresh(self):
        """na.drop(thresh=n) keeps rows with at least n non-null values."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
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
        finally:
            spark.stop()

    def test_na_drop_no_nulls_returns_same(self):
        """na.drop() on DataFrame with no nulls returns same row count."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )
            result = df.na.drop()
            rows = result.collect()
            assert len(rows) == 2
        finally:
            spark.stop()

    def test_na_drop_equivalent_to_dropna(self):
        """na.drop() produces same result as dropna()."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
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
        finally:
            spark.stop()
