"""
Unit tests for Issue #327: orderBy() missing ascending parameter.

Tests the orderBy() method with ascending parameter support.
"""

import pytest
from sparkless.sql import SparkSession
from sparkless import functions as F


class TestIssue327OrderByAscending:
    """Test orderBy() with ascending parameter."""

    def test_orderby_ascending_true(self):
        """Test orderBy with ascending=True."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "AAA"},
                    {"Name": "Bob", "StringValue": "ZZZ"},
                    {"Name": "Charlie", "StringValue": "MMM"},
                ]
            )

            result = df.orderBy("StringValue", ascending=True)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["StringValue"] == "AAA"
            assert rows[1]["StringValue"] == "MMM"
            assert rows[2]["StringValue"] == "ZZZ"
        finally:
            spark.stop()

    def test_orderby_ascending_false(self):
        """Test orderBy with ascending=False."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "AAA"},
                    {"Name": "Bob", "StringValue": "ZZZ"},
                    {"Name": "Charlie", "StringValue": "MMM"},
                ]
            )

            result = df.orderBy("StringValue", ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["StringValue"] == "ZZZ"
            assert rows[1]["StringValue"] == "MMM"
            assert rows[2]["StringValue"] == "AAA"
        finally:
            spark.stop()

    def test_orderby_default_ascending(self):
        """Test orderBy without ascending parameter (defaults to True)."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "AAA"},
                    {"Name": "Bob", "StringValue": "ZZZ"},
                    {"Name": "Charlie", "StringValue": "MMM"},
                ]
            )

            result = df.orderBy("StringValue")
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["StringValue"] == "AAA"
            assert rows[1]["StringValue"] == "MMM"
            assert rows[2]["StringValue"] == "ZZZ"
        finally:
            spark.stop()

    def test_orderby_numeric_ascending(self):
        """Test orderBy with numeric column and ascending=True."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            result = df.orderBy("Value", ascending=True)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["Value"] == 5
            assert rows[1]["Value"] == 10
            assert rows[2]["Value"] == 20
        finally:
            spark.stop()

    def test_orderby_numeric_descending(self):
        """Test orderBy with numeric column and ascending=False."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            result = df.orderBy("Value", ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["Value"] == 20
            assert rows[1]["Value"] == 10
            assert rows[2]["Value"] == 5
        finally:
            spark.stop()

    def test_orderby_multiple_columns_ascending(self):
        """Test orderBy with multiple columns and ascending=True."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Category": "A", "Value": 10},
                    {"Name": "Bob", "Category": "A", "Value": 5},
                    {"Name": "Charlie", "Category": "B", "Value": 20},
                ]
            )

            result = df.orderBy("Category", "Value", ascending=True)
            rows = result.collect()

            assert len(rows) == 3
            # First by Category (A, A, B), then by Value within same Category
            assert rows[0]["Category"] == "A"
            assert rows[0]["Value"] == 5
            assert rows[1]["Category"] == "A"
            assert rows[1]["Value"] == 10
            assert rows[2]["Category"] == "B"
            assert rows[2]["Value"] == 20
        finally:
            spark.stop()

    def test_orderby_multiple_columns_descending(self):
        """Test orderBy with multiple columns and ascending=False."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Category": "A", "Value": 10},
                    {"Name": "Bob", "Category": "A", "Value": 5},
                    {"Name": "Charlie", "Category": "B", "Value": 20},
                ]
            )

            result = df.orderBy("Category", "Value", ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            # Both columns in descending order
            assert rows[0]["Category"] == "B"
            assert rows[0]["Value"] == 20
            assert rows[1]["Category"] == "A"
            assert rows[1]["Value"] == 10
            assert rows[2]["Category"] == "A"
            assert rows[2]["Value"] == 5
        finally:
            spark.stop()

    def test_orderby_with_column_object(self):
        """Test orderBy with Column object and ascending parameter."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            result = df.orderBy(F.col("Value"), ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["Value"] == 20
            assert rows[1]["Value"] == 10
            assert rows[2]["Value"] == 5
        finally:
            spark.stop()

    def test_sort_with_ascending_parameter(self):
        """Test sort() alias with ascending parameter."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "AAA"},
                    {"Name": "Bob", "StringValue": "ZZZ"},
                    {"Name": "Charlie", "StringValue": "MMM"},
                ]
            )

            result = df.sort("StringValue", ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["StringValue"] == "ZZZ"
            assert rows[1]["StringValue"] == "MMM"
            assert rows[2]["StringValue"] == "AAA"
        finally:
            spark.stop()

    def test_orderby_with_null_values(self):
        """Test orderBy with null values and ascending parameter."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": None},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            result = df.orderBy("Value", ascending=True)
            rows = result.collect()

            assert len(rows) == 3
            # Nulls should be last (PySpark default behavior)
            assert rows[0]["Value"] == 10
            assert rows[1]["Value"] == 20
            assert rows[2]["Value"] is None
        finally:
            spark.stop()

    def test_orderby_backward_compatibility(self):
        """Test that orderBy without ascending still works (backward compatibility)."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            # Should work without ascending parameter (defaults to True)
            result = df.orderBy("Value")
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["Value"] == 5
            assert rows[1]["Value"] == 10
            assert rows[2]["Value"] == 20
        finally:
            spark.stop()
