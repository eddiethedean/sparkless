"""
Unit tests for Issue #332: Column resolution fails with cast+alias+select.

Tests that column names are correctly resolved when combining aggregation, cast, alias, and select.
"""

from sparkless.sql import SparkSession
from sparkless import functions as F
from sparkless.sql import types as T


class TestIssue332CastAliasSelect:
    """Test cast+alias+select column resolution."""

    def test_cast_alias_select_basic(self):
        """Test basic cast+alias+select scenario."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                    {"Name": "Bob", "Value": 4},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["AvgValue"] == 1.5
            assert rows[1]["Name"] == "Bob"
            assert rows[1]["AvgValue"] == 3.5
        finally:
            spark.stop()

    def test_cast_alias_select_multiple_aggregations(self):
        """Test multiple aggregations with cast+alias."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Score": 10},
                    {"Name": "Alice", "Value": 2, "Score": 20},
                    {"Name": "Bob", "Value": 3, "Score": 30},
                    {"Name": "Bob", "Value": 4, "Score": 40},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(
                    F.mean("Value").cast(T.DoubleType()).alias("AvgValue"),
                    F.sum("Score").cast(T.IntegerType()).alias("TotalScore"),
                )
                .select("Name", "AvgValue", "TotalScore")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["AvgValue"] == 1.5
            assert rows[0]["TotalScore"] == 30
        finally:
            spark.stop()

    def test_cast_alias_select_different_cast_types(self):
        """Test different cast types with alias."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                ]
            )

            # Test StringType cast
            result1 = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.StringType()).alias("AvgValueStr"))
                .select("Name", "AvgValueStr")
            )
            rows1 = result1.collect()
            assert len(rows1) == 1
            assert isinstance(rows1[0]["AvgValueStr"], str)

            # Test IntegerType cast
            result2 = (
                df.groupBy("Name")
                .agg(F.sum("Value").cast(T.IntegerType()).alias("SumValue"))
                .select("Name", "SumValue")
            )
            rows2 = result2.collect()
            assert len(rows2) == 1
            assert isinstance(rows2[0]["SumValue"], int)
        finally:
            spark.stop()

    def test_cast_alias_select_with_withcolumn(self):
        """Test cast+alias in withColumn then select."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 20},
                ]
            )

            # withColumn uses the first argument as column name, alias is ignored
            result = (
                df.withColumn("ValueDouble", F.col("Value").cast(T.DoubleType()))
                .select("Name", "ValueDouble")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert "ValueDouble" in result.columns
            assert rows[0]["ValueDouble"] == 10.0
        finally:
            spark.stop()

    def test_cast_alias_select_nested_operations(self):
        """Test nested operations: cast+alias in groupBy then select."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Category": "A"},
                    {"Name": "Alice", "Value": 2, "Category": "A"},
                    {"Name": "Bob", "Value": 3, "Category": "B"},
                    {"Name": "Bob", "Value": 4, "Category": "B"},
                ]
            )

            result = (
                df.groupBy("Name", "Category")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .select("Name", "Category", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["AvgValue"] == 1.5
        finally:
            spark.stop()

    def test_cast_alias_select_with_filter(self):
        """Test cast+alias+select with filter."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                    {"Name": "Bob", "Value": 4},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .filter(F.col("AvgValue") > 2.0)
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Bob"
            assert rows[0]["AvgValue"] == 3.5
        finally:
            spark.stop()

    def test_cast_alias_select_with_orderby(self):
        """Test cast+alias+select with orderBy."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                    {"Name": "Bob", "Value": 4},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").cast(T.DoubleType()).alias("AvgValue"))
                .orderBy("AvgValue", ascending=False)
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Bob"
            assert rows[0]["AvgValue"] == 3.5
            assert rows[1]["Name"] == "Alice"
            assert rows[1]["AvgValue"] == 1.5
        finally:
            spark.stop()

    def test_cast_alias_select_backward_compatibility(self):
        """Test that aggregation+alias without cast still works (backward compatibility)."""
        spark = SparkSession.builder.appName("issue-332").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                ]
            )

            result = (
                df.groupBy("Name")
                .agg(F.mean("Value").alias("AvgValue"))
                .select("Name", "AvgValue")
            )

            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["AvgValue"] == 1.5
        finally:
            spark.stop()
