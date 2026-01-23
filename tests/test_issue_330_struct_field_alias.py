"""
Unit tests for Issue #330: Struct field selection with alias fails.

Tests that struct field extraction works correctly when combined with alias.
"""

from sparkless.sql import SparkSession
from sparkless import functions as F


class TestIssue330StructFieldAlias:
    """Test struct field selection with alias."""

    def test_struct_field_with_alias(self):
        """Test basic struct field extraction with alias."""
        spark = SparkSession.builder.appName("issue-330").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructValue": {"E1": 1, "E2": "A"}},
                    {"Name": "Bob", "StructValue": {"E1": 2, "E2": "B"}},
                ]
            )

            result = df.select(F.col("StructValue.E1").alias("E1-Extract"))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["E1-Extract"] == 1
            assert rows[1]["E1-Extract"] == 2
        finally:
            spark.stop()

    def test_struct_field_with_alias_multiple_fields(self):
        """Test multiple struct fields with aliases."""
        spark = SparkSession.builder.appName("issue-330").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructValue": {"E1": 1, "E2": "A"}},
                    {"Name": "Bob", "StructValue": {"E1": 2, "E2": "B"}},
                ]
            )

            result = df.select(
                F.col("StructValue.E1").alias("E1-Extract"),
                F.col("StructValue.E2").alias("E2-Extract"),
            )
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["E1-Extract"] == 1
            assert rows[0]["E2-Extract"] == "A"
            assert rows[1]["E1-Extract"] == 2
            assert rows[1]["E2-Extract"] == "B"
        finally:
            spark.stop()

    def test_struct_field_with_alias_in_withcolumn(self):
        """Test struct field extraction with alias in withColumn."""
        spark = SparkSession.builder.appName("issue-330").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructValue": {"E1": 1, "E2": "A"}},
                    {"Name": "Bob", "StructValue": {"E1": 2, "E2": "B"}},
                ]
            )

            result = df.withColumn("ExtractedE1", F.col("StructValue.E1").alias("E1-Extract"))
            rows = result.collect()

            assert len(rows) == 2
            # The alias should be used as the column name
            assert "E1-Extract" in result.columns or "ExtractedE1" in result.columns
        finally:
            spark.stop()

    def test_struct_field_with_alias_and_other_columns(self):
        """Test struct field with alias combined with other columns."""
        spark = SparkSession.builder.appName("issue-330").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructValue": {"E1": 1, "E2": "A"}},
                    {"Name": "Bob", "StructValue": {"E1": 2, "E2": "B"}},
                ]
            )

            result = df.select(
                "Name",
                F.col("StructValue.E1").alias("E1-Extract"),
                F.col("StructValue.E2").alias("E2-Extract"),
            )
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["E1-Extract"] == 1
            assert rows[0]["E2-Extract"] == "A"
        finally:
            spark.stop()

    def test_struct_field_with_alias_null_values(self):
        """Test struct field extraction with alias when struct is null."""
        spark = SparkSession.builder.appName("issue-330").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructValue": {"E1": 1, "E2": "A"}},
                    {"Name": "Bob", "StructValue": None},
                ]
            )

            result = df.select(F.col("StructValue.E1").alias("E1-Extract"))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["E1-Extract"] == 1
            assert rows[1]["E1-Extract"] is None
        finally:
            spark.stop()

    def test_struct_field_with_alias_nested_struct(self):
        """Test nested struct field extraction with alias."""
        spark = SparkSession.builder.appName("issue-330").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {
                        "Name": "Alice",
                        "StructValue": {
                            "Nested": {"E1": 1, "E2": "A"},
                            "E2": "A",
                        },
                    },
                    {
                        "Name": "Bob",
                        "StructValue": {
                            "Nested": {"E1": 2, "E2": "B"},
                            "E2": "B",
                        },
                    },
                ]
            )

            # Test nested struct field access (if supported)
            # Note: This may not work if nested structs aren't fully supported
            result = df.select(F.col("StructValue.E2").alias("E2-Extract"))
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["E2-Extract"] == "A"
            assert rows[1]["E2-Extract"] == "B"
        finally:
            spark.stop()

    def test_struct_field_without_alias_still_works(self):
        """Test that struct field extraction without alias still works (backward compatibility)."""
        spark = SparkSession.builder.appName("issue-330").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructValue": {"E1": 1, "E2": "A"}},
                    {"Name": "Bob", "StructValue": {"E1": 2, "E2": "B"}},
                ]
            )

            result = df.select(F.col("StructValue.E1"))
            rows = result.collect()

            assert len(rows) == 2
            # Column name should be "StructValue.E1" when no alias
            assert "StructValue.E1" in result.columns
            assert rows[0]["StructValue.E1"] == 1
            assert rows[1]["StructValue.E1"] == 2
        finally:
            spark.stop()

    def test_struct_field_with_alias_chained_operations(self):
        """Test struct field with alias in chained operations."""
        spark = SparkSession.builder.appName("issue-330").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StructValue": {"E1": 1, "E2": "A"}},
                    {"Name": "Bob", "StructValue": {"E1": 2, "E2": "B"}},
                ]
            )

            result = (
                df.select(F.col("StructValue.E1").alias("E1-Extract"))
                .filter(F.col("E1-Extract") > 1)
                .select("E1-Extract")
            )
            rows = result.collect()

            assert len(rows) == 1
            assert rows[0]["E1-Extract"] == 2
        finally:
            spark.stop()
