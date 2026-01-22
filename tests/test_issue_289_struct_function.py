"""
Tests for issue #289: struct function support.

PySpark supports the struct function for creating struct-type columns
from multiple columns. This test verifies that Sparkless supports the same.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue289StructFunction:
    """Test struct function support."""

    def test_struct_basic(self):
        """Test basic struct function usage (from issue example)."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Create a struct column from Name and Value
            result = df.withColumn("new_struct", F.struct("Name", "Value"))

            rows = result.collect()
            assert len(rows) == 2

            # Check that struct column exists and has correct structure
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "new_struct" in alice_row
            struct_val = alice_row["new_struct"]
            assert struct_val is not None
            # Struct should be accessible as dict-like or have Name and Value fields
            assert hasattr(struct_val, "Name") or (
                isinstance(struct_val, dict) and "Name" in struct_val
            )
            assert hasattr(struct_val, "Value") or (
                isinstance(struct_val, dict) and "Value" in struct_val
            )

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert "new_struct" in bob_row
        finally:
            spark.stop()

    def test_struct_with_col_function(self):
        """Test struct function with F.col()."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Age": 25},
                    {"Name": "Bob", "Value": 2, "Age": 30},
                ]
            )

            result = df.withColumn(
                "person", F.struct(F.col("Name"), F.col("Value"), F.col("Age"))
            )

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "person" in alice_row
        finally:
            spark.stop()

    def test_struct_single_column(self):
        """Test struct function with a single column."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice"},
                    {"Name": "Bob"},
                ]
            )

            result = df.withColumn("name_struct", F.struct("Name"))

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "name_struct" in alice_row
        finally:
            spark.stop()

    def test_struct_with_nulls(self):
        """Test struct function with null values."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Age": None},
                    {"Name": "Bob", "Value": None, "Age": 30},
                ]
            )

            result = df.withColumn("person", F.struct("Name", "Value", "Age"))

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "person" in alice_row
        finally:
            spark.stop()

    def test_struct_in_select(self):
        """Test struct function in select statement."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            result = df.select(F.struct("Name", "Value").alias("new_struct"))

            rows = result.collect()
            assert len(rows) == 2
            assert "new_struct" in rows[0]
        finally:
            spark.stop()

    def test_struct_multiple_types(self):
        """Test struct function with different data types."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Score": 95.5, "Active": True},
                    {"Name": "Bob", "Value": 2, "Score": 87.0, "Active": False},
                ]
            )

            result = df.withColumn(
                "mixed", F.struct("Name", "Value", "Score", "Active")
            )

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "mixed" in alice_row
        finally:
            spark.stop()
