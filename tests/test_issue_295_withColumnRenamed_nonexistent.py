"""
Tests for issue #295: withColumnRenamed should treat non-existent columns as no-op.

In PySpark, renaming a non-existent column is treated as a no-op (no error,
statement is ignored). This test verifies that Sparkless matches this behavior.
"""

from sparkless.sql import SparkSession


class TestIssue295WithColumnRenamedNonexistent:
    """Test withColumnRenamed with non-existent columns (no-op behavior)."""

    def test_withColumnRenamed_nonexistent_column_no_op(self):
        """Test that renaming a non-existent column is treated as a no-op."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            # Create dataframe with timestamp strings
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Rename non-existent column - should be no-op
            result = df.withColumnRenamed("Does-Not-Exist", "Still-Does-Not-Exist")

            # Verify DataFrame is unchanged
            assert result.count() == 2
            assert set(result.columns) == {"Name", "Value"}
            assert "Does-Not-Exist" not in result.columns
            assert "Still-Does-Not-Exist" not in result.columns

            # Verify data is unchanged
            rows = result.collect()
            assert len(rows) == 2
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Value"] == 1
            assert rows[1]["Name"] == "Bob"
            assert rows[1]["Value"] == 2

            # Verify original DataFrame is unchanged
            assert df.count() == 2
            assert set(df.columns) == {"Name", "Value"}
        finally:
            spark.stop()

    def test_withColumnRenamed_existing_column_works(self):
        """Test that renaming an existing column still works correctly."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Rename existing column
            result = df.withColumnRenamed("Name", "FullName")

            # Verify rename worked
            assert result.count() == 2
            assert "FullName" in result.columns
            assert "Name" not in result.columns
            assert "Value" in result.columns

            # Verify data
            rows = result.collect()
            assert rows[0]["FullName"] == "Alice"
            assert rows[0]["Value"] == 1
            assert rows[1]["FullName"] == "Bob"
            assert rows[1]["Value"] == 2
        finally:
            spark.stop()

    def test_withColumnRenamed_case_insensitive_nonexistent(self):
        """Test that case-insensitive non-existent column is treated as no-op."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Try to rename with different case (doesn't exist)
            result = df.withColumnRenamed("DOES-NOT-EXIST", "new_name")

            # Should be no-op
            assert result.count() == 2
            assert set(result.columns) == {"Name", "Value"}
            assert "new_name" not in result.columns
        finally:
            spark.stop()

    def test_withColumnRenamed_chained_with_nonexistent(self):
        """Test chaining withColumnRenamed with both existing and non-existent columns."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Chain: rename existing, then try to rename non-existent
            result = df.withColumnRenamed("Name", "FullName").withColumnRenamed(
                "Does-Not-Exist", "Still-Does-Not-Exist"
            )

            # First rename should work, second should be no-op
            assert result.count() == 2
            assert "FullName" in result.columns
            assert "Name" not in result.columns
            assert "Value" in result.columns
            assert "Does-Not-Exist" not in result.columns
            assert "Still-Does-Not-Exist" not in result.columns

            # Verify data
            rows = result.collect()
            assert rows[0]["FullName"] == "Alice"
            assert rows[0]["Value"] == 1
        finally:
            spark.stop()

    def test_withColumnsRenamed_with_nonexistent_columns(self):
        """Test withColumnsRenamed skips non-existent columns (no-op for missing ones)."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Rename mix of existing and non-existent columns
            result = df.withColumnsRenamed(
                {
                    "Name": "FullName",  # Exists - should rename
                    "Does-Not-Exist": "Still-Does-Not-Exist",  # Doesn't exist - should skip
                }
            )

            # Only existing column should be renamed
            assert result.count() == 2
            assert "FullName" in result.columns
            assert "Name" not in result.columns
            assert "Value" in result.columns
            assert "Does-Not-Exist" not in result.columns
            assert "Still-Does-Not-Exist" not in result.columns

            # Verify data
            rows = result.collect()
            assert rows[0]["FullName"] == "Alice"
            assert rows[0]["Value"] == 1
        finally:
            spark.stop()

    def test_withColumnsRenamed_all_nonexistent_no_op(self):
        """Test withColumnsRenamed with all non-existent columns is a no-op."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Try to rename only non-existent columns
            result = df.withColumnsRenamed(
                {
                    "Does-Not-Exist-1": "New-Name-1",
                    "Does-Not-Exist-2": "New-Name-2",
                }
            )

            # Should be complete no-op
            assert result.count() == 2
            assert set(result.columns) == {"Name", "Value"}
            assert "New-Name-1" not in result.columns
            assert "New-Name-2" not in result.columns

            # Verify data unchanged
            rows = result.collect()
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Value"] == 1
        finally:
            spark.stop()

    def test_withColumnRenamed_after_operations(self):
        """Test withColumnRenamed with non-existent column after other operations."""
        spark = SparkSession.builder.appName("issue-295").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Apply filter, then try to rename non-existent column
            result = df.filter(df.Value > 1).withColumnRenamed(
                "Does-Not-Exist", "Still-Does-Not-Exist"
            )

            # Should filter correctly, no-op on rename
            assert result.count() == 1
            assert set(result.columns) == {"Name", "Value"}
            assert "Does-Not-Exist" not in result.columns

            # Verify filtered data
            rows = result.collect()
            assert rows[0]["Name"] == "Bob"
            assert rows[0]["Value"] == 2
        finally:
            spark.stop()
