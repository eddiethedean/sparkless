"""
Tests for issue #293: explode does not explode lists as expected in withColumn.

PySpark's explode function creates a new row for each element in an array.
This test verifies that Sparkless supports the same behavior.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue293ExplodeWithColumn:
    """Test explode functionality in withColumn operations."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate a unique app name for each test to avoid conflicts in parallel execution."""
        import uuid

        return f"issue-293-{test_name}-{uuid.uuid4().hex[:8]}"

    def test_explode_in_withcolumn(self):
        """Test explode in withColumn (from issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            # Create dataframe with lists containing string values
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": ["1", "2"]},
                    {"Name": "Bob", "Value": ["2", "3"]},
                    {"Name": "Charlie", "Value": ["4", "5"]},
                ]
            )

            # Explode lists into separate rows
            df = df.withColumn("ExplodedValue", F.explode("Value"))

            rows = df.collect()

            # Should have 6 rows (2 + 2 + 2)
            assert len(rows) == 6

            # Check Alice's rows
            alice_rows = [r for r in rows if r["Name"] == "Alice"]
            assert len(alice_rows) == 2
            assert set(r["ExplodedValue"] for r in alice_rows) == {"1", "2"}
            # Original Value column should still contain the array
            assert all(r["Value"] == ["1", "2"] for r in alice_rows)

            # Check Bob's rows
            bob_rows = [r for r in rows if r["Name"] == "Bob"]
            assert len(bob_rows) == 2
            assert set(r["ExplodedValue"] for r in bob_rows) == {"2", "3"}
            assert all(r["Value"] == ["2", "3"] for r in bob_rows)

            # Check Charlie's rows
            charlie_rows = [r for r in rows if r["Name"] == "Charlie"]
            assert len(charlie_rows) == 2
            assert set(r["ExplodedValue"] for r in charlie_rows) == {"4", "5"}
            assert all(r["Value"] == ["4", "5"] for r in charlie_rows)
        finally:
            spark.stop()

    def test_explode_in_select(self):
        """Test explode in select statement."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": ["1", "2"]},
                    {"Name": "Bob", "Value": ["2", "3"]},
                ]
            )

            # Explode in select
            result = df.select("Name", "Value", F.explode("Value").alias("ExplodedValue"))

            rows = result.collect()
            assert len(rows) == 4  # 2 rows per original row

            # Check that all rows have the expected structure
            for row in rows:
                assert "Name" in row.asDict()
                assert "Value" in row.asDict()
                assert "ExplodedValue" in row.asDict()
                assert isinstance(row["Value"], list)
                assert row["ExplodedValue"] in ["1", "2", "3"]
        finally:
            spark.stop()

    def test_explode_with_integers(self):
        """Test explode with integer arrays."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Numbers": [1, 2, 3]},
                    {"Name": "Bob", "Numbers": [4, 5]},
                ]
            )

            df = df.withColumn("ExplodedNumber", F.explode("Numbers"))

            rows = df.collect()
            assert len(rows) == 5  # 3 + 2

            alice_rows = [r for r in rows if r["Name"] == "Alice"]
            assert len(alice_rows) == 3
            assert set(r["ExplodedNumber"] for r in alice_rows) == {1, 2, 3}

            bob_rows = [r for r in rows if r["Name"] == "Bob"]
            assert len(bob_rows) == 2
            assert set(r["ExplodedNumber"] for r in bob_rows) == {4, 5}
        finally:
            spark.stop()

    def test_explode_with_empty_arrays(self):
        """Test explode with empty arrays (should drop rows)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": ["1", "2"]},
                    {"Name": "Bob", "Value": []},  # Empty array
                    {"Name": "Charlie", "Value": ["3"]},
                ]
            )

            df = df.withColumn("ExplodedValue", F.explode("Value"))

            rows = df.collect()
            # Empty array row should be dropped
            assert len(rows) == 3  # 2 + 0 + 1

            # Bob's row should be dropped
            bob_rows = [r for r in rows if r["Name"] == "Bob"]
            assert len(bob_rows) == 0
        finally:
            spark.stop()

    def test_explode_with_null_arrays(self):
        """Test explode with null arrays (should drop rows for regular explode)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": ["1", "2"]},
                    {"Name": "Bob", "Value": None},  # Null array
                    {"Name": "Charlie", "Value": ["3"]},
                ]
            )

            df = df.withColumn("ExplodedValue", F.explode("Value"))

            rows = df.collect()
            # Null array row should be dropped for regular explode
            assert len(rows) == 3  # 2 + 0 + 1

            # Bob's row should be dropped
            bob_rows = [r for r in rows if r["Name"] == "Bob"]
            assert len(bob_rows) == 0
        finally:
            spark.stop()

    def test_explode_outer_with_null_arrays(self):
        """Test explode_outer with null arrays (should keep rows)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": ["1", "2"]},
                    {"Name": "Bob", "Value": None},  # Null array
                    {"Name": "Charlie", "Value": ["3"]},
                ]
            )

            df = df.withColumn("ExplodedValue", F.explode_outer("Value"))

            rows = df.collect()
            # explode_outer keeps null array rows
            assert len(rows) == 4  # 2 + 1 + 1

            # Bob's row should be kept with null ExplodedValue
            bob_rows = [r for r in rows if r["Name"] == "Bob"]
            assert len(bob_rows) == 1
            assert bob_rows[0]["ExplodedValue"] is None
        finally:
            spark.stop()

    def test_explode_with_multiple_columns(self):
        """Test explode with multiple columns preserved."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Age": 30, "Tags": ["a", "b"]},
                    {"Name": "Bob", "Age": 25, "Tags": ["c"]},
                ]
            )

            df = df.withColumn("Tag", F.explode("Tags"))

            rows = df.collect()
            assert len(rows) == 3  # 2 + 1

            alice_rows = [r for r in rows if r["Name"] == "Alice"]
            assert len(alice_rows) == 2
            assert all(r["Age"] == 30 for r in alice_rows)
            assert all(r["Name"] == "Alice" for r in alice_rows)
        finally:
            spark.stop()

    def test_explode_chained_operations(self):
        """Test explode with chained operations."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": ["1", "2"]},
                    {"Name": "Bob", "Value": ["3", "4"]},
                ]
            )

            # Chain explode with filter
            df = df.withColumn("ExplodedValue", F.explode("Value"))
            df = df.filter(F.col("ExplodedValue") > "2")

            rows = df.collect()
            # Should only have rows where ExplodedValue > "2"
            assert len(rows) == 2  # "3" and "4"
            assert all(r["ExplodedValue"] in ["3", "4"] for r in rows)
        finally:
            spark.stop()
