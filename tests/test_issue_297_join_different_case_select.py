"""
Tests for issue #297: Join with different case columns and select with third case.

PySpark allows selecting columns with a different case when multiple columns
with different cases exist after a join. It picks the first matching column.
"""

from sparkless.sql import SparkSession


class TestIssue297JoinDifferentCaseSelect:
    """Test join with different case columns and select with third case."""

    def test_join_different_case_select_third_case(self):
        """Test joining DataFrames with different case keys and selecting with third case."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            # Create two dataframes with different case keys
            df1 = spark.createDataFrame(
                [
                    {"name": "Alice", "Value1": 1},
                    {"name": "Bob", "Value1": 2},
                ]
            )

            df2 = spark.createDataFrame(
                [
                    {"NAME": "Alice", "Value2": 1},
                    {"NAME": "Bob", "Value2": 2},
                ]
            )

            # Join the dataframes and apply select with different case
            df = df1.join(df2, on="Name", how="left").select("NaMe", "Value1", "Value2")

            # Should not raise an exception - PySpark picks the first match
            result = df.collect()

            # Verify results
            assert len(result) == 2
            assert result[0]["NaMe"] == "Alice"
            assert result[0]["Value1"] == 1
            assert result[0]["Value2"] == 1
            assert result[1]["NaMe"] == "Bob"
            assert result[1]["Value1"] == 2
            assert result[1]["Value2"] == 2

            # Verify column names in result
            assert "NaMe" in df.columns
            assert "Value1" in df.columns
            assert "Value2" in df.columns
        finally:
            spark.stop()

    def test_join_different_case_select_left_column(self):
        """Test that selecting with different case picks the left DataFrame's column."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"name": "Alice", "value": 1}])
            df2 = spark.createDataFrame([{"NAME": "Bob", "value": 2}])

            # Join on different case column names
            df = df1.join(df2, on="Name", how="left")

            # After join, both "name" and "NAME" exist
            # Selecting with different case should pick the first one (from left DataFrame)
            result = df.select("NaMe", "value").collect()

            assert len(result) == 1
            # Should pick "name" from left DataFrame (first match)
            assert result[0]["NaMe"] == "Alice"
        finally:
            spark.stop()

    def test_join_same_case_no_ambiguity(self):
        """Test that joins with same case columns work normally."""
        spark = SparkSession.builder.appName("issue-297").getOrCreate()
        try:
            df1 = spark.createDataFrame([{"name": "Alice", "value1": 1}])
            df2 = spark.createDataFrame([{"name": "Alice", "value2": 2}])  # Same name to match

            df = df1.join(df2, on="name", how="left")
            result = df.select("name", "value1", "value2").collect()

            assert len(result) == 1
            assert result[0]["name"] == "Alice"
            assert result[0]["value1"] == 1
            assert result[0]["value2"] == 2
        finally:
            spark.stop()
