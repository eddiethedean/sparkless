"""
Tests for issue #360: input_file_name() function.

PySpark supports F.input_file_name() which returns the path of the file being read.
Sparkless returns empty string in mock (no file path context).

Works with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""

import pytest


class TestIssue360InputFileName:
    """Test input_file_name() (issue #360)."""

    def test_input_file_name_returns_string_column(self, spark):
        """F.input_file_name() returns a string column (empty in mock)."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                {"dataset": "dataset_a", "table": "table_1"},
                {"dataset": "dataset_b", "table": "table_2"},
            ]
        )
        result = df.withColumn("InputFileName", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 2
        assert "InputFileName" in rows[0]
        # In Sparkless mock we return empty string; PySpark returns actual path
        assert isinstance(rows[0]["InputFileName"], str)

    def test_input_file_name_exact_issue_scenario(self, spark):
        """Exact scenario from issue #360: withColumn after createDataFrame."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                ("dataset_a", "table_1"),
                ("dataset_b", "table_2"),
            ],
            ["dataset", "table"],
        )
        df = df.withColumn("InputFileName", F.input_file_name())
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["dataset"] == "dataset_a"
        assert rows[0]["table"] == "table_1"
        assert "InputFileName" in rows[0]

    def test_input_file_name_select_only(self, spark):
        """Select only input_file_name() as single column."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"a": 1}, {"a": 2}])
        result = df.select(F.input_file_name().alias("path"))
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["path"] == "" or rows[0]["path"].startswith("/") or "\\" in rows[0]["path"]
