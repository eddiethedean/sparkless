"""
Integration tests for case sensitivity configuration.

Tests that spark.sql.caseSensitive configuration works correctly
across all DataFrame operations.
"""

import pytest
from sparkless.sql import SparkSession, functions as F
from sparkless.core.exceptions.analysis import AnalysisException


class TestCaseSensitivityConfiguration:
    """Test case sensitivity configuration and its effects."""

    def test_default_case_insensitive(self):
        """Test that default behavior is case-insensitive."""
        spark = SparkSession("TestApp")
        assert spark.conf.is_case_sensitive() is False

        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])
        result = df.select("name").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Alice"
        spark.stop()

    def test_case_sensitive_mode(self):
        """Test case-sensitive mode."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        assert spark.conf.is_case_sensitive() is True

        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

        # Case-sensitive: "name" should not match "Name"
        with pytest.raises(Exception):  # Should raise column not found
            df.select("name").collect()

        # Exact case should work
        result = df.select("Name").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Alice"

        spark.stop()

    def test_case_insensitive_select(self):
        """Test select with case-insensitive matching."""
        spark = SparkSession("TestApp")
        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

        # Various case combinations should all work
        result1 = df.select("name").collect()
        result2 = df.select("NAME").collect()
        result3 = df.select("Name").collect()

        assert len(result1) == len(result2) == len(result3) == 1
        assert result1[0]["Name"] == result2[0]["Name"] == result3[0]["Name"] == "Alice"

        spark.stop()

    def test_case_insensitive_filter(self):
        """Test filter with case-insensitive matching."""
        spark = SparkSession("TestApp")
        df = spark.createDataFrame(
            [{"Name": "Alice", "Age": 25}, {"Name": "Bob", "Age": 30}]
        )

        result = df.filter(F.col("name") == "Alice").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Alice"

        result = df.filter(F.col("NAME") == "Bob").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Bob"

        spark.stop()

    def test_case_insensitive_groupBy(self):
        """Test groupBy with case-insensitive matching."""
        spark = SparkSession("TestApp")
        df = spark.createDataFrame(
            [
                {"Dept": "IT", "Salary": 100},
                {"Dept": "IT", "Salary": 200},
                {"Dept": "HR", "Salary": 150},
            ]
        )

        result = df.groupBy("dept").agg(F.sum("salary").alias("total")).collect()
        assert len(result) == 2

        spark.stop()

    def test_case_insensitive_join(self):
        """Test join with case-insensitive matching."""
        spark = SparkSession("TestApp")
        df1 = spark.createDataFrame([{"ID": 1, "Name": "Alice"}])
        df2 = spark.createDataFrame([{"id": 1, "Dept": "IT"}])

        result = df1.join(df2, on="id", how="inner").collect()
        assert len(result) == 1
        assert "Name" in result[0].__dict__["_data_dict"]
        assert "Dept" in result[0].__dict__["_data_dict"]

        spark.stop()

    def test_case_sensitive_mode_exact_match_required(self):
        """Test that case-sensitive mode requires exact matches."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

        # These should all fail in case-sensitive mode
        with pytest.raises(Exception):
            df.select("name").collect()

        with pytest.raises(Exception):
            df.select("NAME").collect()

        with pytest.raises(Exception):
            df.filter(F.col("name") == "Alice").collect()

        # Only exact match should work
        result = df.select("Name").collect()
        assert len(result) == 1

        spark.stop()

    def test_case_insensitive_unionByName(self):
        """Test unionByName with case-insensitive matching."""
        spark = SparkSession("TestApp")
        df1 = spark.createDataFrame([{"Name": "Alice", "Age": 25}])
        df2 = spark.createDataFrame([{"NAME": "Bob", "AGE": 30}])

        result = df1.unionByName(df2).collect()
        assert len(result) == 2
        # Both rows should have same column names (from df1)
        assert "Name" in [f.name for f in result[0]._schema.fields]
        assert "Age" in [f.name for f in result[0]._schema.fields]

        spark.stop()

    def test_ambiguity_detection(self):
        """Test that ambiguity is detected and raises error."""
        spark = SparkSession("TestApp")

        # Create DataFrame with ambiguous column names (different case)
        # Note: This might not be possible to create directly, but if we can,
        # it should be detected during resolution
        # Actually, Python dicts can't have duplicate keys, so this is tricky
        # Let's test with schema instead
        from sparkless.spark_types import StructType, StructField, StringType

        schema = StructType(
            [
                StructField("Name", StringType()),
                StructField("name", StringType()),
            ]
        )

        # This should work, but resolution should detect ambiguity
        df = spark.createDataFrame([{"Name": "Alice"}], schema=schema)

        # When trying to select "name", it should detect ambiguity
        with pytest.raises(AnalysisException, match="Ambiguous"):
            df.select("name").collect()

        spark.stop()
