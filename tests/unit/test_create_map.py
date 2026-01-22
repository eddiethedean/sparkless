"""Tests for F.create_map() function.

This module tests the create_map() function which creates a MapType column
from key-value pairs.
"""

import pytest

from sparkless import SparkSession
from sparkless.functions import F


@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder.appName("test_create_map").getOrCreate()


class TestCreateMap:
    """Test suite for F.create_map() function."""

    def test_create_map_with_literals(self, spark):
        """Test create_map with literal keys and column values."""
        df = spark.createDataFrame([{"val1": "a", "val2": 1}])
        result = df.select(
            F.create_map(
                F.lit("key1"), F.col("val1"),
                F.lit("key2"), F.col("val2")
            ).alias("map_col")
        )
        row = result.first()

        assert row is not None
        assert row["map_col"] == {"key1": "a", "key2": 1}

    def test_create_map_with_column_values(self, spark):
        """Test create_map with literal keys and column values (common use case)."""
        df = spark.createDataFrame([
            {"first": "Alice", "last": "Smith"},
            {"first": "Bob", "last": "Jones"},
        ])
        result = df.select(
            F.create_map(
                F.lit("first_name"), F.col("first"),
                F.lit("last_name"), F.col("last"),
            ).alias("map_col")
        )
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["map_col"] == {"first_name": "Alice", "last_name": "Smith"}
        assert rows[1]["map_col"] == {"first_name": "Bob", "last_name": "Jones"}

    def test_create_map_multiple_pairs(self, spark):
        """Test create_map with multiple key-value pairs."""
        df = spark.createDataFrame([{"a": 1, "b": 2, "c": 3}])
        result = df.select(
            F.create_map(
                F.lit("x"), F.col("a"),
                F.lit("y"), F.col("b"),
                F.lit("z"), F.col("c"),
            ).alias("map_col")
        )
        row = result.first()

        assert row is not None
        assert row["map_col"] == {"x": 1, "y": 2, "z": 3}

    def test_create_map_with_null_values(self, spark):
        """Test create_map handles null values correctly."""
        from sparkless.spark_types import StructType, StructField, StringType

        schema = StructType([
            StructField("val1", StringType()),
            StructField("val2", StringType()),
        ])
        df = spark.createDataFrame([{"val1": "a", "val2": None}], schema=schema)
        result = df.select(
            F.create_map(
                F.lit("key1"), F.col("val1"),
                F.lit("key2"), F.col("val2")
            ).alias("map_col")
        )
        row = result.first()

        assert row is not None
        assert row["map_col"]["key1"] == "a"
        assert row["map_col"]["key2"] is None

    def test_create_map_validation_odd_args(self, spark):
        """Test create_map rejects odd number of arguments."""
        with pytest.raises(ValueError, match="even number"):
            F.create_map(F.lit("key1"))

    def test_create_map_validation_empty(self, spark):
        """Test create_map rejects empty arguments."""
        with pytest.raises(ValueError, match="even number"):
            F.create_map()

    def test_create_map_in_withcolumn(self, spark):
        """Test create_map works with withColumn."""
        df = spark.createDataFrame([{"name": "Alice", "age": 25}])
        result = df.withColumn(
            "info",
            F.create_map(
                F.lit("name"), F.col("name"),
                F.lit("age"), F.col("age"),
            )
        )
        row = result.first()

        assert row is not None
        assert row["info"] == {"name": "Alice", "age": 25}

    def test_create_map_after_filter(self, spark):
        """Test create_map works after filter operation."""
        df = spark.createDataFrame([
            {"val1": "a", "val2": 1},
            {"val1": "b", "val2": 2},
        ])
        result = df.filter(F.col("val2") > 1).select(
            F.create_map(
                F.lit("key"), F.col("val1"),
            ).alias("map_col")
        )
        row = result.first()

        assert row is not None
        assert row["map_col"] == {"key": "b"}

    def test_create_map_all_literals(self, spark):
        """Test create_map with all literal values."""
        df = spark.createDataFrame([{"dummy": 1}])
        result = df.select(
            F.create_map(
                F.lit("static_key"), F.lit("static_value"),
            ).alias("map_col")
        )
        row = result.first()

        assert row is not None
        assert row["map_col"] == {"static_key": "static_value"}
