"""Tests for WindowFunction arithmetic operators.

PySpark allows arithmetic operations on window function results:
    F.percent_rank().over(window) * 100
    F.row_number().over(window) + 1

This module tests that sparkless supports these operations.
"""

from sparkless import functions as F
from sparkless.window import Window


class TestWindowFunctionArithmetic:
    """Tests for arithmetic operators on WindowFunction results."""

    def test_window_function_multiply(self, spark):
        """Test multiplying window function result by a scalar."""
        df = spark.createDataFrame(
            [
                {"dept": "A", "salary": 100},
                {"dept": "A", "salary": 200},
                {"dept": "A", "salary": 300},
            ]
        )
        window = Window.partitionBy("dept").orderBy("salary")

        result = df.select(
            F.col("salary"),
            (F.percent_rank().over(window) * 100).alias("percentile"),
        )

        rows = result.collect()
        # percent_rank: 0.0, 0.5, 1.0 -> * 100 -> 0.0, 50.0, 100.0
        assert rows[0]["percentile"] == 0.0
        assert rows[1]["percentile"] == 50.0
        assert rows[2]["percentile"] == 100.0

    def test_window_function_rmul(self, spark):
        """Test reverse multiply (scalar * window_func)."""
        df = spark.createDataFrame(
            [
                {"dept": "A", "salary": 100},
                {"dept": "A", "salary": 200},
            ]
        )
        window = Window.partitionBy("dept").orderBy("salary")

        result = df.select(
            F.col("salary"),
            (100 * F.percent_rank().over(window)).alias("percentile"),
        )

        rows = result.collect()
        assert rows[0]["percentile"] == 0.0
        assert rows[1]["percentile"] == 100.0

    def test_window_function_add(self, spark):
        """Test adding a scalar to window function result."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (F.row_number().over(window) + 10).alias("row_plus_10"),
        )

        rows = result.collect()
        assert rows[0]["row_plus_10"] == 11
        assert rows[1]["row_plus_10"] == 12
        assert rows[2]["row_plus_10"] == 13

    def test_window_function_radd(self, spark):
        """Test reverse add (scalar + window_func)."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (100 + F.row_number().over(window)).alias("hundred_plus_row"),
        )

        rows = result.collect()
        assert rows[0]["hundred_plus_row"] == 101
        assert rows[1]["hundred_plus_row"] == 102

    def test_window_function_subtract(self, spark):
        """Test subtracting a scalar from window function result."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (F.row_number().over(window) - 1).alias("zero_indexed"),
        )

        rows = result.collect()
        # row_number: 1, 2, 3 -> - 1 -> 0, 1, 2
        assert rows[0]["zero_indexed"] == 0
        assert rows[1]["zero_indexed"] == 1
        assert rows[2]["zero_indexed"] == 2

    def test_window_function_rsub(self, spark):
        """Test reverse subtract (scalar - window_func)."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (10 - F.row_number().over(window)).alias("ten_minus_row"),
        )

        rows = result.collect()
        # 10 - row_number: 10 - 1 = 9, 10 - 2 = 8, 10 - 3 = 7
        assert rows[0]["ten_minus_row"] == 9
        assert rows[1]["ten_minus_row"] == 8
        assert rows[2]["ten_minus_row"] == 7

    def test_window_function_divide(self, spark):
        """Test dividing window function result by a scalar."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}, {"val": 4}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (F.row_number().over(window) / 2).alias("row_half"),
        )

        rows = result.collect()
        # row_number: 1, 2, 3, 4 -> / 2 -> 0.5, 1.0, 1.5, 2.0
        assert rows[0]["row_half"] == 0.5
        assert rows[1]["row_half"] == 1.0
        assert rows[2]["row_half"] == 1.5
        assert rows[3]["row_half"] == 2.0

    def test_window_function_rdiv(self, spark):
        """Test reverse divide (scalar / window_func)."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}, {"val": 3}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (12 / F.row_number().over(window)).alias("twelve_div_row"),
        )

        rows = result.collect()
        # 12 / row_number: 12 / 1 = 12, 12 / 2 = 6, 12 / 3 = 4
        assert rows[0]["twelve_div_row"] == 12.0
        assert rows[1]["twelve_div_row"] == 6.0
        assert rows[2]["twelve_div_row"] == 4.0

    def test_window_function_negate(self, spark):
        """Test negating window function result."""
        df = spark.createDataFrame([{"val": 1}, {"val": 2}])
        window = Window.orderBy("val")

        result = df.select(
            F.col("val"),
            (-F.row_number().over(window)).alias("neg_row"),
        )

        rows = result.collect()
        assert rows[0]["neg_row"] == -1
        assert rows[1]["neg_row"] == -2

    def test_window_function_chained_operations(self, spark):
        """Test chaining multiple arithmetic operations."""
        df = spark.createDataFrame(
            [
                {"dept": "A", "salary": 100},
                {"dept": "A", "salary": 200},
                {"dept": "A", "salary": 300},
            ]
        )
        window = Window.partitionBy("dept").orderBy("salary")

        # (percent_rank * 100) + 1
        result = df.select(
            F.col("salary"),
            ((F.percent_rank().over(window) * 100) + 1).alias("score"),
        )

        rows = result.collect()
        # percent_rank: 0.0, 0.5, 1.0 -> * 100 + 1 -> 1.0, 51.0, 101.0
        assert rows[0]["score"] == 1.0
        assert rows[1]["score"] == 51.0
        assert rows[2]["score"] == 101.0

    def test_dense_rank_with_arithmetic(self, spark):
        """Test dense_rank with arithmetic operations."""
        df = spark.createDataFrame(
            [
                {"name": "A", "score": 100},
                {"name": "B", "score": 100},  # Same score, same rank
                {"name": "C", "score": 90},
            ]
        )
        window = Window.orderBy(F.col("score").desc())

        result = df.select(
            F.col("name"),
            F.col("score"),
            (F.dense_rank().over(window) * 10).alias("rank_score"),
        )

        rows = result.collect()
        # dense_rank: 1, 1, 2 -> * 10 -> 10, 10, 20
        # Find rows by name since order might vary for ties
        row_a = next(r for r in rows if r["name"] == "A")
        row_b = next(r for r in rows if r["name"] == "B")
        row_c = next(r for r in rows if r["name"] == "C")

        assert row_a["rank_score"] == 10
        assert row_b["rank_score"] == 10
        assert row_c["rank_score"] == 20
