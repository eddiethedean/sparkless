"""
Tests for issue #286: AggregateFunction arithmetic operations.

PySpark allows arithmetic operations on aggregate functions (e.g., F.countDistinct() - 1).
This test verifies that Sparkless supports the same operations.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue286AggregateFunctionArithmetic:
    """Test arithmetic operations on aggregate functions."""

    def test_count_distinct_minus_one(self):
        """Test subtracting from countDistinct aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            # Subtract 1 from countDistinct - should work without TypeError
            result = df.groupBy("Name").agg(
                (F.countDistinct("Value") - 1).alias("count_minus_one")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice has 2 distinct values (1, 2), so countDistinct - 1 = 1
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_minus_one"] == 1

            # Bob has 1 distinct value (3), so countDistinct - 1 = 0
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["count_minus_one"] == 0

            # Verify column name
            assert "count_minus_one" in result.columns
        finally:
            spark.stop()

    def test_count_plus_one(self):
        """Test adding to count aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            result = df.groupBy("Name").agg((F.count("Value") + 1).alias("count_plus_one"))

            rows = result.collect()
            assert len(rows) == 2

            # Alice has 2 rows, so count + 1 = 3
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_plus_one"] == 3

            # Bob has 1 row, so count + 1 = 2
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["count_plus_one"] == 2
        finally:
            spark.stop()

    def test_sum_multiply(self):
        """Test multiplying sum aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            result = df.groupBy("Name").agg((F.sum("Value") * 2).alias("sum_times_two"))

            rows = result.collect()
            assert len(rows) == 2

            # Alice: sum = 3, so 3 * 2 = 6
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["sum_times_two"] == 6

            # Bob: sum = 3, so 3 * 2 = 6
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["sum_times_two"] == 6
        finally:
            spark.stop()

    def test_avg_divide(self):
        """Test dividing avg aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                    {"Name": "Bob", "Value": 30},
                ]
            )

            result = df.groupBy("Name").agg((F.avg("Value") / 2).alias("avg_divided_by_two"))

            rows = result.collect()
            assert len(rows) == 2

            # Alice: avg = 15, so 15 / 2 = 7.5
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["avg_divided_by_two"] == 7.5

            # Bob: avg = 30, so 30 / 2 = 15.0
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["avg_divided_by_two"] == 15.0
        finally:
            spark.stop()

    def test_max_modulo(self):
        """Test modulo operation on max aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 7},
                    {"Name": "Alice", "Value": 8},
                    {"Name": "Bob", "Value": 9},
                ]
            )

            result = df.groupBy("Name").agg((F.max("Value") % 3).alias("max_mod_three"))

            rows = result.collect()
            assert len(rows) == 2

            # Alice: max = 8, so 8 % 3 = 2
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["max_mod_three"] == 2

            # Bob: max = 9, so 9 % 3 = 0
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["max_mod_three"] == 0
        finally:
            spark.stop()

    def test_reverse_operations(self):
        """Test reverse arithmetic operations (e.g., 1 - countDistinct)."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            # Test reverse subtraction: 10 - countDistinct
            result = df.groupBy("Name").agg((10 - F.countDistinct("Value")).alias("ten_minus_count"))

            rows = result.collect()
            assert len(rows) == 2

            # Alice: 10 - 2 = 8
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["ten_minus_count"] == 8

            # Bob: 10 - 1 = 9
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["ten_minus_count"] == 9
        finally:
            spark.stop()

    def test_chained_arithmetic(self):
        """Test chained arithmetic operations on aggregate functions."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            # Test: (countDistinct - 1) * 2
            result = df.groupBy("Name").agg(
                ((F.countDistinct("Value") - 1) * 2).alias("count_minus_one_times_two")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: (2 - 1) * 2 = 2
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_minus_one_times_two"] == 2

            # Bob: (1 - 1) * 2 = 0
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["count_minus_one_times_two"] == 0
        finally:
            spark.stop()

    def test_multiple_aggregate_arithmetic(self):
        """Test multiple aggregate functions with arithmetic in same aggregation."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            result = df.groupBy("Name").agg(
                (F.countDistinct("Value") - 1).alias("count_minus_one"),
                (F.count("Value") + 1).alias("count_plus_one"),
                (F.sum("Value") * 2).alias("sum_times_two"),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Verify all columns exist
            assert "count_minus_one" in result.columns
            assert "count_plus_one" in result.columns
            assert "sum_times_two" in result.columns

            # Verify Alice's values
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_minus_one"] == 1
            assert alice_row["count_plus_one"] == 3
            assert alice_row["sum_times_two"] == 6
        finally:
            spark.stop()
