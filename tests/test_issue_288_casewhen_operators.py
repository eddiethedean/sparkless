"""
Tests for issue #288: CaseWhen arithmetic and logical operators.

PySpark supports arithmetic and logical operations on CaseWhen expressions
(e.g., F.when(...).otherwise(...) - F.when(...).otherwise(...)).
This test verifies that Sparkless supports the same operations.
"""

import pytest
from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue288CaseWhenOperators:
    """Test arithmetic and logical operations on CaseWhen expressions."""

    def test_casewhen_subtraction(self):
        """Test subtracting two CaseWhen expressions (from issue example)."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 1, "Value2": 2},
                    {"Name": "Bob", "Value1": 3, "Value2": 4},
                ]
            )

            # Perform a math operation between two CaseWhen expressions
            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                - F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (1) - Value2 (2) = -1
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == -1

            # Bob: Value2 (4) - Value1 (3) = 1
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 1
        finally:
            spark.stop()

    def test_casewhen_addition(self):
        """Test adding two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 1, "Value2": 2},
                    {"Name": "Bob", "Value1": 3, "Value2": 4},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                + F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (1) + Value2 (2) = 3
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 3

            # Bob: Value2 (4) + Value1 (3) = 7
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 7
        finally:
            spark.stop()

    def test_casewhen_multiplication(self):
        """Test multiplying two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 2, "Value2": 3},
                    {"Name": "Bob", "Value1": 4, "Value2": 5},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                * F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (2) * Value2 (3) = 6
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 6

            # Bob: Value2 (5) * Value1 (4) = 20
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 20
        finally:
            spark.stop()

    def test_casewhen_division(self):
        """Test dividing two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 10, "Value2": 2},
                    {"Name": "Bob", "Value1": 20, "Value2": 4},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                / F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (10) / Value2 (2) = 5.0
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 5.0

            # Bob: Value2 (4) / Value1 (20) = 0.2
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 0.2
        finally:
            spark.stop()

    def test_casewhen_modulo(self):
        """Test modulo operation on two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 10, "Value2": 3},
                    {"Name": "Bob", "Value1": 20, "Value2": 7},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                % F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (10) % Value2 (3) = 1
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 1

            # Bob: Value2 (7) % Value1 (20) = 7
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 7
        finally:
            spark.stop()

    def test_casewhen_bitwise_or(self):
        """Test bitwise OR operation on two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 5, "Value2": 3},
                    {"Name": "Bob", "Value1": 10, "Value2": 6},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                | F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (5) | Value2 (3) = 7
            # 5 = 101, 3 = 011, 5 | 3 = 111 = 7
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 7

            # Bob: Value2 (6) | Value1 (10) = 14
            # 6 = 110, 10 = 1010, 6 | 10 = 1110 = 14
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 14
        finally:
            spark.stop()

    @pytest.mark.skip(
        reason="Bitwise NOT (~) not yet supported in Polars backend translation"
    )
    def test_casewhen_bitwise_not(self):
        """Test bitwise NOT operation (unary ~) on CaseWhen expression.

        Note: This test is skipped as the Polars backend doesn't yet support
        the ~ operator translation. The operator is implemented in CaseWhen
        but requires backend support to execute.
        """
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 5},
                    {"Name": "Bob", "Value1": 10},
                ]
            )

            result = df.withColumn(
                "result",
                ~F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(F.lit(0)),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: ~Value1 (5) = ~5 = -6 (in two's complement)
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == -6

            # Bob: ~0 = -1
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == -1
        finally:
            spark.stop()

    def test_casewhen_with_literal(self):
        """Test CaseWhen expression with literal value."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 5},
                    {"Name": "Bob", "Value": 10},
                ]
            )

            # CaseWhen + literal
            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value")).otherwise(F.lit(0))
                + 10,
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value (5) + 10 = 15
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 15

            # Bob: 0 + 10 = 10
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 10
        finally:
            spark.stop()

    def test_casewhen_reverse_operations(self):
        """Test reverse operations (literal op CaseWhen)."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 5},
                    {"Name": "Bob", "Value": 10},
                ]
            )

            # Literal - CaseWhen (reverse subtraction)
            result = df.withColumn(
                "result",
                100
                - F.when(F.col("Name") == "Alice", F.col("Value")).otherwise(F.lit(0)),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: 100 - Value (5) = 95
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 95

            # Bob: 100 - 0 = 100
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 100
        finally:
            spark.stop()

    def test_casewhen_chained_operations(self):
        """Test chained arithmetic operations on CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 2, "Value2": 3},
                    {"Name": "Bob", "Value1": 4, "Value2": 5},
                ]
            )

            # (CaseWhen1 - CaseWhen2) * 2
            result = df.withColumn(
                "result",
                (
                    F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                        F.col("Value2")
                    )
                    - F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                        F.col("Value1")
                    )
                )
                * 2,
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: (Value1 (2) - Value2 (3)) * 2 = -2
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == -2

            # Bob: (Value2 (5) - Value1 (4)) * 2 = 2
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 2
        finally:
            spark.stop()

    def test_casewhen_with_nulls(self):
        """Test CaseWhen operations with null values."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 1, "Value2": None},
                    {"Name": "Bob", "Value1": None, "Value2": 4},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                + F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (1) + Value2 (None) = None
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] is None

            # Bob: Value2 (4) + Value1 (None) = None
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] is None
        finally:
            spark.stop()
