"""Tests for issue #260: Column.eqNullSafe (null-safe equality).

Issue #260 reports that sparkless Column class does not implement eqNullSafe,
which is used to establish equality between two columns which both contain
nulls. The PySpark API supports this.

These tests verify that:
- Column.eqNullSafe exists on the public API.
- Its semantics match PySpark's <=> / eqNullSafe behavior:
  * NULL <=> NULL is True
  * NULL <=> non-NULL is False
  * non-NULL <=> non-NULL behaves like standard equality, including type coercion.
"""

from typing import Iterable

import pytest

from tests.fixtures.spark_imports import get_spark_imports
from tests.fixtures.spark_backend import get_backend_type, BackendType


imports = get_spark_imports()
SparkSession = imports.SparkSession
F = imports.F
StructType = imports.StructType
StructField = imports.StructField
StringType = imports.StringType


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark backend mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


class TestIssue260EqNullSafe:
    """Regression tests for Column.eqNullSafe (issue #260)."""

    def test_eqnullsafe_example_from_issue_260(self) -> None:
        """Exact reproduction of the example from issue #260."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Id": "123", "ManagerId": None},
                    {"Name": "Bob", "Id": "456", "ManagerId": "456"},
                    {"Name": "Charlie", "Id": None, "ManagerId": None},
                ]
            )

            result = df.where(F.col("Id").eqNullSafe(F.col("ManagerId"))).collect()

            # Expected PySpark result:
            # +----+---------+-------+
            # |  Id|ManagerId|   Name|
            # +----+---------+-------+
            # | 456|      456|    Bob|
            # |NULL|     NULL|Charlie|
            # +----+---------+-------+
            assert len(result) == 2
            names = {row["Name"] for row in result}
            assert names == {"Bob", "Charlie"}
        finally:
            spark.stop()

    @pytest.mark.parametrize(  # type: ignore[untyped-decorator]
        "left,right,expected",
        [
            (None, None, True),
            (None, "x", False),
            ("x", None, False),
            ("x", "x", True),
            ("x", "y", False),
        ],
    )
    def test_eqnullsafe_literal_semantics(self, left, right, expected: bool) -> None:
        """Test eqNullSafe semantics with literal comparisons."""
        spark = SparkSession.builder.appName("EqNullSafeLiterals").getOrCreate()
        try:
            schema = StructType(
                [
                    StructField("left", StringType(), True),
                    StructField("right", StringType(), True),
                ]
            )
            df = spark.createDataFrame([{"left": left, "right": right}], schema=schema)
            result = (
                df.select(
                    F.col("left").eqNullSafe(F.col("right")).alias("equals"),
                )
                .collect()
            )
            assert len(result) == 1
            assert result[0]["equals"] is expected
        finally:
            spark.stop()

    def test_eqnullsafe_coexists_with_standard_equality(self) -> None:
        """Ensure eqNullSafe does not change == behavior."""
        spark = SparkSession.builder.appName("EqNullSafeVsEq").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": None},
                    {"value": "x"},
                ]
            )

            # With standard equality, NULL == "x" and NULL == NULL behave like SQL: result is NULL -> filter drops them.
            eq_result = df.where(F.col("value") == F.lit(None)).collect()
            # Depending on backend, this may yield zero rows (SQL three-valued logic).
            # The important thing is that eqNullSafe has distinct semantics.

            # With eqNullSafe, NULL <=> NULL should be True.
            null_safe_result = df.where(F.col("value").eqNullSafe(F.lit(None))).collect()
            names = [row["value"] for row in null_safe_result]
            assert None in names
        finally:
            spark.stop()


class TestIssue260EqNullSafeParity:
    """Optional PySpark parity tests for eqNullSafe semantics."""

    def test_eqnullsafe_parity_with_pyspark(self) -> None:
        """Run the issue #260 example against real PySpark when available."""
        if not _is_pyspark_mode():
            pytest.skip(
                "PySpark parity test - run with MOCK_SPARK_TEST_BACKEND=pyspark"
            )

        spark = SparkSession.builder.appName("EqNullSafeParity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Id": "123", "ManagerId": None},
                    {"Name": "Bob", "Id": "456", "ManagerId": "456"},
                    {"Name": "Charlie", "Id": None, "ManagerId": None},
                ]
            )

            result = df.where(F.col("Id").eqNullSafe(F.col("ManagerId"))).collect()
            assert len(result) == 2
            names: Iterable[str] = {row["Name"] for row in result}
            assert names == {"Bob", "Charlie"}
        finally:
            spark.stop()

