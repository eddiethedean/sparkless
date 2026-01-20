"""
Regression tests for Issue #263: isnan() on string columns.

PySpark allows isnan() on string columns (always False). Sparkless' Polars backend
previously raised:
polars.exceptions.InvalidOperationError: `is_nan` operation not supported for dtype `str`
"""

from __future__ import annotations

import math

import pytest

from tests.fixtures.spark_backend import BackendType, get_backend_type
from tests.fixtures.spark_imports import get_spark_imports


def _is_pyspark_mode() -> bool:
    return bool(get_backend_type() == BackendType.PYSPARK)


imports = get_spark_imports()
F = imports.F


class TestIssue263IsnanString:
    def test_isnan_on_string_column_filter_does_not_error_and_returns_empty(
        self, spark
    ):
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": None},
                {"Name": "Bob", "Value": ""},
                {"Name": "Charlie", "Value": "123"},
            ]
        )

        result = df.filter(F.isnan(F.col("Value")))
        assert result.collect() == []

    def test_isnan_on_numeric_column_true_only_for_nan(self, spark):
        df = spark.createDataFrame(
            [
                {"id": 1, "v": float("nan")},
                {"id": 2, "v": 1.0},
                {"id": 3, "v": None},
            ]
        )

        rows = df.select("id", F.isnan(F.col("v")).alias("is_nan")).collect()
        is_nan_by_id = {r["id"]: r["is_nan"] for r in rows}

        assert is_nan_by_id[1] is True
        assert is_nan_by_id[2] is False
        assert is_nan_by_id[3] is False

        filtered = df.filter(F.isnan(F.col("v"))).select("id").collect()
        assert [r["id"] for r in filtered] == [1]

    def test_isnan_literal_matches_python_math(self, spark):
        df = spark.createDataFrame([{"x": 1}])
        rows = df.select(
            F.isnan(F.lit(float("nan"))).alias("nan"),
            F.isnan(F.lit(1.0)).alias("one"),
            F.isnan(F.lit(None)).alias("none"),
        ).collect()

        assert rows[0]["nan"] is True
        assert rows[0]["one"] is False
        assert rows[0]["none"] is False
        assert rows[0]["nan"] == math.isnan(float("nan"))

    @pytest.mark.skipif(
        not _is_pyspark_mode(),
        reason="PySpark parity test - only run with PySpark backend",
    )
    def test_isnan_string_column_pyspark_parity(self, spark):
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": None},
                {"Name": "Bob", "Value": ""},
                {"Name": "Charlie", "Value": "123"},
            ]
        )

        result = df.filter(F.isnan(F.col("Value")))
        assert result.collect() == []
