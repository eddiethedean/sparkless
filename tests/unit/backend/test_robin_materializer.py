"""Tests for Robin materializer _expression_to_robin (alias, literal-on-left, etc.)."""

from __future__ import annotations

from typing import Any

import pytest

from sparkless.backend.factory import BackendFactory
from sparkless.functions import F
from tests.fixtures.spark_backend import BackendType, get_backend_type

_HAS_ROBIN = BackendFactory._robin_available()  # type: ignore[attr-defined]


@pytest.mark.unit
@pytest.mark.skipif(
    not _HAS_ROBIN, reason="Robin backend requires robin-sparkless to be installed"
)
class TestRobinMaterializerExpressionTranslation:
    """Test _expression_to_robin supports alias and literal-on-left for withColumn (robin-sparkless 0.4.0+)."""

    def teardown_method(self) -> None:
        BackendFactory._robin_available_cache = None

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_alias_expression_robin(self, spark: Any) -> None:
        """WithColumn with col.alias('x') is supported when Robin backend is used."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        # Expression: double "a" and alias as "doubled"
        df = spark.createDataFrame([{"a": 1}, {"a": 2}, {"a": 3}])
        result = df.withColumn("doubled", (F.col("a") * 2).alias("doubled")).collect()
        assert len(result) == 3
        assert result[0]["doubled"] == 2
        assert result[1]["doubled"] == 4
        assert result[2]["doubled"] == 6

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_literal_plus_column_robin(self, spark: Any) -> None:
        """WithColumn with lit(2) + col('x') (literal on left) is supported when Robin backend is used."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([{"x": 10}, {"x": 20}])
        result = df.withColumn("plus_two", F.lit(2) + F.col("x")).collect()
        assert len(result) == 2
        assert result[0]["plus_two"] == 12
        assert result[1]["plus_two"] == 22

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_literal_times_column_robin(self, spark: Any) -> None:
        """WithColumn with lit(3) * col('x') (literal on left) is supported when Robin backend is used."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([{"x": 5}, {"x": 7}])
        result = df.withColumn("triple", F.lit(3) * F.col("x")).collect()
        assert len(result) == 2
        assert result[0]["triple"] == 15
        assert result[1]["triple"] == 21

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_filter_like_robin(self, spark: Any) -> None:
        """Filter with F.col('name').like('al%%') is supported when Robin backend is used (#469)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([("alice",), ("bob",), ("alex",)], ["name"])
        result = df.filter(F.col("name").like("al%")).collect()
        assert len(result) == 2
        names = sorted(r["name"] for r in result)
        assert names == ["alex", "alice"]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_filter_isnull_robin(self, spark: Any) -> None:
        """Filter with F.col('value').isNull() is supported when Robin backend is used (#469)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "value"])
        result = df.filter(F.col("value").isNull()).collect()
        assert len(result) == 1
        assert result[0]["id"] == 2 and result[0]["value"] is None

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_filter_isnotnull_robin(self, spark: Any) -> None:
        """Filter with F.col('value').isNotNull() is supported when Robin backend is used (#469)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "value"])
        result = df.filter(F.col("value").isNotNull()).collect()
        assert len(result) == 2
        ids = sorted(r["id"] for r in result)
        assert ids == [1, 3]
