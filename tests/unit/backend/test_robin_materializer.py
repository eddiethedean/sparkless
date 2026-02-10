"""Tests for Robin materializer _expression_to_robin (alias, literal-on-left, etc.)."""

from __future__ import annotations

from typing import Any

import pytest

from sparkless.backend.factory import BackendFactory
from sparkless.functions import F
from tests.fixtures.spark_backend import BackendType, get_backend_type


@pytest.mark.unit
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
