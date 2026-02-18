from __future__ import annotations

# mypy: ignore-errors

from typing import Any, List, Tuple

import pytest

from sparkless.session import SparkSession
from tests.fixtures.spark_backend import BackendType, get_backend_type


@pytest.mark.unit
def test_robin_sql_simple_select() -> None:
    spark = SparkSession.builder.appName("RobinSQLTest").getOrCreate()  # type: ignore[union-attr]
    # Ensure we're on the Robin backend (v4 default).
    assert getattr(spark, "backend_type", None) == "robin"

    df = spark.createDataFrame(
        [
            {"id": 1, "v": 10},
            {"id": 2, "v": 20},
            {"id": 3, "v": 30},
        ]
    )
    df.createOrReplaceTempView("t_robin_sql")

    out = spark.sql("SELECT id, v FROM t_robin_sql WHERE v >= 20 ORDER BY id").collect()
    rows: List[Tuple[Any, Any]] = [(r["id"], r["v"]) for r in out]
    assert rows == [(2, 20), (3, 30)]


@pytest.mark.unit
@pytest.mark.skipif(
    get_backend_type() == BackendType.ROBIN,
    reason="Robin SQL does not support alias in aggregation SELECT (COUNT(v) AS cnt)",
)
def test_robin_sql_group_by_agg() -> None:
    spark = SparkSession.builder.appName("RobinSQLAgg").getOrCreate()  # type: ignore[union-attr]
    df = spark.createDataFrame(
        [
            {"grp": 1, "v": 10},
            {"grp": 1, "v": 20},
            {"grp": 2, "v": 30},
        ]
    )
    df.createOrReplaceTempView("t_robin_agg")

    out = spark.sql(
        "SELECT grp, COUNT(v) AS cnt FROM t_robin_agg GROUP BY grp ORDER BY grp"
    ).collect()
    rows = [(r["grp"], r["cnt"]) for r in out]
    assert rows == [(1, 2), (2, 1)]
