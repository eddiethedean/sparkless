"""
Tests for Robin-format logical plan serialization.

These tests exercise sparkless.dataframe.robin_plan.to_robin_plan on a small
subset of operations to ensure the structure matches the documented contract in
docs/internal/robin_plan_contract.md.
"""

import json

import pytest

from sparkless.sql import SparkSession, functions as F
from sparkless.dataframe.robin_plan import to_robin_plan


class TestRobinPlan:
    @pytest.fixture
    def spark(self):
        """Create SparkSession for testing."""
        session = SparkSession("RobinPlanTest")
        try:
            yield session
        finally:
            session.stop()

    def test_filter_select_limit_robin_plan(self, spark):
        """Robin plan for filter -> select -> limit has expected ops and keys."""
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35},
        ]
        df = (
            spark.createDataFrame(data)
            .filter(F.col("age") > 25)
            .select("name", "age")
            .limit(2)
        )
        plan = to_robin_plan(df)
        assert len(plan) == 3

        # filter
        assert plan[0]["op"] == "filter"
        assert "payload" in plan[0]
        cond = plan[0]["payload"]["condition"]
        assert isinstance(cond, dict)
        assert cond["op"] == "gt"
        assert cond["left"] == {"col": "age"}
        assert cond["right"] == {"lit": 25}

        # select
        assert plan[1]["op"] == "select"
        cols = plan[1]["payload"]["columns"]
        assert isinstance(cols, list)
        assert cols[0] == {"col": "name"}
        assert cols[1] == {"col": "age"}

        # limit
        assert plan[2]["op"] == "limit"
        assert plan[2]["payload"]["n"] == 2

    def test_robin_plan_is_json_serializable(self, spark):
        """Robin plan round-trips through json.dumps/loads."""
        data = [{"x": 1, "y": 2}]
        df = spark.createDataFrame(data).filter(F.col("x") == 1).select("x").limit(1)
        plan = to_robin_plan(df)
        dumped = json.dumps(plan, default=str)
        loaded = json.loads(dumped)
        assert len(loaded) == len(plan)
        for i, entry in enumerate(plan):
            assert loaded[i]["op"] == entry["op"]
            assert "payload" in loaded[i]

