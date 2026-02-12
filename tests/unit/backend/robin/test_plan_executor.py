"""Tests for Robin plan executor (Phase 5): robin_expr_to_column and execute_robin_plan."""

from __future__ import annotations

import pytest

from sparkless.backend.factory import BackendFactory
from sparkless.spark_types import StructField, StructType, LongType, StringType

_HAS_ROBIN = BackendFactory._robin_available()  # type: ignore[attr-defined]


@pytest.mark.unit
@pytest.mark.skipif(
    not _HAS_ROBIN, reason="Robin backend requires robin-sparkless to be installed"
)
class TestRobinPlanExecutor:
    """Phase 5: in-repo Robin plan executor."""

    def teardown_method(self) -> None:
        BackendFactory._robin_available_cache = None

    def test_execute_robin_plan_empty_plan_returns_data_as_rows(self) -> None:
        """Empty plan returns input data as Sparkless Rows."""
        from sparkless.backend.robin.plan_executor import execute_robin_plan

        data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        schema = StructType(
            [
                StructField("a", LongType()),
                StructField("b", StringType()),
            ]
        )
        plan: list = []
        rows = execute_robin_plan(data, schema, plan)
        assert len(rows) == 2
        assert rows[0]["a"] == 1 and rows[0]["b"] == "x"
        assert rows[1]["a"] == 2 and rows[1]["b"] == "y"

    def test_execute_robin_plan_filter_select_limit(self) -> None:
        """filter -> select -> limit produces correct rows."""
        from sparkless.backend.robin.plan_executor import execute_robin_plan

        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 35},
        ]
        schema = StructType(
            [
                StructField("name", StringType()),
                StructField("age", LongType()),
            ]
        )
        plan = [
            {
                "op": "filter",
                "payload": {
                    "condition": {
                        "op": "gt",
                        "left": {"col": "age"},
                        "right": {"lit": 25},
                    },
                },
            },
            {
                "op": "select",
                "payload": {
                    "columns": [{"col": "name"}, {"col": "age"}],
                },
            },
            {"op": "limit", "payload": {"n": 2}},
        ]
        rows = execute_robin_plan(data, schema, plan)
        assert len(rows) == 2
        assert rows[0]["name"] == "Bob" and rows[0]["age"] == 30
        assert rows[1]["name"] == "Charlie" and rows[1]["age"] == 35

    def test_execute_robin_plan_unsupported_op_raises(self) -> None:
        """Unsupported plan op raises ValueError so caller can fall back."""
        from sparkless.backend.robin.plan_executor import execute_robin_plan

        data = [{"a": 1}]
        schema = StructType([StructField("a", LongType())])
        plan = [{"op": "groupBy", "payload": {"columns": []}}]
        with pytest.raises(ValueError, match="Unsupported plan op"):
            execute_robin_plan(data, schema, plan)

    def test_robin_expr_to_column_col_and_lit(self) -> None:
        """robin_expr_to_column builds col and lit."""
        from sparkless.backend.robin.plan_executor import robin_expr_to_column

        col_expr = robin_expr_to_column({"col": "age"})
        assert col_expr is not None
        lit_expr = robin_expr_to_column({"lit": 42})
        assert lit_expr is not None

    def test_robin_expr_to_column_op_raises_for_unknown(self) -> None:
        """robin_expr_to_column raises for unsupported op."""
        from sparkless.backend.robin.plan_executor import robin_expr_to_column

        with pytest.raises(ValueError, match="Unsupported Robin plan op"):
            robin_expr_to_column(
                {
                    "op": "unknown_op",
                    "left": {"col": "a"},
                    "right": {"lit": 1},
                }
            )

    def test_execute_robin_plan_orderby_desc_op_uses_descending(self) -> None:
        """orderBy with {'op': 'desc', 'left': {'col': ...}} sorts in descending order."""
        from sparkless.backend.robin.plan_executor import execute_robin_plan

        data = [
            {"id": 1, "salary": 100},
            {"id": 2, "salary": 50},
            {"id": 3, "salary": 75},
        ]
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("salary", LongType()),
            ]
        )
        plan = [
            {
                "op": "orderBy",
                "payload": {
                    # Matches Robin plan shape for F.col("salary").desc()
                    "columns": [
                        {"op": "desc", "left": {"col": "salary"}, "right": None},
                    ],
                    # Original bug case: ascending=True but op=desc should override
                    "ascending": [True],
                },
            }
        ]
        rows = execute_robin_plan(data, schema, plan)
        salaries = [r["salary"] for r in rows]
        assert salaries == [100, 75, 50]

    def test_execute_robin_plan_orderby_asc_op_uses_ascending(self) -> None:
        """orderBy with {'op': 'asc', 'left': {'col': ...}} sorts in ascending order."""
        from sparkless.backend.robin.plan_executor import execute_robin_plan

        data = [
            {"id": 1, "salary": 100},
            {"id": 2, "salary": 50},
            {"id": 3, "salary": 75},
        ]
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("salary", LongType()),
            ]
        )
        plan = [
            {
                "op": "orderBy",
                "payload": {
                    # Matches Robin plan shape for F.col("salary").asc()
                    "columns": [
                        {"op": "asc", "left": {"col": "salary"}, "right": None},
                    ],
                    # ascending=False should be ignored for explicit asc()
                    "ascending": [False],
                },
            }
        ]
        rows = execute_robin_plan(data, schema, plan)
        salaries = [r["salary"] for r in rows]
        assert salaries == [50, 75, 100]

    def test_execute_robin_plan_orderby_mixed_col_and_desc(self) -> None:
        """Mixed plain column and desc op uses payload ascending for plain col and op for desc."""
        from sparkless.backend.robin.plan_executor import execute_robin_plan

        data = [
            {"id": 1, "salary": 50},
            {"id": 2, "salary": 75},
            {"id": 1, "salary": 100},
        ]
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("salary", LongType()),
            ]
        )
        plan = [
            {
                "op": "orderBy",
                "payload": {
                    "columns": [
                        {"col": "id"},
                        {"op": "desc", "left": {"col": "salary"}, "right": None},
                    ],
                    # First column (id) ascending, second column (salary) desc via op
                    "ascending": [True, True],
                },
            }
        ]
        rows = execute_robin_plan(data, schema, plan)
        result = [(r["id"], r["salary"]) for r in rows]
        # Sorted by id asc, then salary desc within each id
        assert result == [(1, 100), (1, 50), (2, 75)]
