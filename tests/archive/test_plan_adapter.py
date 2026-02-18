"""Unit tests for the Robin plan adapter (Sparkless -> LOGICAL_PLAN_FORMAT)."""
# mypy: ignore-errors

from __future__ import annotations

import pytest

from sparkless.robin.plan_adapter import adapt_plan_for_robin, expr_to_robin_format


# --- Expression conversion ---


@pytest.mark.unit
def test_expr_column_to_robin() -> None:
    assert expr_to_robin_format({"type": "column", "name": "x"}) == {"col": "x"}


@pytest.mark.unit
def test_expr_literal_to_robin() -> None:
    assert expr_to_robin_format({"type": "literal", "value": 42}) == {"lit": 42}
    assert expr_to_robin_format({"type": "literal", "value": None}) == {"lit": None}
    assert expr_to_robin_format({"type": "literal", "value": "a"}) == {"lit": "a"}
    assert expr_to_robin_format({"type": "literal", "value": True}) == {"lit": True}


@pytest.mark.unit
def test_expr_comparison_to_robin() -> None:
    col_node = {"type": "column", "name": "x"}
    lit_node = {"type": "literal", "value": 50}
    expr = {"type": "op", "op": ">", "left": col_node, "right": lit_node}
    assert expr_to_robin_format(expr) == {
        "op": "gt",
        "left": {"col": "x"},
        "right": {"lit": 50},
    }
    assert expr_to_robin_format(
        {"type": "op", "op": "==", "left": col_node, "right": lit_node}
    ) == {
        "op": "eq",
        "left": {"col": "x"},
        "right": {"lit": 50},
    }
    assert expr_to_robin_format(
        {"type": "op", "op": "<=", "left": col_node, "right": lit_node}
    ) == {
        "op": "le",
        "left": {"col": "x"},
        "right": {"lit": 50},
    }


@pytest.mark.unit
def test_expr_arithmetic_to_robin() -> None:
    col_node = {"type": "column", "name": "x"}
    lit_node = {"type": "literal", "value": 2}
    expr = {"type": "op", "op": "*", "left": col_node, "right": lit_node}
    assert expr_to_robin_format(expr) == {
        "fn": "multiply",
        "args": [{"col": "x"}, {"lit": 2}],
    }
    assert expr_to_robin_format(
        {"type": "op", "op": "+", "left": col_node, "right": lit_node}
    ) == {
        "fn": "add",
        "args": [{"col": "x"}, {"lit": 2}],
    }
    assert expr_to_robin_format(
        {"type": "op", "op": "-", "left": col_node, "right": lit_node}
    ) == {
        "fn": "subtract",
        "args": [{"col": "x"}, {"lit": 2}],
    }


@pytest.mark.unit
def test_expr_nested_filter_condition() -> None:
    # (x > 50) and (x < 100)
    left_gt = {
        "type": "op",
        "op": ">",
        "left": {"type": "column", "name": "x"},
        "right": {"type": "literal", "value": 50},
    }
    right_lt = {
        "type": "op",
        "op": "<",
        "left": {"type": "column", "name": "x"},
        "right": {"type": "literal", "value": 100},
    }
    and_expr = {"type": "op", "op": "and", "left": left_gt, "right": right_lt}
    got = expr_to_robin_format(and_expr)
    assert got == {
        "op": "and",
        "left": {"op": "gt", "left": {"col": "x"}, "right": {"lit": 50}},
        "right": {"op": "lt", "left": {"col": "x"}, "right": {"lit": 100}},
    }


@pytest.mark.unit
def test_expr_not_to_robin() -> None:
    # Sparkless puts unary operand in "left"
    inner = {
        "type": "op",
        "op": "==",
        "left": {"type": "column", "name": "a"},
        "right": {"type": "literal", "value": 0},
    }
    expr = {"type": "op", "op": "not", "left": inner, "right": None}
    assert expr_to_robin_format(expr) == {
        "op": "not",
        "arg": {"op": "eq", "left": {"col": "a"}, "right": {"lit": 0}},
    }


@pytest.mark.unit
def test_expr_eq_null_safe_to_robin() -> None:
    expr = {
        "type": "op",
        "op": "eqNullSafe",
        "left": {"type": "column", "name": "a"},
        "right": {"type": "column", "name": "b"},
    }
    assert expr_to_robin_format(expr) == {
        "op": "eq_null_safe",
        "left": {"col": "a"},
        "right": {"col": "b"},
    }


@pytest.mark.unit
def test_expr_between_to_robin() -> None:
    expr = {
        "type": "op",
        "op": "between",
        "left": {"type": "column", "name": "a"},
        "right": {
            "type": "between_bounds",
            "lower": {"type": "literal", "value": 3},
            "upper": {"type": "literal", "value": 7},
        },
    }
    got = expr_to_robin_format(expr)
    assert got == {
        "op": "between",
        "left": {"col": "a"},
        "lower": {"lit": 3},
        "upper": {"lit": 7},
    }


@pytest.mark.unit
def test_expr_pow_to_robin() -> None:
    expr = {
        "type": "op",
        "op": "**",
        "left": {"type": "column", "name": "x"},
        "right": {"type": "literal", "value": 2},
    }
    assert expr_to_robin_format(expr) == {
        "op": "pow",
        "left": {"col": "x"},
        "right": {"lit": 2},
    }


@pytest.mark.unit
def test_expr_cast_to_robin() -> None:
    expr = {
        "type": "op",
        "op": "cast",
        "left": {"type": "column", "name": "x"},
        "right": {"type": "literal", "value": "string"},
    }
    assert expr_to_robin_format(expr) == {
        "fn": "cast",
        "args": [{"col": "x"}, {"lit": "string"}],
    }


@pytest.mark.unit
def test_expr_opaque_passthrough() -> None:
    expr = {"type": "opaque", "repr": "some_expr"}
    assert expr_to_robin_format(expr) == {"type": "opaque", "repr": "some_expr"}


@pytest.mark.unit
def test_expr_non_dict_passthrough() -> None:
    assert expr_to_robin_format(42) == 42
    assert expr_to_robin_format("x") == "x"


# --- Step adaptation ---


@pytest.mark.unit
def test_adapt_plan_filter_unwraps_condition() -> None:
    plan = [
        {
            "op": "filter",
            "payload": {
                "condition": {
                    "type": "op",
                    "op": ">",
                    "left": {"type": "column", "name": "x"},
                    "right": {"type": "literal", "value": 50},
                },
            },
        },
    ]
    got = adapt_plan_for_robin(plan)
    assert len(got) == 1
    assert got[0]["op"] == "filter"
    assert got[0]["payload"] == {"op": "gt", "left": {"col": "x"}, "right": {"lit": 50}}
    assert "condition" not in got[0]["payload"]


@pytest.mark.unit
def test_adapt_plan_with_column_renames_expression_to_expr() -> None:
    plan = [
        {
            "op": "withColumn",
            "payload": {
                "name": "double_x",
                "expression": {
                    "type": "op",
                    "op": "*",
                    "left": {"type": "column", "name": "x"},
                    "right": {"type": "literal", "value": 2},
                },
            },
        },
    ]
    got = adapt_plan_for_robin(plan)
    assert len(got) == 1
    assert got[0]["op"] == "withColumn"
    assert "expression" not in got[0]["payload"]
    assert got[0]["payload"]["name"] == "double_x"
    assert got[0]["payload"]["expr"] == {
        "fn": "multiply",
        "args": [{"col": "x"}, {"lit": 2}],
    }


@pytest.mark.unit
def test_adapt_plan_passthrough_other_steps() -> None:
    plan = [
        {"op": "limit", "payload": {"n": 10}},
        {"op": "select", "payload": {"columns": [{"type": "column", "name": "id"}]}},
    ]
    got = adapt_plan_for_robin(plan)
    # limit is unchanged; select is adapted to Robin shape (name + expr)
    assert got[0] == plan[0]
    assert got[1]["op"] == "select"
    assert got[1]["payload"]["columns"] == [{"name": "id", "expr": {"col": "id"}}]


@pytest.mark.unit
def test_adapt_plan_empty() -> None:
    assert adapt_plan_for_robin([]) == []


@pytest.mark.unit
def test_adapt_plan_mixed_filter_with_column_limit() -> None:
    plan = [
        {
            "op": "filter",
            "payload": {
                "condition": {
                    "type": "op",
                    "op": ">=",
                    "left": {"type": "column", "name": "v"},
                    "right": {"type": "literal", "value": 1},
                }
            },
        },
        {
            "op": "withColumn",
            "payload": {
                "name": "twice",
                "expression": {
                    "type": "op",
                    "op": "*",
                    "left": {"type": "column", "name": "v"},
                    "right": {"type": "literal", "value": 2},
                },
            },
        },
        {"op": "limit", "payload": {"n": 5}},
    ]
    got = adapt_plan_for_robin(plan)
    assert len(got) == 3
    assert got[0]["op"] == "filter" and got[0]["payload"]["op"] == "ge"
    assert (
        got[1]["op"] == "withColumn"
        and "expr" in got[1]["payload"]
        and got[1]["payload"]["expr"]["fn"] == "multiply"
    )
    assert got[2]["op"] == "limit" and got[2]["payload"] == {"n": 5}


@pytest.mark.unit
def test_robin_filter_and_with_column_integration() -> None:
    """Integration: filter and withColumn via Robin (plan adapter). Skip if extension or select unavailable."""
    try:
        from sparkless import SparkSession
        from sparkless import functions as F
    except ImportError:
        pytest.skip("sparkless not available")
    try:
        from sparkless import _robin  # noqa: F401
    except Exception:
        pytest.skip("Robin extension not available")
    spark = SparkSession.builder.appName("PlanAdapterIntegration").getOrCreate()
    try:
        # Probe select-by-name (robin-sparkless 0.11.7+)
        df = spark.createDataFrame([{"x": 1}])
        df.select("x").collect()
    except Exception as e:
        if "select payload" in str(e) or "invalid plan" in str(e).lower():
            pytest.skip(f"Robin crate does not accept select payload: {e}")
        raise
    # Filter: x > 2
    df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}])
    filtered = df.filter(F.col("x") > 2).collect()
    assert len(filtered) == 2
    assert {r["x"] for r in filtered} == {3, 4}
    # WithColumn: double_x = x * 2
    with_col = df.withColumn("double_x", F.col("x") * 2)
    rows = with_col.collect()
    assert len(rows) == 4
    one = next(r for r in rows if r["x"] == 2)
    assert one["double_x"] == 4.0
    spark.stop()
