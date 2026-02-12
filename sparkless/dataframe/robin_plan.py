"""
Robin-format logical plan builder.

This module converts Sparkless's internal logical plan into the JSON format
expected by the Robin-sparkless plan interpreter. It is intentionally scoped
to a subset of operations and expressions; unsupported cases raise ValueError
so the lazy engine can fall back to the existing operation-by-operation path.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .logical_plan import to_logical_plan as _to_sparkless_plan

if TYPE_CHECKING:
    from sparkless.dataframe import DataFrame


_COMPARISON_OP_MAP = {
    "==": "eq",
    "!=": "ne",
    ">": "gt",
    "<": "lt",
    ">=": "ge",
    "<=": "le",
    "eqNullSafe": "eq_null_safe",
}

_ARITHMETIC_OP_MAP = {
    "+": "add",
    "-": "sub",
    "*": "mul",
    "/": "div",
}

_LOGICAL_OP_MAP = {
    "&": "and",
    "|": "or",
    "!": "not",
}


def _expr_to_robin(expr: Any) -> Dict[str, Any]:
    """Convert a Sparkless logical-plan expression dict to Robin expression JSON.

    Accepts the output of sparkless.dataframe.logical_plan.serialize_expression
    and returns a dict using the Robin-style keys: col, lit, op, fn/args.
    Raises ValueError for unsupported/opaque expressions so the caller can
    trigger a fallback to the non-plan path.
    """
    # Strings are treated as column names for convenience
    if isinstance(expr, str):
        return {"col": expr}

    if not isinstance(expr, dict):
        raise ValueError(f"Unsupported expression type for Robin plan: {type(expr)!r}")

    # If this already looks like a Robin expression, pass it through
    if "col" in expr or "lit" in expr or "fn" in expr:
        return expr

    expr_type = expr.get("type")

    if expr_type == "column":
        name = expr.get("name")
        if not isinstance(name, str):
            raise ValueError("Column expression missing 'name'")
        return {"col": name}

    if expr_type == "literal":
        # Value is assumed to be JSON-serializable already
        return {"lit": expr.get("value")}

    if expr_type == "op":
        op_name = expr.get("op")
        left = expr.get("left")
        right = expr.get("right")

        # alias: left=expression, right=alias name (literal). Plan executor expects
        # {"op": "alias", "left": <robin_expr>, "right": {"lit": alias_name}} so
        # output column name is the alias, not a column lookup.
        if op_name == "alias":
            robin_left = _expr_to_robin(left) if left is not None else None
            if robin_left is None:
                raise ValueError(
                    "Alias expression requires a valid left (inner expression)"
                )
            alias_name = None
            if right is not None:
                if isinstance(right, dict) and right.get("type") == "literal":
                    alias_name = right.get("value")
                else:
                    robin_right = _expr_to_robin(right)
                    if isinstance(robin_right, dict) and "lit" in robin_right:
                        alias_name = robin_right["lit"]
            if not isinstance(alias_name, str):
                raise ValueError(
                    "Alias expression requires right to be a string literal (alias name)"
                )
            return {"op": "alias", "left": robin_left, "right": {"lit": alias_name}}

        # Map Sparkless operator symbols to Robin names where needed
        if op_name in _COMPARISON_OP_MAP:
            op_name = _COMPARISON_OP_MAP[op_name]
        elif op_name in _ARITHMETIC_OP_MAP:
            op_name = _ARITHMETIC_OP_MAP[op_name]
        elif op_name in _LOGICAL_OP_MAP:
            op_name = _LOGICAL_OP_MAP[op_name]

        op_left: Optional[Dict[str, Any]] = (
            _expr_to_robin(left) if left is not None else None
        )
        op_right: Optional[Dict[str, Any]] = (
            _expr_to_robin(right) if right is not None else None
        )
        return {"op": op_name, "left": op_left, "right": op_right}

    # Window and other complex expression types are currently unsupported
    raise ValueError(f"Unsupported expression kind for Robin plan: {expr_type!r}")


def _convert_payload(op_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a Sparkless logical-plan payload to Robin-style payload.

    Simple operations (limit, offset, drop, distinct, withColumnRenamed) are
    passed through unchanged. Operations that contain expressions are converted
    using _expr_to_robin. Unsupported operations raise ValueError.
    """
    if op_name in {"limit", "offset", "drop", "distinct", "withColumnRenamed"}:
        # These payloads do not embed expression dicts in the current plan format
        return payload

    if op_name == "filter":
        condition = payload.get("condition")
        if condition is None:
            raise ValueError("Filter payload missing 'condition'")
        return {"condition": _expr_to_robin(condition)}

    if op_name == "select":
        columns = payload.get("columns", [])
        robin_cols: List[Dict[str, Any]] = []
        for col in columns:
            if isinstance(col, str):
                robin_cols.append({"col": col})
            else:
                robin_cols.append(_expr_to_robin(col))
        return {"columns": robin_cols}

    if op_name == "withColumn":
        name = payload.get("name")
        expr = payload.get("expression")
        if not isinstance(name, str):
            raise ValueError("withColumn payload missing 'name'")
        if expr is None:
            raise ValueError("withColumn payload missing 'expression'")
        return {"name": name, "expression": _expr_to_robin(expr)}

    if op_name == "orderBy":
        columns = payload.get("columns", [])
        ascending = payload.get("ascending", [])
        order_cols: List[Dict[str, Any]] = []
        for col in columns:
            order_cols.append(_expr_to_robin(col))
        return {"columns": order_cols, "ascending": ascending}

    # groupBy, join, union, and more complex operations are not yet emitted
    raise ValueError(f"Operation '{op_name}' is not yet supported for Robin plans")


def to_robin_plan(df: DataFrame) -> List[Dict[str, Any]]:
    """Produce a Robin-format logical plan for a DataFrame.

    This wraps the existing Sparkless logical plan and converts expression
    payloads into the Robin expression format. Unsupported operations or
    expressions raise ValueError so callers can fall back to the existing
    operation-by-operation Robin backend.
    """
    base_plan = _to_sparkless_plan(df)
    robin_plan: List[Dict[str, Any]] = []
    for entry in base_plan:
        op_name = entry.get("op")
        payload = entry.get("payload", {})
        if not isinstance(op_name, str) or not isinstance(payload, dict):
            raise ValueError("Invalid logical plan entry for Robin conversion")
        robin_payload = _convert_payload(op_name, payload)
        robin_plan.append({"op": op_name, "payload": robin_payload})
    return robin_plan
