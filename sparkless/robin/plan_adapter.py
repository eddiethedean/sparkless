"""
Plan adapter: convert Sparkless logical plan payloads and expression trees
into the format required by the robin-sparkless crate (LOGICAL_PLAN_FORMAT).

Used only in the Robin execution path before calling the crate.
"""

from __future__ import annotations

from typing import Any, Dict, List

# Sparkless op symbols/names -> Robin op names (comparison and logical)
_OP_COMPARISON_MAP = {
    ">": "gt",
    "<": "lt",
    "==": "eq",
    "!=": "ne",
    ">=": "ge",
    "<=": "le",
}
_OP_LOGICAL_UNCHANGED = {"and", "or", "not"}
_OP_EQ_NULL_SAFE = "eqNullSafe"  # -> "eq_null_safe"
_OP_BETWEEN = "between"
_OP_POW = "**"  # -> "pow" (Robin accepts "**"|"pow" at op level)
_OP_CAST = "cast"  # -> fn "cast"

# Arithmetic: Sparkless sends as op; Robin expects as fn
_ARITHMETIC_TO_FN = {
    "*": "multiply",
    "+": "add",
    "-": "subtract",
    "/": "divide",
    "%": "mod",
}


def expr_to_robin_format(expr: Any) -> Any:
    """
    Convert a single expression node from Sparkless shape to Robin shape.
    Recursive; returns a new dict (no mutation of input).
    """
    if not isinstance(expr, dict):
        return expr

    # Column: {"type": "column", "name": n} -> {"col": n}
    if expr.get("type") == "column" and "name" in expr:
        return {"col": expr["name"]}

    # Literal: {"type": "literal", "value": v} -> {"lit": v}
    if expr.get("type") == "literal" and "value" in expr:
        return {"lit": expr["value"]}

    # Opaque / unsupported: pass through so Robin can error with clear message
    if expr.get("type") == "opaque":
        return dict(expr)

    # Op: {"type": "op", "op": op, "left": L, "right": R} or "arg" for unary
    if expr.get("type") == "op" and "op" in expr:
        op = expr["op"]
        left = expr.get("left")
        right = expr.get("right")
        arg = expr.get("arg")

        # Comparison
        if op in _OP_COMPARISON_MAP:
            return {
                "op": _OP_COMPARISON_MAP[op],
                "left": expr_to_robin_format(left) if left is not None else None,
                "right": expr_to_robin_format(right) if right is not None else None,
            }

        # Logical (and, or, not)
        if op in _OP_LOGICAL_UNCHANGED:
            out: Dict[str, Any] = {"op": op}
            if op == "not":
                # Robin expects "arg"; Sparkless may put operand in "left"
                operand = arg if arg is not None else left
                if operand is not None:
                    out["arg"] = expr_to_robin_format(operand)
                return out
            if left is not None:
                out["left"] = expr_to_robin_format(left)
            if right is not None:
                out["right"] = expr_to_robin_format(right)
            return out

        if op == _OP_EQ_NULL_SAFE:
            return {
                "op": "eq_null_safe",
                "left": expr_to_robin_format(left) if left is not None else None,
                "right": expr_to_robin_format(right) if right is not None else None,
            }

        # Between: Sparkless right = {"type": "between_bounds", "lower": l, "upper": u}
        if op == _OP_BETWEEN:
            lower_val = right.get("lower") if isinstance(right, dict) else None
            upper_val = right.get("upper") if isinstance(right, dict) else None

            def _as_expr(v: Any) -> Any:
                if v is None:
                    return None
                out = expr_to_robin_format(v)
                # Robin expects expression objects; wrap plain scalars as lit
                if not isinstance(out, dict):
                    return {"lit": v}
                return out

            return {
                "op": "between",
                "left": expr_to_robin_format(left) if left is not None else None,
                "lower": _as_expr(lower_val),
                "upper": _as_expr(upper_val),
            }

        # Pow: ** -> op "pow"
        if op == _OP_POW:
            return {
                "op": "pow",
                "left": expr_to_robin_format(left) if left is not None else None,
                "right": expr_to_robin_format(right) if right is not None else None,
            }

        # Arithmetic -> fn
        if op in _ARITHMETIC_TO_FN:
            return {
                "fn": _ARITHMETIC_TO_FN[op],
                "args": [
                    expr_to_robin_format(left) if left is not None else None,
                    expr_to_robin_format(right) if right is not None else None,
                ],
            }

        # Cast -> fn "cast"
        if op == _OP_CAST:
            type_expr = expr_to_robin_format(right) if right is not None else {"lit": "string"}
            return {
                "fn": "cast",
                "args": [
                    expr_to_robin_format(left) if left is not None else None,
                    type_expr,
                ],
            }

        # Other ops: pass through with converted left/right/arg so structure is valid
        out = {"op": op}
        if left is not None:
            out["left"] = expr_to_robin_format(left)
        if right is not None:
            out["right"] = expr_to_robin_format(right)
        if arg is not None:
            out["arg"] = expr_to_robin_format(arg)
        return out

    # Function call: Sparkless might use {"type": "fn", "fn": name, "args": [...]}
    # Robin expects {"fn": name, "args": [...]}
    if expr.get("type") == "fn" and "fn" in expr:
        fn_name = expr["fn"]
        args = expr.get("args") or []
        return {
            "fn": fn_name,
            "args": [expr_to_robin_format(a) for a in args],
        }

    # Unknown shape: pass through
    return dict(expr)


def adapt_plan_for_robin(plan: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Adapt a Sparkless logical plan so it conforms to Robin's LOGICAL_PLAN_FORMAT.
    Only filter and withColumn steps are rewritten; others are copied unchanged.
    Returns a new list (does not mutate input).
    """
    result: List[Dict[str, Any]] = []
    for step in plan:
        if not isinstance(step, dict):
            result.append(step)
            continue
        op_name = step.get("op")
        payload = step.get("payload")

        if op_name == "filter" and isinstance(payload, dict):
            # Robin expects payload = expression; Sparkless sends {"condition": <expr>}
            condition = payload.get("condition", payload)
            result.append({
                "op": "filter",
                "payload": expr_to_robin_format(condition),
            })
        elif op_name == "withColumn" and isinstance(payload, dict):
            # Robin expects "expr"; Sparkless sends "expression"
            name = payload.get("name")
            expr_val = payload.get("expression") or payload.get("expr")
            if name is not None and expr_val is not None:
                result.append({
                    "op": "withColumn",
                    "payload": {
                        "name": name,
                        "expr": expr_to_robin_format(expr_val),
                    },
                })
            else:
                result.append(dict(step))
        else:
            result.append(dict(step))
    return result
