"""
Plan adapter: convert Sparkless logical plan payloads and expression trees
into the format required by the robin-sparkless crate (LOGICAL_PLAN_FORMAT).

Used only in the Robin execution path before calling the crate.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

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
# Sparkless uses &/| for logical and/or; Robin expects "and"/"or"
_OP_LOGICAL_ALIAS = {"&": "and", "|": "or"}
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


def _robin_arg_from_value(item: Any) -> Any:
    """Convert a single item (scalar or expr dict) to Robin expression form. Never use lit with list/dict."""
    if isinstance(item, dict) and ("type" in item or "col" in item or "fn" in item or "op" in item):
        return expr_to_robin_format(item)
    if isinstance(item, (str, int, float, bool, type(None))):
        return {"lit": item}
    return {"lit": item}


def _expand_right_to_args(right: Any, left: Any = None) -> List[Any]:
    """Build args list from right (list or literal-with-list) and optional left. Each arg in Robin format."""
    args: List[Any] = []
    if left is not None:
        args.append(expr_to_robin_format(left))
    if isinstance(right, list):
        args.extend(_robin_arg_from_value(a) for a in right if a is not None)
        return args
    if isinstance(right, dict) and right.get("type") == "literal":
        v = right.get("value")
        if isinstance(v, list):
            args.extend(_robin_arg_from_value(a) for a in v if a is not None)
            return args
        if v is not None:
            args.append({"lit": v})
        return args
    if right is not None:
        args.append(expr_to_robin_format(right))
    return args


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

    # Literal: {"type": "literal", "value": v} -> {"lit": v}. Robin accepts only scalar (str, int, float, bool, None).
    if expr.get("type") == "literal" and "value" in expr:
        v = expr["value"]
        if isinstance(v, (list, dict)):
            return dict(expr)
        return {"lit": v}

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

        # Logical: and, or, not; also & -> and, | -> or
        if op in _OP_LOGICAL_ALIAS:
            op = _OP_LOGICAL_ALIAS[op]
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

        # isnull / isnotnull: unary -> Robin op "is_null" / "is_not_null" with "arg" (crate does not support fn form)
        if op in ("isnull", "isnotnull"):
            robin_op = "is_null" if op == "isnull" else "is_not_null"
            return {
                "op": robin_op,
                "arg": expr_to_robin_format(left) if left is not None else None,
            }

        # isin: Robin expects at least 2 args. Empty list -> [left, lit(None)] (PySpark: col.isin([]) is false).
        if op == "isin":
            left_expr = expr_to_robin_format(left) if left is not None else None
            if isinstance(right, list):
                args = [left_expr] if left_expr is not None else []
                if len(right) == 0:
                    args.append({"lit": None})
                else:
                    args.extend({"lit": v} for v in right)
                return {"fn": "isin", "args": args}
            right_expr = expr_to_robin_format(right) if right is not None else None
            return {"op": "isin", "left": left_expr, "right": right_expr}

        # create_map: Robin expects fn "create_map" with "args" array (key-value pairs).
        # Sparkless serializes value as literal with value=list or as list of expr dicts.
        if op == "create_map":
            args = _expand_right_to_args(right, left)
            return {"fn": "create_map", "args": args}

        # array: Robin expects fn "array" with "args" array.
        # Sparkless: left=first column, right=literal with value=[rest columns] or list.
        if op == "array":
            args = [expr_to_robin_format(left)] if left is not None else []
            if isinstance(right, list):
                args.extend(_robin_arg_from_value(a) for a in right if a is not None)
            elif isinstance(right, dict) and right.get("type") == "literal":
                v = right.get("value")
                if isinstance(v, list):
                    args.extend(_robin_arg_from_value(a) for a in v if a is not None)
                elif v is not None:
                    args.append(_robin_arg_from_value(v))
            elif right is not None:
                args.append(expr_to_robin_format(right))
            return {"fn": "array", "args": args}

        # Ops that Robin expects as fn with args (string, getItem, regexp_extract)
        _UNARY_TO_FN = ("upper", "lower")
        if op in _UNARY_TO_FN:
            args = [expr_to_robin_format(left)] if left is not None else []
            return {"fn": op, "args": args}

        _BINARY_TO_FN = ("split", "startswith", "contains")
        if op in _BINARY_TO_FN:
            args = []
            if left is not None:
                args.append(expr_to_robin_format(left))
            if right is not None:
                args.append(expr_to_robin_format(right))
            return {"fn": op, "args": args}

        # concat: Robin expects fn "concat" with args (one or more column/lit).
        # Sparkless sends op with left/right (binary); for multiple cols it may nest.
        if op == "concat":
            args = _expand_right_to_args(right, left)
            return {"fn": "concat", "args": args}

        # getItem: (column, key) -> fn "get_item" (Robin uses snake_case)
        if op == "getItem":
            args = [expr_to_robin_format(left)] if left is not None else []
            if right is not None:
                args.append(expr_to_robin_format(right))
            return {"fn": "get_item", "args": args}

        # regexp_extract: Robin requires exactly 3 args [column, pattern, idx] with
        # pattern and idx as literals (issue #523). Sparkless serializes (pattern, idx)
        # as {"type":"literal","value":[pattern, idx]}.
        if op == "regexp_extract":
            args = [expr_to_robin_format(left)] if left is not None else []
            if right is not None:
                # Unpack (pattern, idx) from literal value list
                if isinstance(right, dict) and right.get("type") == "literal":
                    v = right.get("value")
                    if isinstance(v, (list, tuple)) and len(v) >= 2:
                        args.append({"lit": v[0]})
                        args.append({"lit": v[1]})
                    else:
                        args.append(expr_to_robin_format(right))
                else:
                    r = expr_to_robin_format(right)
                    if isinstance(r, dict) and "lit" in r and isinstance(r["lit"], list) and len(r["lit"]) >= 2:
                        pattern, idx = r["lit"][0], r["lit"][1]
                        args.append({"lit": pattern})
                        args.append({"lit": idx})
                    else:
                        args.append(r)
            if len(args) == 2:
                args.append({"lit": 0})
            elif len(args) == 1:
                args.append({"lit": ""})
                args.append({"lit": 0})
            return {"fn": "regexp_extract", "args": args}

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

    # Window: Robin expects fn-based shape, not type "window". Emit fn + args + window spec.
    if expr.get("type") == "window":
        fn = expr.get("function", "window")
        partition_by = list(expr.get("partition_by") or [])
        order_by = list(expr.get("order_by") or [])
        col_name = expr.get("column")
        args: List[Any] = []
        if col_name and isinstance(col_name, str) and not col_name.startswith("__"):
            args.append({"col": col_name})
        return {
            "fn": fn,
            "args": args,
            "window": {"partition_by": partition_by, "order_by": order_by},
        }

    # CaseWhen: recursive conversion of branches and otherwise (crate may support case_when)
    if expr.get("type") == "case_when":
        branches = [
            {
                "condition": expr_to_robin_format(b.get("condition")),
                "value": expr_to_robin_format(b.get("value")),
            }
            for b in expr.get("branches", [])
        ]
        otherwise = expr.get("otherwise")
        return {
            "type": "case_when",
            "branches": branches,
            "otherwise": expr_to_robin_format(otherwise) if otherwise is not None else None,
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

        if op_name == "select" and isinstance(payload, dict):
            # Robin expects each column item to have "name" (and optionally "expr")
            columns_in = payload.get("columns", [])
            robin_columns: List[Dict[str, Any]] = []
            for col_item in columns_in:
                if not isinstance(col_item, dict):
                    robin_columns.append({"name": str(col_item)})
                    continue
                if col_item.get("type") == "column" and "name" in col_item:
                    # Simple column ref -> {name: n}; Robin may also want expr for column ref
                    n = col_item["name"]
                    robin_columns.append({"name": n, "expr": {"col": n}})
                else:
                    # Expression (alias, window, etc.) -> {name: alias, expr: robin_expr}
                    alias = col_item.get("name") or col_item.get("alias") or col_item.get("_name")
                    robin_expr = expr_to_robin_format(col_item)
                    if alias:
                        robin_columns.append({"name": str(alias), "expr": robin_expr})
                    else:
                        robin_columns.append({"name": "col", "expr": robin_expr})
            result.append({"op": "select", "payload": {"columns": robin_columns}})
        elif op_name == "drop" and isinstance(payload, dict):
            # Robin expects "columns"; Sparkless sends "cols"
            cols = payload.get("cols", payload.get("columns", []))
            result.append({"op": "drop", "payload": {"columns": list(cols)}})
        elif op_name == "filter" and isinstance(payload, dict):
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


def _resolve_expr(expr: Any, available: List[str], case_sensitive: bool) -> Any:
    """Recursively resolve column names in an expression. Returns new dict/list/scalar."""
    if not isinstance(expr, dict):
        return expr
    if expr.get("type") == "column" and "name" in expr:
        from sparkless.core.column_resolver import ColumnResolver
        resolved = ColumnResolver.resolve_column_name(
            expr["name"], available, case_sensitive
        )
        if resolved is not None:
            return {"type": "column", "name": resolved}
        return dict(expr)
    if expr.get("type") == "literal":
        return dict(expr)
    out: Dict[str, Any] = {}
    for k, v in expr.items():
        if isinstance(v, list):
            out[k] = [_resolve_expr(e, available, case_sensitive) for e in v]
        elif isinstance(v, dict):
            out[k] = _resolve_expr(v, available, case_sensitive)
        else:
            out[k] = v
    return out


def _schema_after_step(step: Dict[str, Any], available: List[str]) -> List[str]:
    """Return the list of column names available after applying this step."""
    op_name = step.get("op")
    payload = step.get("payload") if isinstance(step.get("payload"), dict) else {}
    if op_name == "select":
        cols = payload.get("columns", [])
        # Prefer alias so expressions with alias contribute alias to available
        return [
            c.get("alias") or c.get("name") or c.get("_name") or str(c)
            for c in cols
            if isinstance(c, dict) and (c.get("alias") or c.get("name") or c.get("_name"))
        ] or available
    if op_name == "withColumn":
        name = payload.get("name")
        if name:
            return list(available) + [name]
    if op_name == "drop":
        to_drop = set(payload.get("cols", payload.get("columns", [])))
        return [c for c in available if c not in to_drop]
    if op_name == "join":
        how = (payload.get("how") or "inner").lower()
        if how in ("left_semi", "left_anti", "semi", "anti", "leftsemi", "leftanti"):
            return list(available)
        other_schema = payload.get("other_schema") or []
        right_names = [
            f["name"] for f in other_schema
            if isinstance(f, dict) and "name" in f
        ]
        out = list(available)
        left_set = set(available)
        for n in right_names:
            if n not in left_set:
                out.append(n)
                left_set.add(n)
        return out
    if op_name == "union":
        return list(available)
    return list(available)


def resolve_plan_columns(
    plan: List[Dict[str, Any]],
    initial_schema_names: List[str],
    case_sensitive: bool = False,
) -> List[Dict[str, Any]]:
    """
    Resolve column names in the plan to match the schema at each step.
    Rewrites {"type": "column", "name": n} using ColumnResolver so Robin
    receives names that exist in the current step's schema.
    """
    if not initial_schema_names and not plan:
        return list(plan)
    available = list(initial_schema_names)
    result: List[Dict[str, Any]] = []
    for step in plan:
        if not isinstance(step, dict):
            result.append(step)
            available = available
            continue
        payload = step.get("payload")
        resolved_step = dict(step)
        if isinstance(payload, dict):
            resolved_payload = dict(payload)
            if step.get("op") == "select":
                resolved_payload["columns"] = [
                    _resolve_expr(c, available, case_sensitive)
                    for c in payload.get("columns", [])
                ]
            elif step.get("op") == "filter":
                cond = payload.get("condition", payload)
                resolved_payload["condition"] = _resolve_expr(cond, available, case_sensitive)
            elif step.get("op") == "withColumn":
                expr_val = payload.get("expression") or payload.get("expr")
                if expr_val is not None:
                    resolved_payload["expression"] = _resolve_expr(
                        expr_val, available, case_sensitive
                    )
            elif step.get("op") == "join":
                from sparkless.core.column_resolver import ColumnResolver
                on_list = payload.get("on", [])
                resolved_on = []
                for name in on_list:
                    resolved = ColumnResolver.resolve_column_name(
                        name if isinstance(name, str) else str(name),
                        available,
                        case_sensitive,
                    )
                    resolved_on.append(resolved if resolved is not None else name)
                resolved_payload["on"] = resolved_on
                condition = payload.get("condition")
                if condition is not None:
                    resolved_payload["condition"] = _resolve_expr(
                        condition, available, case_sensitive
                    )
            resolved_step["payload"] = resolved_payload
        result.append(resolved_step)
        available = _schema_after_step(resolved_step, available)
    return result
