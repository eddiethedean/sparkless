"""
Robin-format plan executor (Phase 5).

Executes a Robin-format logical plan (list of {"op", "payload"}) over input data
using the Robin DataFrame API. Used by RobinMaterializer.materialize_from_plan
when the lazy engine chooses the plan-based path.
"""

from __future__ import annotations

from typing import Any, Dict, List

from sparkless.spark_types import Row, StructField, StructType, StringType

# Optional import; executor is only used when backend_type="robin" and robin_sparkless is installed
try:
    import robin_sparkless  # noqa: F401
except ImportError:
    robin_sparkless = None  # type: ignore[assignment]


def _robin_available() -> bool:
    return robin_sparkless is not None


def _lit_value_for_robin(val: Any) -> Any:
    """Convert value for F.lit(); Robin supports only None, int, float, bool, str."""
    if val is None or isinstance(val, (int, float, bool, str)):
        return val
    if hasattr(val, "isoformat"):
        return val.isoformat() if callable(getattr(val, "isoformat")) else str(val)
    return str(val)


def robin_expr_to_column(expr: Dict[str, Any]) -> Any:
    """Build a Robin Column from a Robin-format expression dict.

    Handles: {"col": name}, {"lit": value}, {"op": op_name, "left": ..., "right": ...}
    for comparison (eq, ne, gt, lt, ge, le), arithmetic (add, sub, mul, div),
    and logical (and, or, not) ops. Raises ValueError for unsupported shapes.
    """
    if not _robin_available():
        raise RuntimeError(
            "robin_sparkless is not installed. "
            "Install with: pip install sparkless[robin] (or pip install robin-sparkless)."
        )
    F = robin_sparkless  # type: ignore[union-attr]

    if not isinstance(expr, dict):
        raise ValueError(f"Robin expression must be a dict, got {type(expr)!r}")

    if "col" in expr:
        name = expr["col"]
        if not isinstance(name, str):
            raise ValueError(f"Robin col expression must have string name, got {type(name)!r}")
        return F.col(name)

    if "lit" in expr:
        val = expr["lit"]
        safe = _lit_value_for_robin(val)
        return F.lit(safe)  # type: ignore[arg-type]

    if "op" in expr:
        op = expr["op"]
        left_d = expr.get("left")
        right_d = expr.get("right")
        if op == "not":
            inner = robin_expr_to_column(left_d) if left_d is not None else None
            if inner is None:
                raise ValueError("Robin 'not' expression requires 'left'")
            if hasattr(inner, "not_"):
                return inner.not_()
            if hasattr(F, "not_"):
                return F.not_(inner)
            raise ValueError("Robin backend does not support 'not' expression")
        left = robin_expr_to_column(left_d) if left_d is not None else None
        right = robin_expr_to_column(right_d) if right_d is not None else None
        if left is None or right is None:
            raise ValueError(f"Robin op '{op}' requires both left and right")
        # cast: right is a type string literal (Robin plan uses {"lit": type_str})
        if op == "cast":
            type_str = right_d.get("lit") if isinstance(right_d, dict) else None
            if not isinstance(type_str, str):
                raise ValueError("Robin cast requires right to be a string literal (type name)")
            if hasattr(left, "cast"):
                return left.cast(type_str)
            if hasattr(F, "cast") and callable(F.cast):
                return F.cast(left, type_str)
            raise ValueError("Robin backend does not support cast")
        # alias: right is alias name literal (Robin plan uses {"lit": alias_name})
        if op == "alias":
            alias_name = right_d.get("lit") if isinstance(right_d, dict) else None
            if not isinstance(alias_name, str):
                raise ValueError("Robin alias requires right to be a string literal (alias name)")
            if hasattr(left, "alias"):
                return left.alias(alias_name)
            raise ValueError("Robin backend does not support alias")
        # comparison
        if op == "eq":
            return left.eq(right)
        if op == "ne":
            return left.ne(right)
        if op == "gt":
            return left.gt(right)
        if op == "lt":
            return left.lt(right)
        if op == "ge":
            return left.ge(right)
        if op == "le":
            return left.le(right)
        # arithmetic
        if op == "add":
            return left + right
        if op == "sub":
            return left - right
        if op == "mul":
            return left * right
        if op == "div":
            return left / right
        # logical
        if op == "and":
            return left & right
        if op == "or":
            return left | right
        raise ValueError(f"Unsupported Robin plan op: {op!r}")

    raise ValueError(f"Robin expression must contain 'col', 'lit', or 'op'; got {list(expr.keys())!r}")


def execute_robin_plan(
    data: List[Any],
    schema: StructType,
    plan: List[Dict[str, Any]],
) -> List[Row]:
    """Execute a Robin-format logical plan over data and return List[Row].

    Creates a Robin DataFrame from data/schema, applies each operation in plan
    in order, then collects to Sparkless Rows. Raises ValueError for
    unsupported ops so the caller can fall back to operation-based materialization.
    """
    if not _robin_available():
        raise RuntimeError(
            "robin_sparkless is not installed. "
            "Install with: pip install sparkless[robin] (or pip install robin-sparkless)."
        )

    from sparkless.backend.robin.materializer import (
        _data_to_robin_rows,
        _get_robin_session_and_create_from_rows,
        _spark_type_to_robin_dtype,
    )

    if not schema.fields:
        raise ValueError("execute_robin_plan requires a non-empty schema")

    name_to_field = {f.name: f for f in schema.fields}
    if data:
        if isinstance(data[0], dict):
            init_names = list(data[0].keys())
        elif hasattr(data[0], "_fields"):
            init_names = list(data[0]._fields)
        else:
            init_names = [f.name for f in schema.fields]
        init_fields = []
        for n in init_names:
            if n in name_to_field:
                init_fields.append(name_to_field[n])
            else:
                init_fields.append(StructField(n, StringType(), True))
        if not init_fields:
            init_fields = schema.fields
            init_names = [f.name for f in schema.fields]
    else:
        init_names = [f.name for f in schema.fields]
        init_fields = schema.fields

    robin_schema = [
        (f.name, _spark_type_to_robin_dtype(f.dataType)) for f in init_fields
    ]
    robin_data = _data_to_robin_rows(data, init_names)
    spark, create_from_rows = _get_robin_session_and_create_from_rows()
    df = create_from_rows(robin_data, robin_schema)

    for entry in plan:
        if not isinstance(entry, dict):
            raise ValueError(f"Plan entry must be a dict, got {type(entry)!r}")
        op = entry.get("op")
        payload = entry.get("payload", {})
        if not isinstance(op, str) or not isinstance(payload, dict):
            raise ValueError("Plan entry must have 'op' (str) and 'payload' (dict)")

        if op == "filter":
            cond = payload.get("condition")
            if cond is None:
                raise ValueError("Filter payload missing 'condition'")
            expr = robin_expr_to_column(cond)
            df = df.filter(expr)
        elif op == "select":
            columns = payload.get("columns", [])
            cols = [robin_expr_to_column(c) for c in columns]
            df = df.select(*cols)
        elif op == "limit":
            n = payload.get("n")
            if n is None:
                raise ValueError("Limit payload missing 'n'")
            df = df.limit(int(n))
        elif op == "orderBy":
            col_specs = payload.get("columns", [])
            ascending = payload.get("ascending", [])
            col_names: List[str] = []
            asc_list: List[bool] = []
            _DESC_OPS = ("desc", "desc_nulls_last", "desc_nulls_first")
            _ASC_OPS = ("asc", "asc_nulls_last", "asc_nulls_first")
            for i, e in enumerate(col_specs):
                if not isinstance(e, dict):
                    raise ValueError(
                        "orderBy column spec must be a dict (column ref or desc/asc)"
                    )
                # Plain column reference: {"col": name}
                if "col" in e:
                    col_names.append(e["col"])
                    if isinstance(ascending, bool):
                        asc_list.append(ascending)
                    else:
                        asc_list.append(
                            ascending[i] if i < len(ascending) else True
                        )
                    continue
                # desc/asc of column: {"op": "desc"|"asc"|..., "left": {"col": name}}
                if "op" in e:
                    left = e.get("left")
                    if isinstance(left, dict) and "col" in left:
                        col_names.append(left["col"])
                        asc_list.append(e["op"] not in _DESC_OPS)
                        continue
                raise ValueError(
                    "orderBy currently supports only column references or "
                    "desc/asc of a column in plan"
                )
            if len(asc_list) < len(col_names):
                asc_list = asc_list + [True] * (len(col_names) - len(asc_list))
            df = df.order_by(col_names, asc_list[: len(col_names)])
        elif op == "withColumn":
            name = payload.get("name")
            expr_p = payload.get("expression")
            if not isinstance(name, str):
                raise ValueError("withColumn payload missing 'name'")
            if expr_p is None:
                raise ValueError("withColumn payload missing 'expression'")
            expr = robin_expr_to_column(expr_p)
            df = df.with_column(name, expr)
        elif op == "drop":
            cols = payload.get("cols", payload.get("columns", []))
            if isinstance(cols, str):
                cols = [cols]
            df = df.drop(list(cols))
        elif op == "distinct":
            df = df.distinct()
        elif op == "withColumnRenamed":
            existing = payload.get("existing")
            new = payload.get("new")
            if not existing or not new:
                raise ValueError("withColumnRenamed payload requires 'existing' and 'new'")
            df = df.with_column_renamed(existing, new)
        elif op == "offset":
            n = payload.get("n", 0)
            df = df.offset(int(n))
        else:
            raise ValueError(f"Unsupported plan op: {op!r}")

    collected = df.collect()
    merged: List[dict] = []
    for d in collected:
        if isinstance(d, dict):
            row = dict(d)
            for k in list(row.keys()):
                if k.endswith("_right"):
                    base = k[:-6]
                    row[base] = row[k]
                    del row[k]
            merged.append(row)
        else:
            merged.append(d)
    return [Row(d, schema=schema) for d in merged]
