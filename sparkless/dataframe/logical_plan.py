"""
Logical plan serialization for backend-agnostic execution.

Produces a JSON-serializable list of operations from a DataFrame's
_operations_queue so that backends (e.g. Robin) can execute without
depending on Column/ColumnOperation/Literal trees.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from sparkless.dataframe import DataFrame


def _json_safe_value(value: Any) -> Any:
    """Convert a value to a JSON-serializable form."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return list(value)  # JSON has no bytes; list of ints is reversible
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    return value


def serialize_expression(expr: Any) -> Dict[str, Any]:
    """Serialize a Column, ColumnOperation, or Literal to a recursive dict.

    Returns a JSON-serializable dict with keys: type, and type-specific fields.
    Unsupported expressions raise ValueError (callers can fall back to raw path).
    """
    from sparkless.functions import Column, ColumnOperation
    from sparkless.functions.core.literals import Literal

    if isinstance(expr, str):
        return {"type": "column", "name": expr}

    # Plain Python scalars (used as literals in e.g. F.col("a") * 2)
    if isinstance(expr, (int, float, bool, type(None))):
        return {"type": "literal", "value": expr}

    if isinstance(expr, Literal):
        return {"type": "literal", "value": _json_safe_value(expr.value)}

    # Window functions: structured serialization for plan interpreter
    try:
        from sparkless.functions.window_execution import WindowFunction

        if isinstance(expr, WindowFunction):
            spec = getattr(expr, "window_spec", None)
            partition_by: List[str] = []
            order_by: List[Dict[str, Any]] = []
            rows_between: Any = None
            range_between: Any = None
            if spec is not None:
                for col in getattr(spec, "_partition_by", []) or []:
                    name = (
                        col if isinstance(col, str) else getattr(col, "name", str(col))
                    )
                    if isinstance(name, str) and " " not in name:
                        partition_by.append(name)
                    else:
                        partition_by.append(str(name).split()[0] if name else "")
                for col in getattr(spec, "_order_by", []) or []:
                    if isinstance(col, str):
                        order_by.append({"name": col, "descending": False})
                    else:
                        op = getattr(col, "operation", None)
                        base = getattr(col, "column", col)
                        name = getattr(base, "name", str(base))
                        if hasattr(name, "strip"):
                            name = name.replace(" DESC", "").replace(" ASC", "").strip()
                        else:
                            name = str(name).split()[0]
                        order_by.append({"name": name, "descending": op == "desc"})
                rows_between = getattr(spec, "_rows_between", None)
                range_between = getattr(spec, "_range_between", None)
            func_name = getattr(expr, "function_name", "row_number")
            col_name = getattr(expr, "column_name", None)
            if col_name and isinstance(col_name, str) and col_name.startswith("__"):
                col_name = None
            alias = getattr(expr, "name", None)
            if alias and alias == f"{func_name}() OVER ({spec})":
                alias = None
            return {
                "type": "window",
                "function": func_name,
                "column": col_name,
                "partition_by": partition_by,
                "order_by": order_by,
                "rows_between": rows_between,
                "range_between": range_between,
                "alias": alias,
            }
    except (ImportError, ValueError, TypeError):
        try:
            from sparkless.functions.window_execution import WindowFunction

            if isinstance(expr, WindowFunction):
                return {"type": "window", "opaque": True, "repr": str(expr)}
        except ImportError:
            pass

    # CaseWhen (when/then/otherwise) for plan path (#472)
    try:
        from sparkless.functions.conditional import CaseWhen

        if isinstance(expr, CaseWhen):
            conditions = getattr(expr, "conditions", []) or []
            default_value = getattr(expr, "default_value", None)
            if not conditions or default_value is None:
                raise ValueError(
                    "CaseWhen must have at least one condition and a default value"
                )
            serialized_conditions: List[List[Dict[str, Any]]] = []
            for cond, then_val in conditions:
                serialized_conditions.append(
                    [serialize_expression(cond), serialize_expression(then_val)]
                )
            return {
                "type": "case_when",
                "conditions": serialized_conditions,
                "default": serialize_expression(default_value),
            }
    except ImportError:
        pass

    if isinstance(expr, ColumnOperation):
        op = getattr(expr, "operation", None)
        col_side = getattr(expr, "column", None)
        val_side = getattr(expr, "value", None)
        if op is None:
            # Treat as column if it has name
            name = getattr(expr, "name", None) or getattr(expr, "_name", None)
            if name is not None:
                return {"type": "column", "name": name}
            raise ValueError(
                f"Cannot serialize ColumnOperation without operation: {expr}"
            )

        left = serialize_expression(col_side) if col_side is not None else None
        # Alias value is a string (name); serialize as literal so plan path gets {"lit": name}
        # Comparison value: plain str from SQL parser (e.g. "dept = 'IT'") is a literal, not column
        literal_val_ops = (
            "alias",
            "==",
            "!=",
            "eqNullSafe",
            "<",
            ">",
            "<=",
            ">=",
            "like",
            "rlike",
        )
        if op in literal_val_ops and isinstance(val_side, str):
            right: Any = {"type": "literal", "value": val_side}
        else:
            right = serialize_expression(val_side) if val_side is not None else None

        # Unary ops: desc, asc, -, !, isnull, isnotnull, etc.
        if right is None and op in (
            "!",
            "isnull",
            "isnotnull",
            "asc",
            "desc",
            "desc_nulls_last",
            "desc_nulls_first",
            "asc_nulls_last",
            "asc_nulls_first",
        ):
            return {"type": "op", "op": op, "left": left, "right": None}

        # Binary ops
        if op in (
            "==",
            "!=",
            "eqNullSafe",
            "<",
            ">",
            "<=",
            ">=",
            "+",
            "-",
            "*",
            "/",
            "%",
            "**",
            "&",
            "|",
            "like",
            "rlike",
        ):
            return {"type": "op", "op": op, "left": left, "right": right}

        if op == "isin":
            # value can be list/tuple of literals or scalars
            if isinstance(val_side, (list, tuple)):
                right_val: Any = [
                    _json_safe_value(getattr(v, "value", v)) for v in val_side
                ]
            else:
                right_val = (
                    serialize_expression(val_side) if val_side is not None else None
                )
            return {"type": "op", "op": "isin", "left": left, "right": right_val}

        if op == "between":
            # value is (lower, upper) - can be Literals or scalars
            if isinstance(val_side, (list, tuple)) and len(val_side) == 2:

                def _ser(e: Any) -> Any:
                    if hasattr(e, "value"):  # Literal
                        return _json_safe_value(e.value)
                    try:
                        return serialize_expression(e)
                    except ValueError:
                        return _json_safe_value(e)

                right = {
                    "type": "between_bounds",
                    "lower": _ser(val_side[0]),
                    "upper": _ser(val_side[1]),
                }
            else:
                right = _json_safe_value(val_side)
            return {"type": "op", "op": "between", "left": left, "right": right}

        if op == "cast":
            # value is typically a DataType with simpleString() or a type name string
            if val_side is None:
                type_str = "string"
            elif hasattr(val_side, "simpleString") and callable(val_side.simpleString):
                type_str = val_side.simpleString()
            else:
                type_str = str(val_side)
            return {
                "type": "op",
                "op": "cast",
                "left": left,
                "right": {"type": "literal", "value": type_str},
            }

        # Generic fallback for other ops (alias, etc.)
        return {"type": "op", "op": op, "left": left, "right": right}

    if isinstance(expr, Column):
        # Column (simple reference) - not ColumnOperation
        name = getattr(expr, "name", None) or getattr(expr, "_name", str(expr))
        return {"type": "column", "name": name}

    raise ValueError(f"Unsupported expression type for serialization: {type(expr)}")


def serialize_schema(schema: Any) -> List[Dict[str, Any]]:
    """Serialize a StructType to a JSON-serializable list of field descriptors."""
    if not hasattr(schema, "fields"):
        return []
    return [{"name": f.name, "type": f.dataType.simpleString()} for f in schema.fields]


def _serialize_data(data: List[Any], schema: Any) -> List[Dict[str, Any]]:
    """Serialize DataFrame data (list of dict/Row) to JSON-serializable list of dicts."""
    if not data:
        return []
    from sparkless.spark_types import get_row_value

    field_names = (
        list(schema.fieldNames()) if schema and hasattr(schema, "fieldNames") else []
    )
    out = []
    for row in data:
        if isinstance(row, dict):
            out.append({k: _json_safe_value(v) for k, v in row.items()})
        elif field_names:
            out.append(
                {n: _json_safe_value(get_row_value(row, n)) for n in field_names}
            )
        else:
            out.append(_json_safe_value(row))
    return out


def _serialize_select_column(col: Any) -> Any:
    """Serialize a single select item: string -> column ref, else serialize_expression."""
    if isinstance(col, str):
        return {"type": "column", "name": col}
    return serialize_expression(col)


def _serialize_agg(agg: Any) -> Dict[str, Any]:
    """Serialize a single aggregation expression for groupBy payload.

    Returns a JSON-serializable dict: {"func": str, "column": str or null, "alias": str or null}.
    For string expressions like "sum(age)", returns {"type": "agg_str", "expr": str}.
    """
    if isinstance(agg, str):
        return {"type": "agg_str", "expr": agg}
    try:
        from sparkless.functions.base import AggregateFunction
        from sparkless.functions import ColumnOperation
    except ImportError:
        return {"type": "opaque", "repr": str(agg)}
    if isinstance(agg, AggregateFunction):
        col_name = agg.column_name
        return {
            "func": agg.function_name,
            "column": col_name,
            "alias": agg.name,
        }
    if isinstance(agg, ColumnOperation):
        inner = getattr(agg, "_aggregate_function", None)
        if inner is not None:
            col_name = inner.column_name
            return {
                "func": inner.function_name,
                "column": col_name,
                "alias": getattr(agg, "name", None) or inner.name,
            }
    return {"type": "opaque", "repr": str(agg)}


def to_logical_plan(df: DataFrame) -> List[Dict[str, Any]]:
    """Produce a serializable logical plan from a DataFrame's operation queue.

    Does not modify the DataFrame or its queue. Payloads are JSON-serializable.
    Ops not yet supported (e.g. join, union, orderBy, groupBy in Phase 1) are
    serialized with a best-effort or placeholder payload; see logical_plan_format.md.
    """
    plan: List[Dict[str, Any]] = []
    for op_name, payload in df._operations_queue:
        entry: Dict[str, Any]
        if op_name == "limit":
            entry = {"op": "limit", "payload": {"n": int(payload)}}
        elif op_name == "offset":
            entry = {"op": "offset", "payload": {"n": int(payload)}}
        elif op_name == "drop":
            cols = list(payload) if isinstance(payload, (list, tuple)) else [payload]
            entry = {"op": "drop", "payload": {"cols": [str(c) for c in cols]}}
        elif op_name == "distinct":
            entry = {"op": "distinct", "payload": {}}
        elif op_name == "withColumnRenamed":
            existing, new = payload
            entry = {
                "op": "withColumnRenamed",
                "payload": {"existing": str(existing), "new": str(new)},
            }
        elif op_name == "filter":
            try:
                cond = serialize_expression(payload)
            except ValueError:
                # Placeholder for unsupported filter expressions (e.g. window)
                cond = {"type": "opaque", "repr": str(payload)}
            entry = {"op": "filter", "payload": {"condition": cond}}
        elif op_name == "select":
            columns = [_serialize_select_column(c) for c in payload]
            entry = {"op": "select", "payload": {"columns": columns}}
        elif op_name == "withColumn":
            col_name, expression = payload
            try:
                expr = serialize_expression(expression)
            except ValueError:
                expr = {"type": "opaque", "repr": str(expression)}
            entry = {
                "op": "withColumn",
                "payload": {"name": str(col_name), "expression": expr},
            }
        elif op_name == "join":
            # Phase 3: full serialization with nested other plan, data, schema
            other, on, how = payload
            on_list: List[Any] = list(on) if isinstance(on, (list, tuple)) else [on]
            on_ser = [
                str(x) if not hasattr(x, "name") else getattr(x, "name", str(x))
                for x in on_list
            ]
            other_plan = (
                to_logical_plan(other) if hasattr(other, "_operations_queue") else []
            )
            other_data_ser = _serialize_data(
                getattr(other, "data", []), getattr(other, "schema", None)
            )
            other_schema_ser = (
                serialize_schema(getattr(other, "schema", None))
                if hasattr(other, "schema")
                else []
            )
            entry = {
                "op": "join",
                "payload": {
                    "on": on_ser,
                    "how": str(how),
                    "other_plan": other_plan,
                    "other_data": other_data_ser,
                    "other_schema": other_schema_ser,
                },
            }
        elif op_name == "union":
            # Phase 3: full serialization with nested other plan, data, schema
            other = payload
            other_plan = (
                to_logical_plan(other) if hasattr(other, "_operations_queue") else []
            )
            other_data_ser = _serialize_data(
                getattr(other, "data", []), getattr(other, "schema", None)
            )
            other_schema_ser = (
                serialize_schema(getattr(other, "schema", None))
                if hasattr(other, "schema")
                else []
            )
            entry = {
                "op": "union",
                "payload": {
                    "other_plan": other_plan,
                    "other_data": other_data_ser,
                    "other_schema": other_schema_ser,
                },
            }
        elif op_name == "orderBy":
            if isinstance(payload, tuple) and len(payload) == 2:
                columns_raw, ascending = payload
                columns_tuple: Tuple[Any, ...] = (
                    tuple(columns_raw)
                    if isinstance(columns_raw, (list, tuple))
                    else (columns_raw,)
                )
            else:
                columns_tuple = (
                    (payload,)
                    if not isinstance(payload, (list, tuple))
                    else tuple(payload)
                )
                ascending = True
            try:
                col_list = [
                    serialize_expression(c)
                    if not isinstance(c, str)
                    else {"type": "column", "name": c}
                    for c in columns_tuple
                ]
            except ValueError:
                col_list = []
            asc_list: List[bool] = (
                [ascending] * len(col_list)
                if isinstance(ascending, bool)
                else list(ascending)
                if isinstance(ascending, (list, tuple))
                else [True]
            )
            if len(asc_list) != len(col_list) and col_list:
                asc_list = asc_list + [True] * (len(col_list) - len(asc_list))
            entry = {
                "op": "orderBy",
                "payload": {"columns": col_list, "ascending": asc_list},
            }
        elif op_name == "groupBy":
            # payload is (group_by_cols, aggs)
            try:
                group_cols = payload[0] if isinstance(payload, (list, tuple)) else []
                col_list = [
                    serialize_expression(c)
                    if not isinstance(c, str)
                    else {"type": "column", "name": c}
                    for c in group_cols
                ]
            except (ValueError, TypeError):
                col_list = []
            agg_list_raw = (
                payload[1]
                if isinstance(payload, (list, tuple)) and len(payload) > 1
                else []
            )
            agg_list = [_serialize_agg(a) for a in agg_list_raw]
            entry = {
                "op": "groupBy",
                "payload": {"columns": col_list, "aggs": agg_list},
            }
        else:
            # Unknown op: emit opaque so plan length matches queue
            entry = {"op": op_name, "payload": {"opaque": True, "repr": str(payload)}}
        plan.append(entry)
    return plan
