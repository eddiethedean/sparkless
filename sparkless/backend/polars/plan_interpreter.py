"""
Polars logical plan interpreter.

Executes a serialized logical plan (list of op dicts) against data using Polars,
without using Column/ColumnOperation trees. Used when materialize_from_plan is called.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import polars as pl

from sparkless.spark_types import Row, StructType, get_row_value


def _literal_value(expr: Any) -> Any:
    """Extract scalar value from a serialized literal expr dict, or return expr."""
    if isinstance(expr, dict) and expr.get("type") == "literal":
        return expr.get("value")
    return expr


def _parse_plan_type_to_polars(type_str: str) -> pl.DataType:
    """Map logical plan type string (e.g. simpleString) to Polars dtype."""
    s = (type_str or "string").lower()
    if s in ("string", "str"):
        return pl.Utf8
    if s in ("long", "bigint", "int64"):
        return pl.Int64
    if s in ("int", "integer", "int32"):
        return pl.Int32
    if s in ("double", "float64"):
        return pl.Float64
    if s in ("float", "float32"):
        return pl.Float32
    if s in ("boolean", "bool"):
        return pl.Boolean
    if s in ("date"):
        return pl.Date
    if s in ("timestamp", "datetime"):
        return pl.Datetime("us")
    return pl.Utf8


def _window_over(
    base: pl.Expr,
    partition_by: List[pl.Expr],
    order_by: List[pl.Expr],
    descending: List[bool],
) -> pl.Expr:
    """Apply .over() with optional partition and order."""
    if partition_by and order_by:
        return base.over(*partition_by, order_by=order_by, descending=descending)
    if partition_by:
        return base.over(*partition_by)
    if order_by:
        return base.over(order_by=order_by, descending=descending)
    return base


def _window_expr_to_polars(
    expr: Dict[str, Any], available_columns: List[str]
) -> pl.Expr:
    """Build a Polars window expression from serialized window dict."""
    func = (expr.get("function") or "row_number").lower()
    col_name = expr.get("column")
    partition_by_names = expr.get("partition_by") or []
    order_by_specs = expr.get("order_by") or []
    alias = expr.get("alias")

    def resolve(name: str) -> str:
        return _resolve_col_name(name, available_columns)

    partition_by = [pl.col(resolve(n)) for n in partition_by_names if n]
    order_by: List[pl.Expr] = []
    descending: List[bool] = []
    for ob in order_by_specs:
        n = ob.get("name") if isinstance(ob, dict) else ob
        if not n:
            continue
        order_by.append(pl.col(resolve(n)))
        descending.append(
            bool(ob.get("descending", False)) if isinstance(ob, dict) else False
        )

    # Build base expression by function
    if func == "row_number":
        base = pl.int_range(pl.len()) + 1
        base = _window_over(base, partition_by, order_by, descending)
    elif func == "rank":
        if order_by:
            base = order_by[0].rank(method="min")
        else:
            base = pl.int_range(pl.len()) + 1
        base = _window_over(base, partition_by, order_by, descending)
    elif func == "dense_rank":
        if order_by:
            base = order_by[0].rank(method="dense")
        else:
            base = pl.int_range(pl.len()) + 1
        base = _window_over(base, partition_by, order_by, descending)
    elif func in ("sum", "avg", "mean", "min", "max", "count"):
        if not col_name:
            if func == "count":
                base = pl.len()
            else:
                raise ValueError(f"Window function {func} requires a column")
        else:
            c = resolve(col_name)
            col_expr = pl.col(c)
            if func == "sum":
                base = col_expr.cum_sum() if order_by else col_expr.sum()
            elif func in ("avg", "mean"):
                if order_by:
                    cumsum_expr = col_expr.cum_sum()
                    row_num_expr = pl.int_range(pl.len()) + 1
                    cumsum_expr = _window_over(
                        cumsum_expr, partition_by, order_by, descending
                    )
                    row_num_expr = _window_over(
                        row_num_expr, partition_by, order_by, descending
                    )
                    base = cumsum_expr / row_num_expr
                else:
                    base = col_expr.mean()
            elif func == "min":
                base = col_expr.cum_min() if order_by else col_expr.min()
            elif func == "max":
                base = col_expr.cum_max() if order_by else col_expr.max()
            else:
                base = col_expr.count()
        if partition_by or order_by:
            base = _window_over(base, partition_by, order_by, descending)
    else:
        raise ValueError(f"Unsupported window function in plan: {func}")

    result = base.alias(alias) if alias else base
    return result


def expr_to_polars(expr: Dict[str, Any], available_columns: List[str]) -> pl.Expr:
    """Convert a serialized expression dict to a Polars expression."""
    if not expr or "type" not in expr:
        raise ValueError(f"Invalid expression: {expr}")
    t = expr["type"]
    if t == "column":
        name = expr.get("name", "")
        if name in available_columns:
            return pl.col(name)
        # Case-insensitive match
        for c in available_columns:
            if c.lower() == name.lower():
                return pl.col(c)
        return pl.col(name)
    if t == "literal":
        return pl.lit(expr.get("value"))
    if t == "op":
        op = expr.get("op", "")
        left = expr.get("left")
        right = expr.get("right")
        if left is None:
            raise ValueError(f"Op {op} missing left")
        left_expr = expr_to_polars(left, available_columns)
        if op in (
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
            if op == "!":
                return ~left_expr
            if op == "isnull":
                return left_expr.is_null()
            if op == "isnotnull":
                return left_expr.is_not_null()
            if op in (
                "asc",
                "desc",
                "desc_nulls_last",
                "desc_nulls_first",
                "asc_nulls_last",
                "asc_nulls_first",
            ):
                descending = op in ("desc", "desc_nulls_last", "desc_nulls_first")
                return left_expr.sort(descending=descending)
            return left_expr
        # Unknown unary op (e.g. udf) must not fall through to returning left_expr
        if right is None:
            raise ValueError(f"Unsupported op in plan: {op}")
        if op == "isin" and isinstance(right, list):
            return left_expr.is_in(right)
        if op == "between":
            # right: {"type": "between_bounds", "lower": x, "upper": y}
            if isinstance(right, dict) and right.get("type") == "between_bounds":
                lo = right.get("lower")
                hi = right.get("upper")
                lo_expr = (
                    expr_to_polars(lo, available_columns)
                    if isinstance(lo, dict)
                    else pl.lit(lo)
                )
                hi_expr = (
                    expr_to_polars(hi, available_columns)
                    if isinstance(hi, dict)
                    else pl.lit(hi)
                )
                return (left_expr >= lo_expr) & (left_expr <= hi_expr)
            raise ValueError(f"between requires between_bounds right: {right}")
        if op == "cast":
            # right: {"type": "literal", "value": type_str}
            type_val = (
                right.get("value", "string") if isinstance(right, dict) else str(right)
            )
            dtype = _parse_plan_type_to_polars(type_val)
            return left_expr.cast(dtype)
        right_expr = (
            expr_to_polars(right, available_columns)
            if isinstance(right, dict)
            else pl.lit(right)
        )
        if op == "==":
            return left_expr == right_expr
        if op == "!=":
            return left_expr != right_expr
        if op == "eqNullSafe":
            return (left_expr == right_expr) | (
                left_expr.is_null() & right_expr.is_null()
            )
        if op == "<":
            return left_expr < right_expr
        if op == ">":
            return left_expr > right_expr
        if op == "<=":
            return left_expr <= right_expr
        if op == ">=":
            return left_expr >= right_expr
        if op == "+":
            return left_expr + right_expr
        if op == "-":
            return left_expr - right_expr
        if op == "*":
            return left_expr * right_expr
        if op == "/":
            return left_expr / right_expr
        if op == "%":
            return left_expr % right_expr
        if op == "**":
            return left_expr.pow(right_expr)
        if op == "&":
            return left_expr & right_expr
        if op == "|":
            return left_expr | right_expr
        if op == "like":
            # SQL LIKE: % = any chars, _ = single char. Convert to regex.
            pat_str = _literal_value(right) if isinstance(right, dict) else right
            if isinstance(pat_str, str):
                import re

                re_pat = re.escape(pat_str).replace(r"\%", ".*").replace(r"\_", ".")
                return left_expr.str.contains(f"^{re_pat}$", literal=False)
            return left_expr.str.contains(str(pat_str), literal=False)
        if op == "rlike":
            pat_str = _literal_value(right) if isinstance(right, dict) else right
            return left_expr.str.contains(str(pat_str), literal=False)
        raise ValueError(f"Unsupported op in plan: {op}")
    if t == "window":
        if expr.get("opaque"):
            raise ValueError(
                "Window expressions in logical plan are not yet supported by Polars interpreter"
            )
        return _window_expr_to_polars(expr, available_columns)
    if t == "opaque":
        raise ValueError(f"Opaque expression in plan: {expr.get('repr', expr)}")
    raise ValueError(f"Unknown expression type: {t}")


def _data_to_polars(data: List[Dict[str, Any]], schema: Any) -> pl.DataFrame:
    """Build a Polars DataFrame from data and optional schema."""
    from .type_mapper import mock_type_to_polars_dtype

    if not data:
        if schema and hasattr(schema, "fields") and schema.fields:
            return pl.DataFrame(
                {
                    f.name: pl.Series(
                        f.name, [], dtype=mock_type_to_polars_dtype(f.dataType)
                    )
                    for f in schema.fields
                }
            )
        return pl.DataFrame()
    if schema and hasattr(schema, "fields") and schema.fields:
        series = {}
        for f in schema.fields:
            vals = [get_row_value(row, f.name) for row in data]
            series[f.name] = pl.Series(
                f.name, vals, dtype=mock_type_to_polars_dtype(f.dataType)
            )
        return pl.DataFrame(series)
    return pl.DataFrame(data)


def _polars_to_rows(df: pl.DataFrame, schema: Optional[StructType]) -> List[Row]:
    """Convert Polars DataFrame to List[Row] matching existing materializer behavior."""
    if df.height == 0:
        return []
    rows = []
    for row in df.iter_rows(named=True):
        rows.append(Row(dict(row), schema=None))
    return rows


def _resolve_col_name(name: str, available: List[str]) -> str:
    """Resolve column name (case-insensitive) to actual schema name."""
    if name in available:
        return name
    for c in available:
        if c.lower() == name.lower():
            return c
    return name


def _agg_to_polars(agg_dict: Dict[str, Any], available: List[str]) -> pl.Expr:
    """Convert a serialized agg dict to a Polars aggregation expression."""
    if agg_dict.get("type") == "agg_str":
        # Parse "sum(age)" or "count(1)" style
        import re

        expr = agg_dict.get("expr", "")
        match = re.match(r"(\w+)\s*\(\s*([^)]+)\s*\)", expr)
        if match:
            func, col_arg = match.groups()
            func = func.lower().strip()
            col_arg = col_arg.strip()
            if col_arg == "1" or col_arg == "*":
                if func == "count":
                    e = pl.len()
                else:
                    raise ValueError(f"Unsupported agg_str: {expr}")
            else:
                c = _resolve_col_name(col_arg, available)
                e = _polars_agg_func(pl.col(c), func)
            alias = expr
            return e.alias(alias) if alias else e
        raise ValueError(f"Cannot parse agg_str: {expr}")
    if agg_dict.get("type") == "opaque":
        raise ValueError(
            f"Opaque aggregation in plan: {agg_dict.get('repr', agg_dict)}"
        )
    func = agg_dict.get("func", "sum")
    column = agg_dict.get("column") or "*"
    alias = agg_dict.get("alias")
    if column == "*" or column is None:
        if func == "count":
            e = pl.len()
        else:
            raise ValueError(f"Global agg {func}(*) not supported in plan interpreter")
    else:
        c = _resolve_col_name(str(column), available)
        e = _polars_agg_func(pl.col(c), func)
    return e.alias(alias) if alias else e


def _polars_agg_func(col_expr: pl.Expr, func: str) -> pl.Expr:
    """Map function name to Polars aggregation method."""
    f = func.lower()
    if f == "sum":
        return col_expr.sum()
    if f in ("avg", "mean"):
        return col_expr.mean()
    if f == "max":
        return col_expr.max()
    if f == "min":
        return col_expr.min()
    if f == "count":
        return col_expr.count()
    if f in ("stddev", "stddev_samp", "std"):
        return col_expr.std()
    if f in ("variance", "var_samp", "var"):
        return col_expr.var()
    if f == "first":
        return col_expr.first()
    if f == "last":
        return col_expr.last()
    raise ValueError(f"Unsupported aggregation in plan: {func}")


def execute_plan(
    data: List[Dict[str, Any]],
    schema: Optional[StructType],
    logical_plan: List[Dict[str, Any]],
) -> List[Row]:
    """Execute a logical plan and return List[Row]. Raises on unsupported ops."""
    if not logical_plan:
        return _polars_to_rows(_data_to_polars(data, schema), schema)
    df = _data_to_polars(data, schema)
    available = list(df.columns)
    for entry in logical_plan:
        op = entry.get("op", "")
        payload = entry.get("payload", {})
        if op == "filter":
            cond = payload.get("condition")
            if cond and cond.get("type") != "opaque":
                pred = expr_to_polars(cond, available)
                df = df.filter(pred)
        elif op == "select":
            cols = payload.get("columns", [])
            if cols:
                select_exprs = []
                for c in cols:
                    if isinstance(c, dict) and c.get("type") == "column":
                        select_exprs.append(pl.col(c.get("name", "")))
                    elif isinstance(c, str):
                        select_exprs.append(pl.col(c))
                    else:
                        select_exprs.append(expr_to_polars(c, available))
                df = df.select(select_exprs)
            available = list(df.columns)
        elif op == "withColumn":
            name = payload.get("name", "")
            expr_dict = payload.get("expression", {})
            if expr_dict.get("type") == "opaque":
                raise ValueError("withColumn with opaque expression not supported")
            df = df.with_columns(expr_to_polars(expr_dict, available).alias(name))
            available = list(df.columns)
        elif op == "limit":
            n = payload.get("n", 0)
            df = df.head(n)
        elif op == "offset":
            n = payload.get("n", 0)
            df = df.slice(n)
        elif op == "drop":
            cols = payload.get("cols", [])
            df = df.drop([c for c in cols if c in df.columns])
            available = list(df.columns)
        elif op == "distinct":
            df = df.unique()
        elif op == "withColumnRenamed":
            existing = payload.get("existing", "")
            new = payload.get("new", "")
            df = df.rename({existing: new})
            available = list(df.columns)
        elif op == "orderBy":
            columns = payload.get("columns", [])
            ascending = payload.get("ascending", [True])
            if columns:
                sort_by = [expr_to_polars(c, available) for c in columns]
                if len(ascending) < len(sort_by):
                    ascending = ascending + [True] * (len(sort_by) - len(ascending))
                df = df.sort(
                    sort_by, descending=[not a for a in ascending[: len(sort_by)]]
                )
        elif op == "join":
            other_plan = payload.get("other_plan", [])
            other_data = payload.get("other_data", [])
            on_cols = payload.get("on", [])
            how = payload.get("how", "inner")
            if not on_cols:
                raise ValueError("join requires on columns")
            if other_plan:
                other_rows = execute_plan(other_data, None, other_plan)
                other_data = [
                    r.asDict() if hasattr(r, "asDict") else dict(r) for r in other_rows
                ]
            other_df = pl.DataFrame(other_data) if other_data else pl.DataFrame()
            df = df.join(other_df, on=on_cols, how=how)
            available = list(df.columns)
        elif op == "union":
            other_plan = payload.get("other_plan", [])
            other_data = payload.get("other_data", [])
            if other_plan:
                other_rows = execute_plan(other_data, None, other_plan)
                other_data = [
                    r.asDict() if hasattr(r, "asDict") else dict(r) for r in other_rows
                ]
            other_df = pl.DataFrame(other_data) if other_data else pl.DataFrame()
            df = pl.concat([df, other_df])
            available = list(df.columns)
        elif op == "groupBy":
            columns = payload.get("columns", [])
            aggs = payload.get("aggs", [])
            group_by_cols: List[str] = []
            for c in columns:
                if isinstance(c, dict) and c.get("type") == "column":
                    name = c.get("name", "")
                    resolved = _resolve_col_name(name, available)
                    if resolved in df.columns:
                        group_by_cols.append(resolved)
                elif isinstance(c, str):
                    resolved = _resolve_col_name(c, available)
                    if resolved in df.columns:
                        group_by_cols.append(resolved)
            agg_exprs = [_agg_to_polars(a, available) for a in aggs]
            if not group_by_cols and not agg_exprs:
                # No-op: no grouping and no aggs
                pass
            elif not group_by_cols:
                # Global aggregation
                df = df.select(agg_exprs)
            else:
                if agg_exprs:
                    df = df.group_by(group_by_cols).agg(agg_exprs)
                else:
                    # Only group keys, one row per group
                    df = df.group_by(group_by_cols).agg([])
            available = list(df.columns)
        else:
            raise ValueError(f"Unsupported op in logical plan: {op}")
    return _polars_to_rows(df, schema)
