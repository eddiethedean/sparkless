"""
Robin (robin-sparkless) materializer.

Implements DataMaterializer using robin_sparkless: _create_dataframe_from_rows (0.4.0+)
for arbitrary schema, then filter, select, limit (and optionally orderBy, withColumn,
join, union). Row values are normalized for Robin (e.g. datetime/date to ISO string)
so that create_dataframe_from_rows accepts them (robin-sparkless #239). Unsupported
operations cause can_handle_operations -> False so the caller may raise
SparkUnsupportedOperationError or use another backend.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from sparkless.spark_types import Row, StringType, StructField, StructType

# Sort ops that imply descending; others imply ascending.
# Robin lacks nulls_first/nulls_last (#245); we map to asc/desc only.
_DESC_OPS = frozenset({"desc", "desc_nulls_last", "desc_nulls_first"})

# Map Sparkless DataType.typeName() to robin_sparkless create_dataframe_from_rows dtype strings.
# Robin supports: bigint, int, long, double, float, string, str, varchar, boolean, bool, date, timestamp, datetime.
_SPARK_TYPE_TO_ROBIN_DTYPE: dict[str, str] = {
    "string": "string",
    "int": "int",
    "long": "bigint",
    "bigint": "bigint",
    "double": "double",
    "float": "float",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_ntz": "timestamp",
    "datetime": "timestamp",
    "str": "string",
    "varchar": "string",
}


def _spark_type_to_robin_dtype(data_type: Any) -> str:
    """Map Sparkless DataType to robin_sparkless schema dtype string."""
    name = getattr(data_type, "typeName", None)
    if callable(name):
        name = name()
    else:
        name = getattr(data_type, "__class__", None)
        name = getattr(name, "__name__", "string") if name else "string"
    return _SPARK_TYPE_TO_ROBIN_DTYPE.get(name, "string")


def _row_value_for_robin(val: Any) -> Any:
    """Convert a single row cell value for Robin create_dataframe_from_rows.

    Robin 0.6.0 accepts only None, int, float, bool, str, dict (struct/map),
    or list (array). Python datetime/date are not accepted; convert to ISO string
    so we don't raise when materializing (robin-sparkless #239).
    """
    if val is None or isinstance(val, (int, float, bool, str)):
        return val
    if isinstance(val, dict):
        return {k: _row_value_for_robin(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_row_value_for_robin(v) for v in val]
    if hasattr(val, "isoformat") and callable(getattr(val, "isoformat")):
        return val.isoformat()
    return str(val)


def _data_to_robin_rows(data: List[Any], names: List[str]) -> List[dict]:
    """Convert Sparkless row data to list of dicts for create_dataframe_from_rows."""
    rows: List[dict] = []
    for row in data:
        if isinstance(row, dict):
            rows.append({n: _row_value_for_robin(row.get(n)) for n in names})
        elif isinstance(row, (list, tuple)):
            values = list(row) + [None] * (len(names) - len(row))
            rows.append(dict(zip(names, (_row_value_for_robin(v) for v in values))))
        else:
            rows.append({n: _row_value_for_robin(getattr(row, n, None)) for n in names})
    return rows


def _order_by_payload_to_robin(payload: Any) -> Optional[Tuple[List[str], List[bool]]]:
    """Convert orderBy payload to (col_names, asc_list) for Robin order_by.

    Handles string column names, Column objects, and ColumnOperation
    (desc, asc, desc_nulls_last, etc.). Ignores nulls_first/nulls_last
    until Robin supports it (#245).
    """
    if not isinstance(payload, (list, tuple)) or len(payload) < 1:
        return None
    columns_raw = payload[0]
    ascending_param = payload[1] if len(payload) > 1 else True
    if isinstance(columns_raw, (list, tuple)):
        cols_list = list(columns_raw)
    elif isinstance(columns_raw, str):
        cols_list = [columns_raw]
    else:
        cols_list = [columns_raw]
    col_names: List[str] = []
    asc_list: List[bool] = []
    for i, c in enumerate(cols_list):
        col_name: Optional[str] = None
        asc: Optional[bool] = None
        if isinstance(c, str):
            col_name = c
            if isinstance(ascending_param, bool):
                asc = ascending_param
            elif isinstance(ascending_param, (list, tuple)) and i < len(
                ascending_param
            ):
                asc = bool(ascending_param[i])
            else:
                asc = True
        elif hasattr(c, "operation") and hasattr(c, "column"):
            # ColumnOperation (e.g. col("x").desc())
            op = getattr(c, "operation", None)
            col_side = getattr(c, "column", None)
            if col_side is not None and hasattr(col_side, "name"):
                col_name = getattr(col_side, "name", None)
            if col_name is None:
                return None
            asc = op not in _DESC_OPS
        elif hasattr(c, "name"):
            col_name = getattr(c, "name", None)
            if isinstance(ascending_param, bool):
                asc = ascending_param
            elif isinstance(ascending_param, (list, tuple)) and i < len(
                ascending_param
            ):
                asc = bool(ascending_param[i])
            else:
                asc = True
        else:
            return None
        if not isinstance(col_name, str):
            return None
        col_names.append(col_name)
        asc_list.append(asc)
    return (col_names, asc_list)


# Optional import; materializer is only used when backend_type="robin" and robin_sparkless is installed
try:
    import robin_sparkless  # noqa: F401
except ImportError:
    robin_sparkless = None  # type: ignore[assignment]


def _robin_available() -> bool:
    return robin_sparkless is not None


def _get_robin_session_and_create_from_rows():  # type: () -> Tuple[Any, Any]
    """Return (spark_session, create_from_rows_callable) for Robin DataFrame creation.

    Uses robin_sparkless.SparkSession.builder().get_or_create() only. No fallback.
    Uses create_dataframe_from_rows (public) or _create_dataframe_from_rows (private).
    """
    F = robin_sparkless  # type: ignore[union-attr]
    spark = F.SparkSession.builder().app_name("sparkless-robin-poc").get_or_create()
    create_fn = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows", None
    )
    if create_fn is None:
        raise RuntimeError(
            "Robin backend requires a SparkSession with create_dataframe_from_rows (or "
            "_create_dataframe_from_rows). Ensure robin-sparkless is installed and that "
            "the session is created with backend_type='robin'."
        )
    return (spark, create_fn)


def _simple_filter_to_robin(condition: Any) -> Any:
    """Translate a Sparkless filter to robin_sparkless Column expression.

    Supports: ColumnOperation with op in (>, <, >=, <=, ==, !=), column = Column(name),
    value = scalar or Literal; and ColumnOperation("&", left, right) / ("|", left, right)
    recursively. Returns None if not supported.
    """
    from sparkless.functions import ColumnOperation
    from sparkless.functions.core.column import Column
    from sparkless.functions.core.literals import Literal

    if not _robin_available():
        return None
    F = robin_sparkless  # type: ignore[union-attr]

    if isinstance(condition, ColumnOperation):
        op = getattr(condition, "operation", None)
        col_side = getattr(condition, "column", None)
        val_side = getattr(condition, "value", None)
        # AND/OR: recurse
        if op == "&":
            left_expr = _simple_filter_to_robin(col_side)
            right_expr = _simple_filter_to_robin(val_side)
            if left_expr is not None and right_expr is not None:
                return left_expr & right_expr
            return None
        if op == "|":
            left_expr = _simple_filter_to_robin(col_side)
            right_expr = _simple_filter_to_robin(val_side)
            if left_expr is not None and right_expr is not None:
                return left_expr | right_expr
            return None
        # isin: ColumnOperation("isin", column=Column, value=list)
        # Robin doesn't have col.isin([...]); translate to (col==v1) | (col==v2) | ...
        if op == "isin":
            if not isinstance(col_side, Column):
                return None
            col_name = getattr(col_side, "name", None)
            if not isinstance(col_name, str):
                return None
            values = val_side if isinstance(val_side, (list, tuple)) else []
            if not values:
                return None
            robin_col = F.col(col_name)
            # Build OR chain: (col==v1) | (col==v2) | ...
            result: Any = None
            for v in values:
                lit_val = F.lit(_lit_value_for_robin(v))  # type: ignore[arg-type]
                eq_expr = robin_col.eq(lit_val)
                result = eq_expr if result is None else (result | eq_expr)
            return result
        # like: ColumnOperation("like", column, pattern)
        if op == "like":
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            pattern = (
                val_side
                if isinstance(val_side, str)
                else (
                    getattr(val_side, "value", None)
                    if isinstance(val_side, Literal)
                    else None
                )
            )
            if pattern is None or not isinstance(pattern, str):
                return None
            if hasattr(inner, "like") and callable(getattr(inner, "like")):
                return inner.like(pattern)
            if hasattr(F, "like") and callable(getattr(F, "like")):
                return F.like(inner, pattern)  # type: ignore[union-attr]
            return None
        # isnull / isnotnull: unary (val_side is None)
        if op == "isnull":
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            if hasattr(inner, "isnull") and callable(getattr(inner, "isnull")):
                return inner.isnull()
            if hasattr(inner, "isNull") and callable(getattr(inner, "isNull")):
                return inner.isNull()
            return None
        if op == "isnotnull":
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            if hasattr(inner, "isnotnull") and callable(getattr(inner, "isnotnull")):
                return inner.isnotnull()
            if hasattr(inner, "isNotNull") and callable(getattr(inner, "isNotNull")):
                return inner.isNotNull()
            return None
        if op not in (">", "<", ">=", "<=", "==", "!="):
            return None
        # Left side: expression (Column or any translatable expression)
        left_expr = _expression_to_robin(col_side) if col_side is not None else None
        if left_expr is None:
            return None
        # Right side: Literal/scalar or expression
        if isinstance(val_side, Literal):
            val = getattr(val_side, "value", val_side)
            right_expr = F.lit(_lit_value_for_robin(val))  # type: ignore[arg-type]
        else:
            right_expr = _expression_to_robin(val_side)
            if right_expr is None:
                right_expr = F.lit(_lit_value_for_robin(val_side))  # type: ignore[arg-type]
        if right_expr is None:
            return None
        if op == ">":
            return left_expr.gt(right_expr)
        if op == "<":
            return left_expr.lt(right_expr)
        if op == ">=":
            return left_expr.ge(right_expr)
        if op == "<=":
            return left_expr.le(right_expr)
        if op == "==":
            return left_expr.eq(right_expr)
        if op == "!=":
            return left_expr.ne(right_expr)
    return None


def _join_on_to_column_names(on: Any) -> List[str] | None:
    """Extract join column names from Sparkless join condition for robin_sparkless join(on=...).

    Supports: str -> [str]; list/tuple of str -> list; ColumnOperation(==, col, col) when
    both sides have .name (same name -> one key); ColumnOperation(&, left, right) when both
    sides return lists of same names. Returns None if not supported.

    Limitation: different-name join keys (e.g. left["id"] == right["id_right"]) are not
    supported; use same-named columns or on="col" / on=["col", ...] (see #473).
    """
    from sparkless.functions import ColumnOperation
    from sparkless.functions.core.column import Column

    if isinstance(on, str):
        return [on]
    if isinstance(on, (list, tuple)):
        if all(isinstance(x, str) for x in on):
            return list(on)
        return None
    if isinstance(on, ColumnOperation):
        op = getattr(on, "operation", None)
        col_side = getattr(on, "column", None)
        val_side = getattr(on, "value", None)
        if op == "==":
            if isinstance(col_side, Column) and isinstance(val_side, Column):
                left_name = getattr(col_side, "name", None)
                right_name = getattr(val_side, "name", None)
                if isinstance(left_name, str) and isinstance(right_name, str):
                    # Robin join(on=[...]) expects same-named columns in both DFs
                    if left_name == right_name:
                        return [left_name]
                    # Different names: robin may need left_on/right_on; for now only same name
                    return None
            return None
        if op == "&":
            left_list = _join_on_to_column_names(col_side) if col_side else None
            right_list = _join_on_to_column_names(val_side) if val_side else None
            if (
                left_list is not None
                and right_list is not None
                and left_list == right_list
            ):
                return left_list
            return None
    return None


def _lit_value_for_robin(val: Any) -> Any:
    """Convert value for F.lit(); Robin supports only None, int, float, bool, str."""
    if val is None or isinstance(val, (int, float, bool, str)):
        return val
    # Serialize date/datetime to string to avoid robin lit() TypeError
    if hasattr(val, "isoformat"):
        return val.isoformat() if hasattr(val, "isoformat") else str(val)
    return str(val)


def _expression_to_robin(expr: Any) -> Any:
    """Translate a Sparkless Column expression to robin_sparkless Column.

    Supports: Column, Literal, alias; arithmetic (+, -, *, /, %); comparisons;
    unary (upper, lower, length, trim, abs, year, month, isnull, ...);
    binary (split, substring, replace, left, right, add_months);
    concat, struct; current_timestamp/current_date.
    Returns None if not supported.
    """
    from sparkless.functions import ColumnOperation
    from sparkless.functions.core.column import Column
    from sparkless.functions.core.literals import Literal

    if not _robin_available():
        return None
    F = robin_sparkless  # type: ignore[union-attr]

    # Plain Column only; ColumnOperation is a subclass of Column but must be translated
    # as an expression (e.g. alias -> inner.alias(name)), not as F.col(name).
    if isinstance(expr, Column) and not isinstance(expr, ColumnOperation):
        name = getattr(expr, "name", None)
        if isinstance(name, str):
            return F.col(name)
        return None
    if isinstance(expr, Literal):
        val = getattr(expr, "value", expr)
        safe_val = _lit_value_for_robin(val)
        return F.lit(safe_val)  # type: ignore[arg-type]
    # CaseWhen -> Robin when(cond).then(val).otherwise(default)
    try:
        from sparkless.functions.conditional import CaseWhen
    except ImportError:
        CaseWhen = None  # type: ignore[misc, assignment]
    if CaseWhen is not None and isinstance(expr, CaseWhen):
        conditions = getattr(expr, "conditions", [])
        default_value = getattr(expr, "default_value", None)
        if not conditions or default_value is None:
            return None
        cond_robin = _expression_to_robin(conditions[0][0])
        val_robin = _expression_to_robin(conditions[0][1])
        if cond_robin is None or val_robin is None:
            return None
        # Robin: when(cond).then(val).when(cond2).then(val2).otherwise(default)
        out = F.when(cond_robin).then(val_robin)
        for cond, val in conditions[1:]:
            c = _expression_to_robin(cond)
            v = _expression_to_robin(val)
            if c is None or v is None:
                return None
            if hasattr(out, "when") and callable(out.when):
                out = out.when(c).then(v)
            else:
                return None
        default_robin = _expression_to_robin(default_value)
        if default_robin is None:
            return None
        if hasattr(out, "otherwise") and callable(out.otherwise):
            return out.otherwise(default_robin)
        return None
    # WindowFunction -> Robin Column.row_number()/rank()/etc. .over(partition_by)
    try:
        from sparkless.functions.window_execution import WindowFunction
    except ImportError:
        WindowFunction = None  # type: ignore[misc, assignment]
    if WindowFunction is not None and isinstance(expr, WindowFunction):
        spec = getattr(expr, "window_spec", None)
        if spec is None:
            return None
        part_by = getattr(spec, "_partition_by", []) or []
        order_by = getattr(spec, "_order_by", []) or []
        partition_names = []
        for c in part_by:
            if isinstance(c, str):
                partition_names.append(c)
            elif hasattr(c, "name") and isinstance(getattr(c, "name"), str):
                partition_names.append(c.name)
        order_names = []
        for c in order_by:
            if isinstance(c, str):
                order_names.append(c)
            elif hasattr(c, "name") and isinstance(getattr(c, "name"), str):
                order_names.append(c.name)
        partition_by = partition_names if partition_names else order_names
        if not partition_by:
            partition_by = []
        base_col = (partition_by[0] if partition_by else None) or "__dummy__"
        base = F.col(base_col)
        fn_name = (getattr(expr, "function_name", None) or "row_number").lower()
        descending = False
        if (
            order_names
            and hasattr(spec, "_order_by")
            and (order_cols := getattr(spec, "_order_by", None))
            and hasattr(order_cols[0], "operation")
            and getattr(order_cols[0], "operation", None) == "desc"
        ):
            descending = True
        offset = getattr(expr, "offset", 1) or 1
        if fn_name == "row_number":
            if hasattr(base, "row_number"):
                return base.row_number(descending=descending).over(partition_by)
        elif fn_name == "rank":
            if hasattr(base, "rank"):
                return base.rank(descending=descending).over(partition_by)
        elif fn_name == "dense_rank":
            if hasattr(base, "dense_rank"):
                return base.dense_rank(descending=descending).over(partition_by)
        elif fn_name == "lag":
            if hasattr(base, "lag"):
                return base.lag(offset).over(partition_by)
        elif fn_name == "lead":
            if hasattr(base, "lead"):
                return base.lead(offset).over(partition_by)
        elif fn_name == "first_value":
            if hasattr(base, "first_value"):
                return base.first_value().over(partition_by)
        elif fn_name == "last_value":
            if hasattr(base, "last_value"):
                return base.last_value().over(partition_by)
        elif fn_name == "percent_rank":
            if hasattr(base, "percent_rank"):
                try:
                    return base.percent_rank(partition_by, descending)  # type: ignore[call-arg]
                except TypeError:
                    return base.percent_rank().over(partition_by)  # type: ignore[call-arg]
        elif fn_name in ("sum", "avg", "min", "max"):
            col_name = getattr(expr, "column_name", None) or base_col
            col = F.col(col_name) if isinstance(col_name, str) else base
            if fn_name == "sum" and hasattr(col, "sum"):
                return col.sum().over(partition_by)
            if fn_name == "avg" and hasattr(col, "avg"):
                return col.avg().over(partition_by)
            if fn_name == "min" and hasattr(col, "min"):
                return col.min().over(partition_by)
            if fn_name == "max" and hasattr(col, "max"):
                return col.max().over(partition_by)
        return None
    # ColumnOperation or any object with operation/column/value (e.g. from sparkless.sql)
    if isinstance(expr, ColumnOperation) or (
        hasattr(expr, "operation") and hasattr(expr, "column")
    ):
        op = getattr(expr, "operation", None)
        col_side = getattr(expr, "column", None)
        val_side = getattr(expr, "value", None)
        # Alias: must translate to inner.alias(name) so Robin uses expression, not column lookup
        if op == "alias":
            inner = _expression_to_robin(col_side) if col_side is not None else None
            if inner is None:
                return None
            alias_name = val_side if isinstance(val_side, str) else None
            if alias_name is not None and hasattr(inner, "alias"):
                return inner.alias(alias_name)
            return inner
        # lit(): ColumnOperation(None, "lit", value)
        if op == "lit":
            if isinstance(val_side, Literal):
                val = getattr(val_side, "value", val_side)
            else:
                val = val_side
            safe_val = _lit_value_for_robin(val)
            return F.lit(safe_val)  # type: ignore[arg-type]
        # Cast / astype (Phase 7; astype is an alias for cast)
        if op in ("cast", "astype") and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            if val_side is None:
                robin_type = "string"
            elif hasattr(val_side, "simpleString") and callable(val_side.simpleString):
                type_str = val_side.simpleString()
                robin_type = _SPARK_TYPE_TO_ROBIN_DTYPE.get(
                    type_str.lower()
                    if isinstance(type_str, str)
                    else str(type_str).lower(),
                    "string",
                )
            elif isinstance(val_side, str):
                robin_type = _SPARK_TYPE_TO_ROBIN_DTYPE.get(val_side.lower(), "string")
            else:
                robin_type = _spark_type_to_robin_dtype(val_side)
            if hasattr(inner, "cast"):
                return inner.cast(robin_type)
            if hasattr(F, "cast") and callable(F.cast):
                return F.cast(inner, robin_type)
            return None
        # Unary functions (column, op, None or no value)
        unary_ops = (
            "upper",
            "lower",
            "length",
            "char_length",
            "trim",
            "ltrim",
            "rtrim",
            "abs",
            "sqrt",
            "year",
            "month",
            "dayofmonth",
            "hour",
            "minute",
            "second",
            "isnull",
            "isnotnull",
            "dayofweek",
            "dayofyear",
            "weekofyear",
            "quarter",
        )
        if op in unary_ops and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            fn = getattr(F, op, None)
            if callable(fn):
                return fn(inner)
        # No-column functions
        if (
            op == "current_timestamp"
            and col_side is None
            and hasattr(F, "current_timestamp")
        ):
            return F.current_timestamp()
        if op == "current_date" and col_side is None and hasattr(F, "current_date"):
            return F.current_date()
        # Binary: (column, op, scalar_or_tuple)
        if op == "split" and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            delimiter = ","
            limit: Any = None
            if isinstance(val_side, str):
                delimiter = val_side
            elif isinstance(val_side, (list, tuple)) and val_side:
                delimiter = str(val_side[0])
                if len(val_side) > 1:
                    limit = val_side[1]
            if hasattr(F, "split"):
                split_fn = getattr(F, "split")
                try:
                    if limit is not None:
                        return split_fn(inner, delimiter, limit)  # type: ignore[call-arg]
                    return split_fn(inner, delimiter)  # type: ignore[call-arg]
                except TypeError:
                    # Older robin-sparkless may not accept a limit argument
                    return split_fn(inner, delimiter)  # type: ignore[call-arg]
        if op in ("substring", "substr") and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            if isinstance(val_side, (list, tuple)) and len(val_side) >= 2:
                pos, length = int(val_side[0]), int(val_side[1])
            elif isinstance(val_side, (list, tuple)) and len(val_side) == 1:
                pos = int(val_side[0])
                length = -1
            else:
                return None
            # Robin may expose substring or substr; try both (Phase 7)
            if hasattr(F, "substring") and callable(F.substring):
                return F.substring(inner, pos, length)
            if hasattr(F, "substr") and callable(F.substr):
                return F.substr(inner, pos, length)
        if op == "replace" and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            if isinstance(val_side, (list, tuple)) and len(val_side) >= 2:
                search, replacement = str(val_side[0]), str(val_side[1])
            else:
                return None
            if hasattr(F, "replace"):
                return F.replace(inner, search, replacement)
        if op == "regexp_replace" and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            pattern = (
                val_side[0]
                if isinstance(val_side, (list, tuple)) and val_side
                else val_side
            )
            replacement = (
                val_side[1]
                if isinstance(val_side, (list, tuple)) and len(val_side) > 1
                else ""
            )
            if not isinstance(pattern, str):
                pattern = str(pattern) if pattern is not None else ""
            if not isinstance(replacement, str):
                replacement = str(replacement)
            if hasattr(F, "regexp_replace"):
                return F.regexp_replace(inner, pattern, replacement)
        if op == "left" and col_side is not None and isinstance(val_side, (int, float)):
            inner = _expression_to_robin(col_side)
            if inner is not None and hasattr(F, "left"):
                return F.left(inner, int(val_side))
        if (
            op == "right"
            and col_side is not None
            and isinstance(val_side, (int, float))
        ):
            inner = _expression_to_robin(col_side)
            if inner is not None and hasattr(F, "right"):
                return F.right(inner, int(val_side))
        if (
            op == "add_months"
            and col_side is not None
            and isinstance(val_side, (int, float))
        ):
            inner = _expression_to_robin(col_side)
            if inner is not None and hasattr(F, "add_months"):
                return F.add_months(inner, int(val_side))
        # concat: value is list of columns (col_side is first)
        if op == "concat":
            parts = [col_side] if col_side is not None else []
            if isinstance(val_side, (list, tuple)):
                parts.extend(val_side)
            else:
                parts.append(val_side)
            robin_cols = [_expression_to_robin(p) for p in parts]
            if None in robin_cols or not robin_cols:
                return None
            # If exactly one part is a string Literal (separator), use concat_ws
            from sparkless.functions.core.literals import Literal

            literal_parts = [
                (i, getattr(p, "value", p))
                for i, p in enumerate(parts)
                if isinstance(p, Literal)
            ]
            if (
                len(parts) >= 3
                and len(literal_parts) == 1
                and isinstance(literal_parts[0][1], str)
            ):
                sep = literal_parts[0][1]
                col_parts = [p for i, p in enumerate(parts) if i != literal_parts[0][0]]
                ws_robin = [_expression_to_robin(p) for p in col_parts]
                if None not in ws_robin and hasattr(F, "concat_ws"):
                    return F.concat_ws(sep, *ws_robin)
            if hasattr(F, "concat"):
                return F.concat(*robin_cols)
        # concat_ws: value is (sep, columns[1:])
        if op == "concat_ws" and col_side is not None:
            if not isinstance(val_side, (list, tuple)) or len(val_side) < 1:
                return None
            sep = val_side[0]
            rest = val_side[1] if len(val_side) > 1 else []
            if not isinstance(rest, (list, tuple)):
                rest = [rest]
            robin_cols = [_expression_to_robin(col_side)] + [
                _expression_to_robin(c) for c in rest
            ]
            if None in robin_cols:
                return None
            if hasattr(F, "concat_ws"):
                return F.concat_ws(sep, *robin_cols)
        # struct: value is tuple/list of columns
        if op == "struct":
            cols = (
                val_side
                if isinstance(val_side, (list, tuple))
                else ([col_side] if col_side else [])
            )
            if col_side and not cols:
                cols = [col_side]
            robin_cols = [_expression_to_robin(c) for c in cols]
            if None in robin_cols or not robin_cols:
                return None
            if hasattr(F, "struct"):
                return F.struct(*robin_cols)
        # format_string(format_str, *cols): ColumnOperation(base, "format_string", (format_str, rest))
        if op == "format_string":
            if not isinstance(val_side, (list, tuple)) or not val_side:
                return None
            format_str = val_side[0]
            rest = val_side[1] if len(val_side) > 1 else []
            if not isinstance(rest, (list, tuple)):
                rest = [rest]
            cols = [col_side] + list(rest)
            robin_args = [_expression_to_robin(c) for c in cols]
            if None in robin_args:
                return None
            if not isinstance(format_str, str):
                format_str = str(format_str)
            if hasattr(F, "format_string") and callable(getattr(F, "format_string")):
                return F.format_string(format_str, *robin_args)  # type: ignore[call-arg]
            if hasattr(F, "printf") and callable(getattr(F, "printf")):
                # Some APIs expose printf instead of format_string
                return F.printf(format_str, *robin_args)  # type: ignore[call-arg]
        # create_map: value is tuple of key-value columns (k1, v1, k2, v2, ...)
        if op == "create_map":
            parts = list(val_side) if isinstance(val_side, (list, tuple)) else []
            robin_cols = [_expression_to_robin(p) for p in parts]
            if None in robin_cols:
                return None
            if hasattr(F, "create_map") and callable(F.create_map):
                return F.create_map(*robin_cols)
        # map_from_entries: value is single column (array of structs)
        if op == "map_from_entries" and col_side is not None:
            inner = _expression_to_robin(col_side)
            if (
                inner is not None
                and hasattr(F, "map_from_entries")
                and callable(F.map_from_entries)
            ):
                return F.map_from_entries(inner)
        # withField: Robin lacks struct mutation (robin-sparkless #195, #198).
        # Documented as gap; return None so caller falls back.
        if op == "withField":
            return None
        # getItem (Phase 7): column[key] for array index or map key
        # Robin may expose getItem, element_at, or __getitem__
        if op == "getItem" and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            key = val_side  # int for array, str or other for map
            if hasattr(inner, "getItem"):
                return inner.getItem(key)
            if hasattr(F, "getItem") and callable(F.getItem):
                return F.getItem(inner, key)
            if hasattr(F, "element_at") and callable(F.element_at) and key is not None:
                return F.element_at(inner, key)  # type: ignore[arg-type]
            if isinstance(key, (int, str)):
                try:
                    return inner[key]
                except (TypeError, KeyError):
                    pass
            return None
        # to_date, to_timestamp (column [, format])
        if op in ("to_date", "to_timestamp") and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            fmt = val_side if isinstance(val_side, str) else None
            fn = getattr(F, op, None)
            if callable(fn):
                try:
                    if fmt is not None:
                        return fn(inner, fmt)
                    return fn(inner)
                except TypeError:
                    # Some robin-sparkless versions may not support the format argument
                    return fn(inner)
            # Some APIs may expose to_date/to_timestamp as Column methods
            method = getattr(inner, op, None)
            if callable(method):
                try:
                    if fmt is not None:
                        return method(fmt)
                    return method()
                except TypeError:
                    return method()
            return None
        # round(column, scale)
        if op == "round" and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            scale = 0
            if isinstance(val_side, (int, float)):
                scale = int(val_side)
            precision = getattr(expr, "precision", None)
            if isinstance(precision, int):
                scale = precision
            if hasattr(F, "round") and callable(getattr(F, "round")):
                try:
                    return F.round(inner, scale)  # type: ignore[call-arg]
                except TypeError:
                    # Older robin-sparkless may only accept a single-argument round
                    return F.round(inner)  # type: ignore[call-arg]
            return None
        # log / log10
        if op == "log10" and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            if hasattr(F, "log10") and callable(getattr(F, "log10")):
                return F.log10(inner)  # type: ignore[call-arg]
            if hasattr(F, "log") and callable(getattr(F, "log")):
                try:
                    return F.log(10, inner)  # type: ignore[call-arg]
                except TypeError:
                    return None
            return None
        if op == "log" and col_side is not None:
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            base_any: Any = val_side
            # log(column) -> natural log
            if base_any is None:
                if hasattr(F, "log") and callable(getattr(F, "log")):
                    try:
                        return F.log(inner)  # type: ignore[call-arg]
                    except TypeError:
                        pass
                if hasattr(F, "ln") and callable(getattr(F, "ln")):
                    return F.ln(inner)  # type: ignore[call-arg]
                return None
            # log(base, column)
            if isinstance(base_any, (int, float)):
                arg_base: Any = base_any
            else:
                arg_base = _expression_to_robin(base_any)
            if arg_base is None:
                return None
            if hasattr(F, "log") and callable(getattr(F, "log")):
                try:
                    return F.log(arg_base, inner)  # type: ignore[call-arg]
                except TypeError:
                    return None
            return None
        # array(...): ColumnOperation(first_col, "array", value=rest_cols or ())
        if op == "array":
            # Empty array: base column is synthetic "__array_empty_base__"
            if (
                (val_side == () or val_side is None)
                and col_side is not None
                and getattr(col_side, "name", None) == "__array_empty_base__"
            ):
                if hasattr(F, "array") and callable(getattr(F, "array")):
                    return F.array()  # type: ignore[call-arg]
                return None
            cols = [col_side] if col_side is not None else []
            if isinstance(val_side, (list, tuple)):
                cols.extend(val_side)
            robin_cols = [_expression_to_robin(c) for c in cols]
            if not robin_cols or None in robin_cols:
                return None
            if hasattr(F, "array") and callable(getattr(F, "array")):
                return F.array(*robin_cols)  # type: ignore[call-arg]
            return None
        # explode(array) and related functions
        if op in ("explode", "explode_outer", "posexplode", "posexplode_outer") and (
            col_side is not None
        ):
            inner = _expression_to_robin(col_side)
            if inner is None:
                return None
            fn = getattr(F, op, None)
            if callable(fn):
                return fn(inner)  # type: ignore[call-arg]
            method = getattr(inner, op, None)
            if callable(method):
                return method()
            return None
        # Arithmetic and comparison
        # For robin-sparkless 0.4.0, build literal+column in one shot so the
        # Column is created in a single expression (avoids recursive Column
        # being treated as name lookup in with_column).
        if (
            op in ("+", "-", "*", "/", "%")
            and isinstance(col_side, Literal)
            and isinstance(val_side, Column)
        ):
            lit_val = _lit_value_for_robin(getattr(col_side, "value", col_side))
            col_name = getattr(val_side, "name", None)
            if isinstance(col_name, str):
                # Use module reference so we get the same Column type Robin expects
                R = robin_sparkless  # type: ignore[union-attr]
                if op == "+":
                    return R.lit(lit_val) + R.col(col_name)
                if op == "-":
                    return R.lit(lit_val) - R.col(col_name)
                if op == "*":
                    return R.lit(lit_val) * R.col(col_name)
                if op == "/":
                    return R.lit(lit_val) / R.col(col_name)
                if op == "%" and hasattr(R.lit(lit_val), "__mod__"):
                    return R.lit(lit_val) % R.col(col_name)
        if (
            op in ("+", "-", "*", "/", "%")
            and isinstance(val_side, Literal)
            and isinstance(col_side, Column)
        ):
            lit_val = _lit_value_for_robin(getattr(val_side, "value", val_side))
            col_name = getattr(col_side, "name", None)
            if isinstance(col_name, str):
                R = robin_sparkless  # type: ignore[union-attr]
                if op == "+":
                    return R.col(col_name) + R.lit(lit_val)
                if op == "-":
                    return R.col(col_name) - R.lit(lit_val)
                if op == "*":
                    return R.col(col_name) * R.lit(lit_val)
                if op == "/":
                    return R.col(col_name) / R.lit(lit_val)
                if op == "%":
                    return R.col(col_name) % R.lit(lit_val)
        # col * 2 or col + 2 where 2 is plain int/float (not Literal)
        if op in ("+", "-", "*", "/", "%") and isinstance(col_side, Column):
            col_name = getattr(col_side, "name", None)
            if (
                isinstance(col_name, str)
                and val_side is not None
                and isinstance(val_side, (int, float, str, bool, type(None)))
            ):
                R = robin_sparkless  # type: ignore[union-attr]
                lit_val = _lit_value_for_robin(val_side)
                if op == "+":
                    return R.col(col_name) + R.lit(lit_val)
                if op == "-":
                    return R.col(col_name) - R.lit(lit_val)
                if op == "*":
                    return R.col(col_name) * R.lit(lit_val)
                if op == "/":
                    return R.col(col_name) / R.lit(lit_val)
                if op == "%":
                    return R.col(col_name) % R.lit(lit_val)
        left = _expression_to_robin(col_side) if col_side is not None else None
        right = _expression_to_robin(val_side) if val_side is not None else None
        # Window + scalar / scalar + window: treat scalar as literal so one Robin expression (#471)
        if op in ("+", "-", "*", "/", "%"):
            if (
                right is None
                and val_side is not None
                and isinstance(val_side, (int, float, str, bool, type(None)))
            ):
                right = F.lit(_lit_value_for_robin(val_side))  # type: ignore[arg-type]
            if (
                left is None
                and col_side is not None
                and isinstance(col_side, (int, float, str, bool, type(None)))
            ):
                left = F.lit(_lit_value_for_robin(col_side))  # type: ignore[arg-type]
        if left is None or right is None:
            return None
        if isinstance(col_side, Literal) and op in ("+", "*", "==", "!="):
            left, right = right, left
        if op in (">", "<", ">=", "<=", "==", "!="):
            if op == ">":
                return left.gt(right)
            if op == "<":
                return left.lt(right)
            if op == ">=":
                return left.ge(right)
            if op == "<=":
                return left.le(right)
            if op == "==":
                return left.eq(right)
            if op == "!=":
                return left.ne(right)
        if op in ("+", "-", "*", "/"):
            if op == "+":
                return left + right
            if op == "-":
                return left - right
            if op == "*":
                return left * right
            if op == "/":
                return left / right
        if op == "%" and hasattr(left, "__mod__"):
            return left % right
    return None


class RobinMaterializer:
    """DataMaterializer using robin_sparkless.

    Supports: filter (simple col op literal), select (list of str), limit,
    orderBy (column names + ascending), withColumn (simple expressions).
    Uses create_dataframe_from_rows for arbitrary schema.
    """

    SUPPORTED_OPERATIONS = {
        "filter",
        "select",
        "limit",
        "orderBy",
        "withColumn",
        "join",
        "union",
        "distinct",
        "drop",
    }

    def __init__(self) -> None:
        pass

    def _can_handle_filter(self, payload: Any) -> bool:
        return _simple_filter_to_robin(payload) is not None

    def _can_handle_distinct(self, payload: Any) -> bool:
        return payload == () or payload is None

    def _can_handle_drop(self, payload: Any) -> bool:
        cols = payload if isinstance(payload, (list, tuple)) else [payload]
        return len(cols) > 0 and all(isinstance(c, str) for c in cols)

    def _can_handle_select(self, payload: Any) -> bool:
        if not isinstance(payload, (list, tuple)):
            return False
        for c in payload:
            if isinstance(c, str):
                continue
            # Resolve aliased columns: Column("m", _original_column=expr) -> check expr.
            # ColumnOperation inherits _original_column=None from Column; use c when None.
            expr_to_check = getattr(c, "_original_column", None)
            if expr_to_check is None:
                expr_to_check = c
            if _expression_to_robin(expr_to_check) is None:
                return False
        return True

    def _can_handle_order_by(self, payload: Any) -> bool:
        return _order_by_payload_to_robin(payload) is not None

    def _can_handle_with_column(self, payload: Any) -> bool:
        if not isinstance(payload, (list, tuple)) or len(payload) != 2:
            return False
        return _expression_to_robin(payload[1]) is not None

    def _can_handle_join(self, payload: Any) -> bool:
        if not isinstance(payload, (list, tuple)) or len(payload) < 3:
            return False
        _other, on, how = payload[0], payload[1], payload[2]
        if _join_on_to_column_names(on) is not None or isinstance(on, str):
            pass
        elif isinstance(on, (list, tuple)):
            if not all(isinstance(x, str) for x in on):
                return False
        else:
            return False
        return how in (
            "inner",
            "left",
            "right",
            "outer",
            "full",
            "left_semi",
            "leftsemi",
            "left_anti",
            "leftanti",
            "semi",
            "anti",
        )

    def can_handle_operation(self, op_name: str, op_payload: Any) -> bool:
        if op_name not in self.SUPPORTED_OPERATIONS:
            return False
        if op_name == "filter":
            return self._can_handle_filter(op_payload)
        if op_name == "select":
            return self._can_handle_select(op_payload)
        if op_name == "limit":
            return isinstance(op_payload, int) and op_payload >= 0
        if op_name == "orderBy":
            return self._can_handle_order_by(op_payload)
        if op_name == "withColumn":
            return self._can_handle_with_column(op_payload)
        if op_name == "join":
            return self._can_handle_join(op_payload)
        if op_name == "distinct":
            return self._can_handle_distinct(op_payload)
        if op_name == "drop":
            return self._can_handle_drop(op_payload)
        return op_name == "union"

    def can_handle_operations(
        self, operations: List[Tuple[str, Any]]
    ) -> Tuple[bool, List[str]]:
        unsupported: List[str] = []
        for op_name, op_payload in operations:
            if not self.can_handle_operation(op_name, op_payload):
                unsupported.append(op_name)
        return (len(unsupported) == 0, unsupported)

    def materialize_from_plan(
        self,
        data: List[Any],
        schema: StructType,
        logical_plan: List[Any],
    ) -> List[Row]:
        """Plan-based execution entry point for the Robin backend.

        Calls the in-repo Robin plan executor (Phase 5). On unsupported op or
        expression the executor raises ValueError so the lazy engine falls back
        to the operation-by-operation materialize() path.
        """
        from sparkless.backend.robin.plan_executor import execute_robin_plan

        return execute_robin_plan(data, schema, logical_plan)

    def materialize(
        self,
        data: List[Any],
        schema: StructType,
        operations: List[Tuple[str, Any]],
    ) -> List[Row]:
        if not _robin_available():
            raise RuntimeError(
                "robin_sparkless is not installed. "
                "Install with: pip install sparkless[robin] (or pip install robin-sparkless)."
            )
        F = robin_sparkless  # type: ignore[union-attr]
        if not schema.fields:
            raise ValueError("RobinMaterializer requires a non-empty schema")
        # For initial creation, use columns from data to support operations like drop().
        # The passed schema may be the final projected schema (e.g. after drop) which
        # would omit columns we need for intermediate ops. Use data keys first.
        name_to_field = {f.name: f for f in schema.fields}
        if data:
            if isinstance(data[0], dict):
                init_names = list(data[0].keys())
            elif hasattr(data[0], "_fields"):
                init_names = list(data[0]._fields)
            else:
                init_names = [f.name for f in schema.fields]
            # Include all columns present in data; use schema for types, default string if missing
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

        for op_name, payload in operations:
            if op_name == "filter":
                expr = _simple_filter_to_robin(payload)
                if expr is not None:
                    df = df.filter(expr)
            elif op_name == "select":
                cols = (
                    list(payload) if isinstance(payload, (list, tuple)) else [payload]
                )
                robin_cols: List[Any] = []
                for c in cols:
                    if isinstance(c, str):
                        robin_cols.append(c)
                    else:
                        # Always translate the full expression (c) so that alias is preserved.
                        # Robin resolves columns by name; passing an alias name as a column
                        # would make Robin look up a non-existent column (e.g. 'full_name').
                        # _expression_to_robin(c) handles ColumnOperation(alias, ...) by
                        # returning inner.alias(alias_name).
                        expr = _expression_to_robin(c)
                        if expr is None:
                            from sparkless.core.exceptions.operation import (
                                SparkUnsupportedOperationError,
                            )

                            raise SparkUnsupportedOperationError(
                                operation="select",
                                reason="Backend 'robin' does not support this select expression",
                                alternative="In v4 only the Robin backend is supported; see docs/v4_behavior_changes_and_known_differences.md for supported expressions.",
                            )
                        robin_cols.append(expr)
                df = df.select(*robin_cols)
            elif op_name == "limit":
                df = df.limit(payload)
            elif op_name == "orderBy":
                ob = _order_by_payload_to_robin(payload)
                if ob is None:
                    raise ValueError("orderBy payload not supported")
                col_names, asc_list = ob
                df = df.order_by(col_names, asc_list)
            elif op_name == "withColumn":
                col_name, expression = payload[0], payload[1]
                from sparkless.functions import ColumnOperation
                from sparkless.functions.core.column import Column
                from sparkless.functions.core.literals import Literal

                # Unwrap alias so we translate the inner expression; with_column(name, expr)
                # receives the name and a Robin Column expression (e.g. create_map(...)),
                # not a column reference to the new name (Robin would look up 'map_col' and fail).
                if (
                    isinstance(expression, ColumnOperation)
                    and getattr(expression, "operation", None) == "alias"
                ):
                    expression = getattr(expression, "column", expression)
                # Build simple literal+col / col*lit / col*scalar in this frame so
                # Robin 0.4.0 treats the argument as an expression, not a name lookup.
                robin_expr = None
                if isinstance(expression, ColumnOperation):
                    op = getattr(expression, "operation", None)
                    col_side = getattr(expression, "column", None)
                    val_side = getattr(expression, "value", None)
                    if op in ("+", "-", "*", "/"):
                        if isinstance(col_side, Literal) and isinstance(
                            val_side, Column
                        ):
                            lit_val = _lit_value_for_robin(
                                getattr(col_side, "value", col_side)
                            )
                            cname = getattr(val_side, "name", None)
                            if isinstance(cname, str):
                                if op == "+":
                                    robin_expr = F.lit(lit_val) + F.col(cname)
                                elif op == "-":
                                    robin_expr = F.lit(lit_val) - F.col(cname)
                                elif op == "*":
                                    robin_expr = F.lit(lit_val) * F.col(cname)
                                elif op == "/":
                                    robin_expr = F.lit(lit_val) / F.col(cname)
                        elif isinstance(val_side, Literal) and isinstance(
                            col_side, Column
                        ):
                            lit_val = _lit_value_for_robin(
                                getattr(val_side, "value", val_side)
                            )
                            cname = getattr(col_side, "name", None)
                            if isinstance(cname, str):
                                if op == "+":
                                    robin_expr = F.col(cname) + F.lit(lit_val)
                                elif op == "-":
                                    robin_expr = F.col(cname) - F.lit(lit_val)
                                elif op == "*":
                                    robin_expr = F.col(cname) * F.lit(lit_val)
                                elif op == "/":
                                    robin_expr = F.col(cname) / F.lit(lit_val)
                        elif (
                            isinstance(col_side, Column)
                            and val_side is not None
                            and isinstance(
                                val_side, (int, float, str, bool, type(None))
                            )
                        ):
                            cname = getattr(col_side, "name", None)
                            if isinstance(cname, str):
                                lit_val = _lit_value_for_robin(val_side)
                                if op == "+":
                                    robin_expr = F.col(cname) + F.lit(lit_val)
                                elif op == "-":
                                    robin_expr = F.col(cname) - F.lit(lit_val)
                                elif op == "*":
                                    robin_expr = F.col(cname) * F.lit(lit_val)
                                elif op == "/":
                                    robin_expr = F.col(cname) / F.lit(lit_val)
                if robin_expr is None:
                    robin_expr = _expression_to_robin(expression)
                if robin_expr is not None:
                    df = df.with_column(col_name, robin_expr)
            elif op_name == "join":
                other_df, on, how = payload[0], payload[1], payload[2]
                other_eager = other_df._materialize_if_lazy()
                other_names = [f.name for f in other_eager.schema.fields]
                other_robin_schema = [
                    (f.name, _spark_type_to_robin_dtype(f.dataType))
                    for f in other_eager.schema.fields
                ]
                other_robin_data = _data_to_robin_rows(other_eager.data, other_names)
                other_robin_df = create_from_rows(other_robin_data, other_robin_schema)
                how_str = "outer" if how == "full" else how
                if how_str in ("left_semi", "leftsemi", "left_anti", "leftanti"):
                    how_str = (
                        "left_semi"
                        if how_str in ("left_semi", "leftsemi")
                        else "left_anti"
                    )
                elif how in ("semi", "anti"):
                    how_str = "left_semi" if how == "semi" else "left_anti"
                on_names = _join_on_to_column_names(on)
                if on_names is not None:
                    on_arg = on_names
                else:
                    on_arg = [on] if isinstance(on, str) else list(on)
                # robin-sparkless expects on= as list (Vec), not string
                df = df.join(other_robin_df, on=on_arg, how=how_str)
            elif op_name == "union":
                other_df = payload
                other_eager = other_df._materialize_if_lazy()
                other_names = [f.name for f in other_eager.schema.fields]
                other_robin_schema = [
                    (f.name, _spark_type_to_robin_dtype(f.dataType))
                    for f in other_eager.schema.fields
                ]
                other_robin_data = _data_to_robin_rows(other_eager.data, other_names)
                other_robin_df = create_from_rows(other_robin_data, other_robin_schema)
                df = df.union(other_robin_df)
            elif op_name == "distinct":
                df = df.distinct()
            elif op_name == "drop":
                cols = (
                    list(payload) if isinstance(payload, (list, tuple)) else [payload]
                )
                df = df.drop(cols)

        collected = df.collect()
        # Use projected schema so column order and duplicate names (e.g. after join) are correct
        from sparkless.dataframe.schema.schema_manager import SchemaManager

        final_schema = SchemaManager.project_schema_with_operations(schema, operations)
        # Robin join renames duplicate columns with _right suffix (e.g. name_right).
        # Merge _right values into base column names to match PySpark's last-wins behavior.
        merged: List[dict] = []
        for d in collected:
            if isinstance(d, dict):
                row = dict(d)
                for k in list(row.keys()):
                    if k.endswith("_right"):
                        base = k[:-6]  # remove "_right"
                        row[base] = row[k]
                        del row[k]
                merged.append(row)
            else:
                merged.append(d)
        return [Row(d, schema=final_schema) for d in merged]

    def close(self) -> None:
        """Release resources. robin_sparkless SparkSession does not expose stop();
        the session is process-scoped and cleaned up on process exit."""
        pass
