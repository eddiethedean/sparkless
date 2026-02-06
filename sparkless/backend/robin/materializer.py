"""
Robin (robin-sparkless) materializer.

Implements DataMaterializer using robin_sparkless: create_dataframe_from_rows
for arbitrary schema, then filter, select, limit (and optionally orderBy, withColumn,
join, union). Unsupported operations cause can_handle_operations -> False so the
caller may raise SparkUnsupportedOperationError or use another backend.
"""

from __future__ import annotations

from typing import Any, List, Tuple

from sparkless.spark_types import Row, StructType

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


def _data_to_robin_rows(data: List[Any], names: List[str]) -> List[dict]:
    """Convert Sparkless row data to list of dicts for create_dataframe_from_rows."""
    rows: List[dict] = []
    for row in data:
        if isinstance(row, dict):
            rows.append({n: row.get(n) for n in names})
        elif isinstance(row, (list, tuple)):
            rows.append(dict(zip(names, list(row) + [None] * (len(names) - len(row)))))
        else:
            rows.append({n: getattr(row, n, None) for n in names})
    return rows


# Optional import; materializer is only used when backend_type="robin" and robin_sparkless is installed
try:
    import robin_sparkless  # noqa: F401
except ImportError:
    robin_sparkless = None  # type: ignore[assignment]


def _robin_available() -> bool:
    return robin_sparkless is not None


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
        if op not in (">", "<", ">=", "<=", "==", "!="):
            return None
        # Left side: Column (col name) or ColumnOperation (nested, not supported here)
        if not isinstance(col_side, Column):
            return None
        col_name = getattr(col_side, "name", None)
        if not isinstance(col_name, str):
            return None
        robin_col = F.col(col_name)
        # Right side: Literal or scalar
        if isinstance(val_side, Literal):
            val = getattr(val_side, "value", val_side)
        else:
            val = val_side
        robin_lit = F.lit(val)  # type: ignore[arg-type]
        if op == ">":
            return robin_col > robin_lit
        if op == "<":
            return robin_col < robin_lit
        if op == ">=":
            return robin_col >= robin_lit
        if op == "<=":
            return robin_col <= robin_lit
        if op == "==":
            return robin_col == robin_lit
        if op == "!=":
            return robin_col != robin_lit
    return None


def _join_on_to_column_names(on: Any) -> List[str] | None:
    """Extract join column names from Sparkless join condition for robin_sparkless join(on=...).

    Supports: str -> [str]; list/tuple of str -> list; ColumnOperation(==, col, col) when
    both sides have .name (same name -> one key); ColumnOperation(&, left, right) when both
    sides return lists of same names. Returns None if not supported.
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


def _expression_to_robin(expr: Any) -> Any:
    """Translate a Sparkless Column expression to robin_sparkless Column for withColumn.

    Supports: Column (name ref), Literal, and ColumnOperation with op in
    (+, -, *, /, >, <, >=, <=, ==, !=). Returns None if not supported.
    """
    from sparkless.functions import ColumnOperation
    from sparkless.functions.core.column import Column
    from sparkless.functions.core.literals import Literal

    if not _robin_available():
        return None
    F = robin_sparkless  # type: ignore[union-attr]

    if isinstance(expr, Column):
        name = getattr(expr, "name", None)
        if isinstance(name, str):
            return F.col(name)
        return None
    if isinstance(expr, Literal):
        val = getattr(expr, "value", expr)
        return F.lit(val)  # type: ignore[arg-type]
    if isinstance(expr, ColumnOperation):
        op = getattr(expr, "operation", None)
        col_side = getattr(expr, "column", None)
        val_side = getattr(expr, "value", None)
        left = _expression_to_robin(col_side) if col_side is not None else None
        right = _expression_to_robin(val_side) if val_side is not None else None
        if left is None or right is None:
            return None
        if op in (">", "<", ">=", "<=", "==", "!="):
            if op == ">":
                return left > right
            if op == "<":
                return left < right
            if op == ">=":
                return left >= right
            if op == "<=":
                return left <= right
            if op == "==":
                return left == right
            if op == "!=":
                return left != right
        if op == "+":
            return left + right
        if op == "-":
            return left - right
        if op == "*":
            return left * right
        if op == "/":
            return left / right
        return None
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
    }

    def __init__(self) -> None:
        pass

    def _can_handle_filter(self, payload: Any) -> bool:
        return _simple_filter_to_robin(payload) is not None

    def _can_handle_order_by(self, payload: Any) -> bool:
        if not isinstance(payload, (list, tuple)) or len(payload) < 1:
            return False
        columns, ascending = payload[0], payload[1] if len(payload) > 1 else True
        if isinstance(columns, (list, tuple)):
            cols_list = list(columns)
        elif isinstance(columns, str):
            cols_list = [columns]
        else:
            return False
        if not all(isinstance(c, str) for c in cols_list):
            return False
        if isinstance(ascending, bool):
            return True
        if isinstance(ascending, (list, tuple)):
            return len(ascending) == len(cols_list) and all(
                isinstance(a, bool) for a in ascending
            )
        return False

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
            if isinstance(op_payload, (list, tuple)):
                return all(isinstance(c, str) for c in op_payload)
            return False
        if op_name == "limit":
            return isinstance(op_payload, int) and op_payload >= 0
        if op_name == "orderBy":
            return self._can_handle_order_by(op_payload)
        if op_name == "withColumn":
            return self._can_handle_with_column(op_payload)
        if op_name == "join":
            return self._can_handle_join(op_payload)
        return op_name == "union"

    def can_handle_operations(
        self, operations: List[Tuple[str, Any]]
    ) -> Tuple[bool, List[str]]:
        unsupported: List[str] = []
        for op_name, op_payload in operations:
            if not self.can_handle_operation(op_name, op_payload):
                unsupported.append(op_name)
        return (len(unsupported) == 0, unsupported)

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
        names = [f.name for f in schema.fields]
        robin_schema = [
            (f.name, _spark_type_to_robin_dtype(f.dataType)) for f in schema.fields
        ]
        robin_data = _data_to_robin_rows(data, names)

        spark = F.SparkSession.builder().app_name("sparkless-robin-poc").get_or_create()
        df = spark.create_dataframe_from_rows(robin_data, robin_schema)

        for op_name, payload in operations:
            if op_name == "filter":
                expr = _simple_filter_to_robin(payload)
                if expr is not None:
                    df = df.filter(expr)
            elif op_name == "select":
                cols = (
                    list(payload) if isinstance(payload, (list, tuple)) else [payload]
                )
                df = df.select(cols)
            elif op_name == "limit":
                df = df.limit(payload)
            elif op_name == "orderBy":
                columns, ascending = (
                    payload[0],
                    payload[1] if len(payload) > 1 else True,
                )
                col_names = (
                    list(columns) if isinstance(columns, (list, tuple)) else [columns]
                )
                if isinstance(ascending, bool):
                    asc_list = [ascending] * len(col_names)
                else:
                    asc_list = list(ascending)
                df = df.order_by(col_names, asc_list)
            elif op_name == "withColumn":
                col_name, expression = payload[0], payload[1]
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
                other_robin_df = spark.create_dataframe_from_rows(
                    other_robin_data, other_robin_schema
                )
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
                # Some APIs expect a single string when one key
                on_param = on_arg[0] if len(on_arg) == 1 else on_arg
                df = df.join(other_robin_df, on=on_param, how=how_str)
            elif op_name == "union":
                other_df = payload
                other_eager = other_df._materialize_if_lazy()
                other_names = [f.name for f in other_eager.schema.fields]
                other_robin_schema = [
                    (f.name, _spark_type_to_robin_dtype(f.dataType))
                    for f in other_eager.schema.fields
                ]
                other_robin_data = _data_to_robin_rows(other_eager.data, other_names)
                other_robin_df = spark.create_dataframe_from_rows(
                    other_robin_data, other_robin_schema
                )
                df = df.union(other_robin_df)

        collected = df.collect()
        # Use projected schema so column order and duplicate names (e.g. after join) are correct
        from sparkless.dataframe.schema.schema_manager import SchemaManager

        final_schema = SchemaManager.project_schema_with_operations(schema, operations)
        return [Row(d, schema=final_schema) for d in collected]

    def close(self) -> None:
        """Release resources. robin_sparkless SparkSession does not expose stop();
        the session is process-scoped and cleaned up on process exit."""
        pass
