"""
Minimal Robin (robin-sparkless) materializer proof-of-concept.

Implements DataMaterializer for a tiny subset of operations: filter (simple
ColumnOperation only), select (column name list), limit. Validates the pipeline:
Sparkless -> robin_sparkless session -> create_dataframe -> apply ops -> collect -> Row list.

Schema: PoC only supports 3-column schema; data rows are converted to (i64, i64, str)
for robin_sparkless.create_dataframe. Other operations and schemas fall back to
can_handle_operations -> False so the Polars backend is used instead.
"""

from __future__ import annotations

from typing import Any, List, Tuple

from sparkless.spark_types import Row, StructType

# Optional import; materializer is only used when backend_type="robin" and robin_sparkless is installed
try:
    import robin_sparkless  # noqa: F401
except ImportError:
    robin_sparkless = None  # type: ignore[assignment]


def _robin_available() -> bool:
    return robin_sparkless is not None


def _simple_filter_to_robin(condition: Any) -> Any:
    """Translate a simple Sparkless filter to robin_sparkless Column expression.

    Only supports: ColumnOperation with op in (>, <, >=, <=, ==, !=),
    column = Column(name), value = scalar or Literal.
    Returns None if not supported.
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
        robin_lit = F.lit(val)
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


class RobinMaterializer:
    """Proof-of-concept DataMaterializer using robin_sparkless.

    Supports only: filter (simple col op literal), select (list of str), limit.
    Requires exactly 3 columns in schema for create_dataframe compatibility.
    """

    SUPPORTED_OPERATIONS = {"filter", "select", "limit"}

    def __init__(self) -> None:
        pass

    def _can_handle_filter(self, payload: Any) -> bool:
        return _simple_filter_to_robin(payload) is not None

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
        return False

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
        # PoC: only 3-column schema
        if not schema.fields or len(schema.fields) != 3:
            raise ValueError(
                "RobinMaterializer PoC only supports schema with exactly 3 columns"
            )
        names = [f.name for f in schema.fields]
        # Convert data to list of (i64, i64, str)
        rows_tuples: List[Tuple[int, int, str]] = []
        for row in data:
            if isinstance(row, dict):
                v0 = row.get(names[0])
                v1 = row.get(names[1])
                v2 = row.get(names[2])
            elif isinstance(row, (list, tuple)):
                v0 = row[0] if len(row) > 0 else None
                v1 = row[1] if len(row) > 1 else None
                v2 = row[2] if len(row) > 2 else None
            else:
                v0 = getattr(row, names[0], None)
                v1 = getattr(row, names[1], None)
                v2 = getattr(row, names[2], None)
            # Coerce to (int, int, str)
            try:
                i0 = int(v0) if v0 is not None else 0
                i1 = int(v1) if v1 is not None else 0
                s2 = str(v2) if v2 is not None else ""
            except (TypeError, ValueError):
                i0, i1, s2 = 0, 0, ""
            rows_tuples.append((i0, i1, s2))

        spark = F.SparkSession.builder().app_name("sparkless-robin-poc").get_or_create()
        df = spark.create_dataframe(rows_tuples, names)

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

        collected = df.collect()
        return [Row(d, schema=None) for d in collected]

    def close(self) -> None:
        pass
