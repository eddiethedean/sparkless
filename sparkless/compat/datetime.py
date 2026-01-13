"""Datetime compatibility helpers for Sparkless.
from __future__ import annotations

These helpers normalise date and timestamp outputs when running on the
Polars-backed Sparkless engine, while acting as no-ops under real PySpark.
"""

from datetime import date, datetime
import importlib.util
import os
from typing import (
    Any,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Type,
    Union,
    cast,
)

from sparkless.functions import Functions as F
from sparkless.functions.base import Column, ColumnOperation
from sparkless.spark_types import Row

if TYPE_CHECKING:
    from pyspark.sql.column import Column as PySparkColumnType
else:  # pragma: no cover - PySpark not available in Sparkless env
    PySparkColumnType = Any

PySparkColumn: Optional[Type[PySparkColumnType]]

_ALLOW_PYSPARK_IMPORT = os.environ.get("MOCK_SPARK_ENABLE_PYSPARK_SHIMS", "0") == "1"
if _ALLOW_PYSPARK_IMPORT and importlib.util.find_spec("pyspark.sql.column") is not None:
    try:  # pragma: no cover - optional dependency
        from pyspark.sql.column import Column as _RuntimePySparkColumn
    except Exception:  # pragma: no cover - PySpark not available or broken
        PySparkColumn = None
    else:
        PySparkColumn = cast(
            "Optional[Type[PySparkColumnType]]",
            _RuntimePySparkColumn,
        )
else:
    PySparkColumn = None

ColumnLike = Union[str, Column, ColumnOperation]
RowLike = Union[Row, Mapping[str, object]]


def _is_pyspark_column(column: object) -> bool:
    if PySparkColumn is None:
        return False
    return isinstance(column, PySparkColumn)


def _ensure_to_date_operation(column: ColumnLike) -> ColumnOperation:
    if isinstance(column, ColumnOperation):
        if getattr(column, "operation", None) == "to_date":
            return column
        raise TypeError(
            "Expected a to_date ColumnOperation when passing ColumnOperation to to_date_str"
        )
    return F.to_date(column)


def _ensure_to_timestamp_operation(
    column: ColumnLike, fmt: Optional[str]
) -> ColumnOperation:
    if isinstance(column, ColumnOperation):
        if getattr(column, "operation", None) == "to_timestamp":
            return column
        raise TypeError(
            "Expected a to_timestamp ColumnOperation when passing ColumnOperation to to_timestamp_str"
        )
    if fmt is None:
        return F.to_timestamp(column)
    return F.to_timestamp(column, fmt)


def to_date_str(
    column: ColumnLike, fmt: str = "yyyy-MM-dd"
) -> Union[ColumnOperation, ColumnLike]:
    """Ensure ``to_date`` results are represented as ISO-8601 strings.

    When running under PySpark, the function returns the original column to avoid
    behavioural drift. Under Sparkless, the column is formatted using
    ``date_format`` while preserving the original alias.
    """

    # Type narrowing: check if column is PySpark column at runtime
    # Mypy thinks this is unreachable, but it can be true at runtime when PySpark is available
    if PySparkColumn is not None and isinstance(column, PySparkColumn):
        # PySparkColumn is part of ColumnLike union, so this is valid
        return cast("Union[ColumnOperation, ColumnLike]", column)

    to_date_op = _ensure_to_date_operation(column)
    formatted = F.date_format(cast("Column", to_date_op), fmt)
    return formatted.alias(to_date_op.name)


def to_timestamp_str(
    column: ColumnLike,
    fmt: str = "yyyy-MM-dd HH:mm:ss",
    source_format: Optional[str] = None,
) -> Union[ColumnOperation, ColumnLike]:
    """Format ``to_timestamp`` outputs as strings when using Sparkless."""

    # Type narrowing: check if column is PySpark column at runtime
    # Mypy thinks this is unreachable, but it can be true at runtime when PySpark is available
    if PySparkColumn is not None and isinstance(column, PySparkColumn):
        # PySparkColumn is part of ColumnLike union, so this is valid
        return cast("Union[ColumnOperation, ColumnLike]", column)

    to_timestamp_op = _ensure_to_timestamp_operation(column, source_format)
    formatted = F.date_format(cast("Column", to_timestamp_op), fmt)
    return formatted.alias(to_timestamp_op.name)


def normalize_date_value(value: object) -> Optional[Union[str, object]]:
    """Convert Python ``date`` values to ISO strings while leaving others intact."""

    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    # Return value as-is if not a date (could be str, int, etc.)
    return value


def normalize_timestamp_value(
    value: object, fmt: str = "%Y-%m-%d %H:%M:%S"
) -> Optional[Union[str, object]]:
    """Convert Python ``datetime`` values to formatted strings."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime(fmt)
    # Return value as-is if not a datetime (could be str, int, etc.)
    return value


def normalize_collected_datetimes(
    rows: Sequence[RowLike],
    *,
    date_columns: Optional[Iterable[str]] = None,
    timestamp_columns: Optional[Iterable[str]] = None,
    timestamp_format: str = "%Y-%m-%d %H:%M:%S",
) -> List[MutableMapping[str, object]]:
    """Normalise collected Row objects containing date/timestamp values.

    Returns a list of dictionaries to make downstream assertions (especially in
    snapshot-based tests) stable across engines.
    """

    date_cols = list(date_columns or [])
    ts_cols = list(timestamp_columns or [])

    normalized: List[MutableMapping[str, object]] = []

    for row in rows:
        if isinstance(row, Mapping):
            data: MutableMapping[str, object] = dict(row)
        else:
            data = cast("MutableMapping[str, object]", row.asDict())

        for col in date_cols:
            if col in data:
                data[col] = normalize_date_value(data.get(col))
        for col in ts_cols:
            if col in data:
                data[col] = normalize_timestamp_value(data.get(col), timestamp_format)

        normalized.append(data)

    return normalized
