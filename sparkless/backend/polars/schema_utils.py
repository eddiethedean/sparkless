"""Utilities for reconciling Polars DataFrames with Sparkless schemas."""

from __future__ import annotations

from typing import List, TYPE_CHECKING

import polars as pl

from .type_mapper import mock_type_to_polars_dtype

if TYPE_CHECKING:
    from sparkless.spark_types import StructType


def _empty_series(name: str, dtype: pl.DataType, length: int) -> pl.Series:
    """Create an empty or all-null Polars series with the desired dtype."""

    if length == 0:
        return pl.Series(name, [], dtype=dtype)
    return pl.Series(name, [None] * length, dtype=dtype)


def _cast_series(series: pl.Series, dtype: pl.DataType, length: int) -> pl.Series:
    """Cast a series to the target dtype, handling all-null columns gracefully."""

    if series.dtype == dtype:
        return series

    try:
        return series.cast(dtype)
    except Exception:
        # If the column is entirely null, replace it with a null series of the proper dtype.
        null_count = series.null_count()
        if null_count == length:
            return _empty_series(series.name, dtype, length)

        # Fall back to best-effort cast without strict enforcement.
        return series.cast(dtype, strict=False)


def align_frame_to_schema(frame: pl.DataFrame, schema: StructType) -> pl.DataFrame:
    """Return a DataFrame whose columns match the provided StructType order and dtypes.

    Args:
        frame: Polars DataFrame to align.
        schema: Target StructType definition.

    Returns:
        New Polars DataFrame with columns ordered and typed to match ``schema``.
    """

    if not schema.fields:
        return pl.DataFrame([])

    length = frame.height
    columns: List[pl.Series] = []

    for field in schema.fields:
        expected_dtype = mock_type_to_polars_dtype(field.dataType)

        if field.name in frame.columns:
            series = frame.get_column(field.name)
            aligned_series = _cast_series(series, expected_dtype, length)
        else:
            aligned_series = _empty_series(field.name, expected_dtype, length)

        columns.append(aligned_series)

    return pl.DataFrame(columns)
