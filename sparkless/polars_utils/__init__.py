"""Polars utilities removed in v4 (Robin-only); reader/writer use stdlib and optional pyarrow."""

from __future__ import annotations


def __getattr__(name: str):  # type: ignore[no-redef]
    raise ImportError(
        "sparkless.polars_utils was removed in v4. "
        "Reader/writer use stdlib (csv, json) and optional pyarrow for Parquet. "
        f"Cannot import '{name}'."
    )


__all__ = []
