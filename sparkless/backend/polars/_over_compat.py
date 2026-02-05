"""Compatibility helpers for Polars Expr.over() across versions.

Polars added the `descending` parameter to Expr.over() in 1.22 (see
https://github.com/pola-rs/polars/issues/17674). Sparkless supports polars>=0.20.0,
so we must only pass descending when the installed Polars supports it.
"""

from __future__ import annotations

import inspect
from typing import List

import polars as pl

_over_supports_descending: bool | None = None


def polars_over_supports_descending() -> bool:
    """Return True if the installed Polars Expr.over() accepts the descending parameter."""
    global _over_supports_descending
    if _over_supports_descending is None:
        try:
            sig = inspect.signature(pl.col("x").over)
            _over_supports_descending = "descending" in sig.parameters
        except Exception:
            _over_supports_descending = False
    return _over_supports_descending


def to_descending_bool(descending: List[bool]) -> bool:
    """Convert a per-column descending list to a single bool for Polars over().

    Polars over() expects descending: bool, not List[bool]. When we have multiple
    order columns with mixed directions, we use True if any is descending.
    """
    if not descending:
        return False
    if len(descending) == 1:
        return descending[0]
    return any(descending)
