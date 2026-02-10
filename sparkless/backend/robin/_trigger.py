"""Trigger materialization for Robin tests (separate module so mypy resolves callable)."""

from __future__ import annotations

from sparkless.dataframe.protocols import SupportsDataFrameOps


def trigger_collect(df: SupportsDataFrameOps) -> None:
    """Call collect() to trigger materialization (used in tests that expect an error)."""
    df.collect()
