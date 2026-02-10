"""Shared helpers for Robin backend tests."""

from __future__ import annotations

from typing import Any, List, Protocol


class _Collectible(Protocol):
    def collect(self) -> List[Any]: ...


def trigger_collect(df: _Collectible) -> None:
    """Call collect() to trigger materialization (used in tests that expect an error)."""
    df.collect()
