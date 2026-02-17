"""Tests for Robin backend: unsupported operations raise (no fallback).

v4: Backend package removed; session is always Robin. These tests targeted
legacy BackendFactory and optional backend selection.
"""
from __future__ import annotations

import pytest

pytest.skip(
    "v4: backend package removed; unsupported-op behavior is Robin/crate-driven",
    allow_module_level=True,
)


# Rest of module skipped (allow_module_level=True above)
