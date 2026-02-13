"""Tests for Robin backend: unsupported operations raise (no fallback)."""

from __future__ import annotations

from typing import no_type_check

import pytest

from sparkless import DataFrame, SparkSession
from sparkless.backend.factory import BackendFactory
from sparkless.core.exceptions.operation import SparkUnsupportedOperationError
from sparkless.sql import functions as F

_HAS_ROBIN = BackendFactory._robin_available()  # type: ignore[attr-defined]


@no_type_check
def _trigger_collect_untyped(df: object) -> None:
    """Helper that calls collect(); body is intentionally untyped for mypy."""
    df.collect()  # type: ignore[attr-defined]


@pytest.mark.unit
@pytest.mark.skipif(
    not _HAS_ROBIN, reason="Robin backend requires robin-sparkless to be installed"
)
class TestRobinUnsupportedRaises:
    """Robin backend: unsupported operations raise."""

    def teardown_method(self) -> None:
        BackendFactory._robin_available_cache = None

    @no_type_check
    def test_unsupported_select_expression_raises(self) -> None:
        """Robin backend raises when select(regexp_extract_all(...)) is used."""
        spark: SparkSession = SparkSession("test-robin-pure", backend_type="robin")
        try:
            df: DataFrame = spark.createDataFrame([{"s": "a1 b22 c333"}])
            # Robin may raise at select() (TypeError) or at collect() (SparkUnsupportedOperationError)
            with pytest.raises((SparkUnsupportedOperationError, TypeError)):
                selected = df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m"))
                _trigger_collect_untyped(selected)
        finally:
            spark.stop()

    @no_type_check
    def test_unsupported_raises_with_clear_message(self) -> None:
        """Robin backend raises SparkUnsupportedOperationError or TypeError for unsupported select."""
        spark: SparkSession = SparkSession("test-robin-raise", backend_type="robin")
        try:
            df: DataFrame = spark.createDataFrame([{"s": "a1 b2"}])
            with pytest.raises((SparkUnsupportedOperationError, TypeError)) as exc_info:
                selected = df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m"))
                _trigger_collect_untyped(selected)
            msg = str(exc_info.value)
            # Either Sparkless unsupported message or Robin type/API error
            assert "does not support" in msg or "Column" in msg or "cannot be converted" in msg
        finally:
            spark.stop()
