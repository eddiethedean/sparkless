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
@pytest.mark.xfail(
    reason=(
        "Legacy expectation that Robin raises SparkUnsupportedOperationError for "
        "regexp_extract_all select expressions. In v4, behavior is delegated to the "
        "robin-sparkless crate and tracked as a PySpark parity gap upstream."
    ),
    strict=False,
)
class TestRobinUnsupportedRaises:
    """Robin backend: legacy unsupported-operation expectations (tracked as parity gaps in v4)."""

    def teardown_method(self) -> None:
        BackendFactory._robin_available_cache = None

    @no_type_check
    def test_unsupported_select_expression_raises(self) -> None:
        """Robin backend raises when select(regexp_extract_all(...)) is used."""
        spark: SparkSession = SparkSession("test-robin-pure", backend_type="robin")
        try:
            df: DataFrame = spark.createDataFrame([{"s": "a1 b22 c333"}])
            selected = df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m"))
            with pytest.raises(SparkUnsupportedOperationError):
                _trigger_collect_untyped(selected)
        finally:
            spark.stop()

    @no_type_check
    def test_unsupported_raises_with_clear_message(self) -> None:
        """Robin backend raises SparkUnsupportedOperationError with helpful message."""
        spark: SparkSession = SparkSession("test-robin-raise", backend_type="robin")
        try:
            df: DataFrame = spark.createDataFrame([{"s": "a1 b2"}])
            selected = df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m"))
            with pytest.raises(SparkUnsupportedOperationError) as exc_info:
                _trigger_collect_untyped(selected)
            assert "does not support" in str(exc_info.value)
        finally:
            spark.stop()
