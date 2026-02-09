"""Tests for pure Robin mode: no fallback to Polars when Robin cannot handle an operation."""

from __future__ import annotations

import pytest

from sparkless.backend.factory import BackendFactory


@pytest.mark.unit
class TestRobinFallbackToPolars:
    """Pure Robin mode: unsupported operations raise; no Polars fallback."""

    def teardown_method(self) -> None:
        BackendFactory._robin_available_cache = None

    def test_unsupported_select_expression_raises(self) -> None:
        """Robin backend raises when select(regexp_extract_all(...)) is used (no fallback)."""
        if not BackendFactory._robin_available():
            pytest.skip("robin-sparkless not installed")
        from sparkless import SparkSession
        from sparkless.core.exceptions.operation import SparkUnsupportedOperationError
        from sparkless.sql import functions as F

        spark = SparkSession("test-robin-pure", backend_type="robin")
        try:
            df = spark.createDataFrame([{"s": "a1 b22 c333"}])
            with pytest.raises(SparkUnsupportedOperationError):
                df.select(
                    F.regexp_extract_all("s", r"\d+", 0).alias("m")
                ).collect()
        finally:
            spark.stop()

    def test_unsupported_raises_with_clear_message(self) -> None:
        """Robin backend raises SparkUnsupportedOperationError with helpful message."""
        if not BackendFactory._robin_available():
            pytest.skip("robin-sparkless not installed")
        from sparkless import SparkSession
        from sparkless.core.exceptions.operation import SparkUnsupportedOperationError
        from sparkless.sql import functions as F

        spark = SparkSession("test-robin-raise", backend_type="robin")
        try:
            df = spark.createDataFrame([{"s": "a1 b2"}])
            with pytest.raises(SparkUnsupportedOperationError) as exc_info:
                df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m")).collect()
            assert "does not support" in str(exc_info.value)
        finally:
            spark.stop()
