"""Tests for Robin backend: unsupported operations raise (no fallback)."""

from __future__ import annotations

import pytest

from sparkless.backend.factory import BackendFactory
from sparkless.core.exceptions.operation import SparkUnsupportedOperationError

from tests import robin_helpers


@pytest.mark.unit
class TestRobinUnsupportedRaises:
    """Robin backend: unsupported operations raise."""

    def teardown_method(self) -> None:
        BackendFactory._robin_available_cache = None

    def test_unsupported_select_expression_raises(self) -> None:
        """Robin backend raises when select(regexp_extract_all(...)) is used."""
        with pytest.raises(SparkUnsupportedOperationError):
            robin_helpers.run_unsupported_regexp_extract_all_select(
                app_name="test-robin-pure"
            )

    def test_unsupported_raises_with_clear_message(self) -> None:
        """Robin backend raises SparkUnsupportedOperationError with helpful message."""
        msg = robin_helpers.run_unsupported_regexp_extract_all_select_with_message_check(
            app_name="test-robin-raise"
        )
        assert "does not support" in msg
