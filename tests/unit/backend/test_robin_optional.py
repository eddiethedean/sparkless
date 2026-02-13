"""Tests for optional Robin (robin-sparkless) backend availability and error messages."""

from unittest.mock import patch

import pytest

from sparkless.backend.factory import BackendFactory


@pytest.mark.unit
class TestRobinBackendOptional:
    """When robin_sparkless is not installed, robin is excluded and create_* raise with install hint."""

    def teardown_method(self) -> None:
        """Clear robin availability cache so other tests get real availability."""
        BackendFactory._robin_available_cache = None

    def test_list_available_backends_returns_robin_only_v4(self) -> None:
        """In v4, list_available_backends() always returns ['robin']; only backend is Robin."""
        backends = BackendFactory.list_available_backends()
        assert backends == ["robin"]

    def test_create_storage_backend_robin_raises_when_not_available(self) -> None:
        """create_storage_backend('robin') raises ValueError with install hint when not available."""
        with patch.object(
            BackendFactory, "_robin_available", return_value=False
        ), pytest.raises(ValueError, match="sparkless\\[robin\\]|robin-sparkless"):
            BackendFactory.create_storage_backend("robin")
