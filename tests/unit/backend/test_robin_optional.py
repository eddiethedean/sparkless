"""Tests for Robin backend availability and error messages in v4.

In v4, Robin is the core engine; backend package was removed. Session always
uses MemoryStorageManager and Robin execution (no BackendFactory).
"""
import pytest

pytest.skip(
    "v4: backend package removed; session is always Robin-only",
    allow_module_level=True,
)


@pytest.mark.unit
class TestRobinBackendOptional:
    """Robin backend is always available; legacy factory paths behave per v4 design."""

    def teardown_method(self) -> None:
        """Clear robin availability cache so other tests get real availability."""
        BackendFactory._robin_available_cache = None

    def test_list_available_backends_includes_robin_v4(self) -> None:
        """list_available_backends() always includes 'robin' in v4."""
        backends = BackendFactory.list_available_backends()
        assert "robin" in backends

    def test_create_storage_backend_robin_uses_polars_storage_v4(self) -> None:
        """create_storage_backend('robin') returns a storage backend (Polars-based) in v4."""
        storage = BackendFactory.create_storage_backend("robin")
        # We don't assert exact type here to keep the test resilient; just ensure it behaves like a storage backend.
        assert storage is not None

    def test_create_materializer_robin_raises_with_v4_message(self) -> None:
        """create_materializer('robin') raises ValueError with v4 unified execution message."""
        with pytest.raises(
            ValueError,
            match="Robin materialization is no longer provided via BackendFactory; "
            "use the unified Robin execution path instead.",
        ):
            BackendFactory.create_materializer("robin")

    def test_create_export_backend_robin_raises_with_v4_message(self) -> None:
        """create_export_backend('robin') raises ValueError with v4 export message."""
        with pytest.raises(
            ValueError,
            match="Robin export backend is no longer provided; use Robin catalog/DataFrame APIs.",
        ):
            BackendFactory.create_export_backend("robin")
