"""
Backend factory for creating backend instances.

In v4 Sparkless re-exports Robin via sparkless.sql. This factory only provides
create_storage_backend for code that needs file-based catalog/table storage.
"""

import importlib.util
from typing import Any, List, Optional, cast
from .protocols import StorageBackend


class BackendFactory:
    """Factory for creating backend instances. Only storage is supported."""

    _robin_available_cache: Optional[bool] = None

    @staticmethod
    def create_storage_backend(
        backend_type: str = "robin",
        db_path: Optional[str] = None,
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        **kwargs: Any,
    ) -> StorageBackend:
        """Create a storage backend instance.

        Args:
            backend_type: Must be "robin" (v4 only supports Robin).
            db_path: Optional database/persistence path for the Robin backend.
            max_memory: Kept for API compatibility; ignored.
            allow_disk_spillover: Kept for API compatibility; ignored.
            **kwargs: Additional backend-specific arguments

        Returns:
            Storage backend instance

        Raises:
            ValueError: If backend_type is not "robin"
        """
        if backend_type != "robin":
            raise ValueError(
                f"Unsupported backend type: {backend_type}. "
                "Sparkless v4 supports only the Robin backend (robin-sparkless)."
            )
        if not BackendFactory._robin_available():
            raise ValueError(
                "Robin backend is not available. Install with: pip install robin-sparkless."
            )
        from .robin.storage import RobinStorageManager

        return cast("StorageBackend", RobinStorageManager(db_path=db_path))

    @staticmethod
    def get_backend_type(storage: StorageBackend) -> str:
        """Detect backend type from storage instance."""
        module_name = type(storage).__module__
        if "robin" in module_name:
            return "robin"
        if "memory" in module_name:
            return "memory"
        if "file" in module_name:
            return "file"
        class_name = type(storage).__name__.lower()
        if "robin" in class_name:
            return "robin"
        if "memory" in class_name:
            return "memory"
        if "file" in class_name:
            return "file"
        return "robin"  # default

    @staticmethod
    def list_available_backends() -> List[str]:
        """List all available backend types. In v4 only Robin is supported."""
        return ["robin"]

    @staticmethod
    def validate_backend_type(backend_type: str) -> None:
        """Validate that a backend type is supported."""
        available_backends = BackendFactory.list_available_backends()
        if backend_type not in available_backends:
            raise ValueError(
                f"Unsupported backend type: {backend_type}. "
                f"Available backends: {', '.join(available_backends)}"
            )

    @staticmethod
    def _robin_available() -> bool:
        """Check whether Robin (robin-sparkless) is available."""
        if BackendFactory._robin_available_cache is not None:
            return BackendFactory._robin_available_cache
        try:
            spec = importlib.util.find_spec("robin_sparkless")
        except ModuleNotFoundError:
            BackendFactory._robin_available_cache = False
        else:
            BackendFactory._robin_available_cache = spec is not None
        return BackendFactory._robin_available_cache
