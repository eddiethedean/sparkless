"""
Backend factory for creating backend instances.

This module provides a centralized factory for creating backend instances,
enabling dependency injection and easier testing.
"""

import importlib.util
from typing import Any, List, Optional, cast
from .protocols import StorageBackend, DataMaterializer, ExportBackend


class BackendFactory:
    """Factory for creating backend instances.

    This factory creates backend instances based on the requested type,
    allowing for easy swapping of implementations and testing with mocks.

    Example:
        >>> storage = BackendFactory.create_storage_backend("polars")
        >>> materializer = BackendFactory.create_materializer("polars")
    """

    _duckdb_available_cache: Optional[bool] = None

    @staticmethod
    def create_storage_backend(
        backend_type: str = "polars",
        db_path: Optional[str] = None,
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        **kwargs: Any,
    ) -> StorageBackend:
        """Create a storage backend instance.

        Args:
            backend_type: Type of backend ("polars", "memory", "file")
            db_path: Optional database file path
            max_memory: Maximum memory (ignored for Polars, kept for compatibility)
            allow_disk_spillover: Whether to allow disk spillover (ignored for Polars)
            **kwargs: Additional backend-specific arguments

        Returns:
            Storage backend instance

        Raises:
            ValueError: If backend_type is not supported
        """
        if backend_type == "polars":
            from .polars.storage import PolarsStorageManager

            return PolarsStorageManager(db_path=db_path)
        elif backend_type == "duckdb":
            if not BackendFactory._duckdb_available():
                raise ValueError(
                    "DuckDB backend is not available. Install optional DuckDB dependencies or "
                    "use the Polars backend."
                )

            from sparkless.backend.duckdb.storage import DuckDBStorageManager

            return cast(
                "StorageBackend",
                DuckDBStorageManager(
                    db_path=db_path,
                    max_memory=max_memory,
                    allow_disk_spillover=allow_disk_spillover,
                ),
            )
        elif backend_type == "memory":
            from sparkless.storage.backends.memory import MemoryStorageManager

            return MemoryStorageManager()
        elif backend_type == "file":
            from sparkless.storage.backends.file import FileStorageManager

            base_path = kwargs.get("base_path", "sparkless_storage")
            return FileStorageManager(base_path)
        else:
            raise ValueError(f"Unsupported backend type: {backend_type}")

    @staticmethod
    def create_materializer(
        backend_type: str = "polars",
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
        **kwargs: Any,
    ) -> DataMaterializer:
        """Create a data materializer instance.

        Args:
            backend_type: Type of materializer ("polars", "memory", "file")
            max_memory: Maximum memory (ignored for Polars, kept for compatibility)
            allow_disk_spillover: Whether to allow disk spillover (ignored for Polars)
            **kwargs: Additional materializer-specific arguments

        Returns:
            Data materializer instance

        Raises:
            ValueError: If backend_type is not supported
        """
        if backend_type == "polars":
            from .polars.materializer import PolarsMaterializer

            return PolarsMaterializer()
        elif backend_type == "duckdb":
            if not BackendFactory._duckdb_available():
                raise ValueError(
                    "DuckDB backend is not available. Install optional DuckDB dependencies or "
                    "use the Polars backend."
                )

            from sparkless.backend.duckdb.materializer import DuckDBMaterializer

            return cast("DataMaterializer", DuckDBMaterializer())
        elif backend_type == "memory":
            # For memory backend, use Polars materializer
            from .polars.materializer import PolarsMaterializer

            return PolarsMaterializer()
        elif backend_type == "file":
            # For file backend, use Polars materializer
            from .polars.materializer import PolarsMaterializer

            return PolarsMaterializer()
        else:
            raise ValueError(f"Unsupported materializer type: {backend_type}")

    @staticmethod
    def create_export_backend(backend_type: str = "polars") -> ExportBackend:
        """Create an export backend instance.

        Args:
            backend_type: Type of export backend ("polars", "memory", "file")

        Returns:
            Export backend instance

        Raises:
            ValueError: If backend_type is not supported
        """
        if backend_type == "polars":
            from .polars.export import PolarsExporter

            return PolarsExporter()
        elif backend_type == "duckdb":
            if not BackendFactory._duckdb_available():
                raise ValueError(
                    "DuckDB backend is not available. Install optional DuckDB dependencies or "
                    "use the Polars backend."
                )

            from sparkless.backend.duckdb.export import DuckDBExporter

            return cast("ExportBackend", DuckDBExporter())
        elif backend_type == "memory":
            # For memory backend, use Polars exporter
            from .polars.export import PolarsExporter

            return PolarsExporter()
        elif backend_type == "file":
            # For file backend, use Polars exporter
            from .polars.export import PolarsExporter

            return PolarsExporter()
        else:
            raise ValueError(f"Unsupported export backend type: {backend_type}")

    @staticmethod
    def get_backend_type(storage: StorageBackend) -> str:
        """Detect backend type from storage instance.

        Args:
            storage: Storage backend instance

        Returns:
            Backend type string ("polars", "memory", "file", etc.)

        Raises:
            ValueError: If backend type cannot be determined
        """
        # Use module path inspection to detect backend type
        module_name = type(storage).__module__

        if "polars" in module_name:
            return "polars"
        elif "memory" in module_name:
            return "memory"
        elif "file" in module_name:
            return "file"
        else:
            # Fallback: try to match class name
            class_name = type(storage).__name__.lower()
            if "polars" in class_name:
                return "polars"
            elif "memory" in class_name:
                return "memory"
            elif "file" in class_name:
                return "file"
            else:
                raise ValueError(f"Cannot determine backend type for {type(storage)}")

    @staticmethod
    def list_available_backends() -> List[str]:
        """List all available backend types.

        Returns:
            List of supported backend type strings
        """
        backends = ["polars", "memory", "file"]
        if BackendFactory._duckdb_available():
            backends.append("duckdb")
        return backends

    @staticmethod
    def validate_backend_type(backend_type: str) -> None:
        """Validate that a backend type is supported.

        Args:
            backend_type: Backend type to validate

        Raises:
            ValueError: If backend type is not supported
        """
        available_backends = BackendFactory.list_available_backends()
        if backend_type not in available_backends:
            raise ValueError(
                f"Unsupported backend type: {backend_type}. "
                f"Available backends: {', '.join(available_backends)}"
            )

    @staticmethod
    def _duckdb_available() -> bool:
        """Check whether the optional DuckDB backend is available."""

        if BackendFactory._duckdb_available_cache is not None:
            return BackendFactory._duckdb_available_cache

        try:
            spec = importlib.util.find_spec("sparkless.backend.duckdb.storage")
        except ModuleNotFoundError:
            BackendFactory._duckdb_available_cache = False
        else:
            BackendFactory._duckdb_available_cache = spec is not None

        return BackendFactory._duckdb_available_cache
