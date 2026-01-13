"""
Session builder implementation for Sparkless.

This module provides the SparkSessionBuilder class for creating
SparkSession instances using the builder pattern, maintaining compatibility
with PySpark's SparkSession.builder interface.
"""

from typing import Any, Dict, Union

from sparkless.config import resolve_backend_type

from .session import SparkSession


class SparkSessionBuilder:
    """SparklessSession builder."""

    def __init__(self) -> None:
        """Initialize builder."""
        self._app_name = "SparklessApp"
        self._config: Dict[str, Any] = {}

    def appName(self, name: str) -> "SparkSessionBuilder":
        """Set app name.

        Args:
            name: Application name.

        Returns:
            Self for method chaining.
        """
        self._app_name = name
        return self

    def master(self, master: str) -> "SparkSessionBuilder":
        """Set master URL.

        Args:
            master: Master URL.

        Returns:
            Self for method chaining.
        """
        return self

    def config(
        self, key_or_pairs: Union[str, Dict[str, Any]], value: Any = None
    ) -> "SparkSessionBuilder":
        """Set configuration.

        Args:
            key_or_pairs: Configuration key or dictionary of key-value pairs.
            value: Configuration value (if key_or_pairs is a string).

        Returns:
            Self for method chaining.
        """
        if isinstance(key_or_pairs, str):
            self._config[key_or_pairs] = value
        else:
            self._config.update(key_or_pairs)
        return self

    def getOrCreate(self) -> SparkSession:
        """Get or create session.

        Returns:
            SparkSession instance.
        """
        # Return existing singleton if present; otherwise create and cache
        if SparkSession._singleton_session is None:
            # Extract backend configuration
            backend_override = self._config.get("spark.sparkless.backend")
            backend_type = resolve_backend_type(backend_override)
            max_memory = self._config.get("spark.sparkless.backend.maxMemory", "1GB")
            allow_disk_spillover = self._config.get(
                "spark.sparkless.backend.allowDiskSpillover", False
            )

            session = SparkSession(
                self._app_name,
                backend_type=backend_type,
                max_memory=max_memory,
                allow_disk_spillover=allow_disk_spillover,
            )
            for key, value in self._config.items():
                session.conf.set(key, value)
            SparkSession._singleton_session = session
        return SparkSession._singleton_session
