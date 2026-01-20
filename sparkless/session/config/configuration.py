"""
Configuration management for Sparkless.

This module provides configuration management for Sparkless,
including session configuration, runtime settings, and
environment-specific configurations.

Key Features:
    - Complete PySpark SparkConf API compatibility
    - Configuration validation and type checking
    - Environment-specific settings
    - Configuration builder pattern
    - Runtime configuration updates

Example:
    >>> from sparkless.session.config import Configuration
    >>> conf = Configuration()
    >>> conf.set("spark.app.name", "MyApp")
    >>> conf.get("spark.app.name")
    'MyApp'
"""

from typing import Any, Dict, Optional
from dataclasses import dataclass


class Configuration:
    """SparklessConf for configuration management.

    Provides a comprehensive mock implementation of PySpark's SparkConf
    that supports all major operations including configuration management,
    validation, and environment-specific settings without requiring actual Spark.

    Attributes:
        _config: Internal configuration dictionary.

    Example:
        >>> conf = Configuration()
        >>> conf.set("spark.app.name", "MyApp")
        >>> conf.get("spark.app.name")
        'MyApp'
    """

    def __init__(self) -> None:
        """Initialize Configuration with default settings."""
        self._config = {
            "spark.app.name": "SparklessApp",
            "spark.master": "local[*]",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.caseSensitive": "false",  # Default to case-insensitive (matching PySpark)
        }

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get configuration value.

        Args:
            key: Configuration key.
            default: Default value if key not found.

        Returns:
            Configuration value or default.
        """
        return self._config.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set configuration value.

        Args:
            key: Configuration key.
            value: Configuration value.
        """
        self._config[key] = str(value)

    def setAll(self, pairs: Dict[str, Any]) -> None:
        """Set multiple configuration values.

        Args:
            pairs: Dictionary of key-value pairs.
        """
        for key, value in pairs.items():
            self.set(key, value)

    def setMaster(self, master: str) -> None:
        """Set master URL.

        Args:
            master: Master URL.
        """
        self.set("spark.master", master)

    def setAppName(self, name: str) -> None:
        """Set application name.

        Args:
            name: Application name.
        """
        self.set("spark.app.name", name)

    def getAll(self) -> Dict[str, str]:
        """Get all configuration values.

        Returns:
            Dictionary of all configuration values.
        """
        return self._config.copy()

    def unset(self, key: str) -> None:
        """Unset configuration value.

        Args:
            key: Configuration key to unset.
        """
        if key in self._config:
            del self._config[key]

    def contains(self, key: str) -> bool:
        """Check if configuration contains key.

        Args:
            key: Configuration key.

        Returns:
            True if key exists, False otherwise.
        """
        return key in self._config

    def is_case_sensitive(self) -> bool:
        """Check if case-sensitive identifier resolution is enabled.

        Returns:
            True if case-sensitive mode is enabled, False otherwise.
            Defaults to False (case-insensitive) to match PySpark behavior.
        """
        value = self.get("spark.sql.caseSensitive", "false")
        if value is None:
            return False
        return value.lower() in ("true", "1", "yes")

    def __str__(self) -> str:
        """String representation."""
        return f"Configuration({len(self._config)} settings)"

    def __repr__(self) -> str:
        """Representation."""
        return self.__str__()


@dataclass
class SparkConfig:
    """High-level session configuration for validation and behavior flags.

    This complements `Configuration` (SparkConf-like key/value) with
    strongly-typed knobs used by the mock engine.

    Attributes:
        validation_mode: Union[strict, relaxed] | minimal
        enable_type_coercion: best-effort coercion during DataFrame creation
    """

    validation_mode: str = "relaxed"
    enable_type_coercion: bool = True
    # Performance settings
    enable_lazy_evaluation: bool = True  # Changed default to True for lazy-by-default


class ConfigBuilder:
    """Configuration builder for Sparkless.

    Provides a builder pattern for creating Configuration instances
    with fluent API for setting multiple configuration values.

    Example:
        >>> builder = ConfigBuilder()
        >>> conf = (builder
        ...     .appName("MyApp")
        ...     .master("local[*]")
        ...     .set("spark.sql.adaptive.enabled", "true")
        ...     .build())
    """

    def __init__(self) -> None:
        """Initialize ConfigBuilder."""
        self._config = Configuration()

    def appName(self, name: str) -> "ConfigBuilder":
        """Set application name.

        Args:
            name: Application name.

        Returns:
            Self for method chaining.
        """
        self._config.setAppName(name)
        return self

    def master(self, master: str) -> "ConfigBuilder":
        """Set master URL.

        Args:
            master: Master URL.

        Returns:
            Self for method chaining.
        """
        self._config.setMaster(master)
        return self

    def set(self, key: str, value: Any) -> "ConfigBuilder":
        """Set configuration value.

        Args:
            key: Configuration key.
            value: Configuration value.

        Returns:
            Self for method chaining.
        """
        self._config.set(key, value)
        return self

    def setAll(self, pairs: Dict[str, Any]) -> "ConfigBuilder":
        """Set multiple configuration values.

        Args:
            pairs: Dictionary of key-value pairs.

        Returns:
            Self for method chaining.
        """
        self._config.setAll(pairs)
        return self

    def build(self) -> Configuration:
        """Build the configuration.

        Returns:
            Configuration instance.
        """
        return self._config
