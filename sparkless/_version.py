"""
Version management for Sparkless.

This module provides a centralized way to get the package version,
preventing version drift across the codebase.
"""

try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    # Python < 3.8 compatibility
    from importlib_metadata import version, PackageNotFoundError  # type: ignore

try:
    __version__ = version("sparkless")
except PackageNotFoundError:
    # Fallback to hardcoded version if package not installed
    # This should match pyproject.toml
    __version__ = "3.27.0"
