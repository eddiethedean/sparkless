"""
Test that all example scripts are runnable.

Ensures documentation examples work correctly and produce expected outputs.
"""

from __future__ import annotations

import os
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

import pytest
from importlib.metadata import PackageNotFoundError, version
from packaging.requirements import Requirement
from packaging.version import Version

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - fallback for older interpreters
    import tomli as tomllib  # type: ignore[no-redef,unused-ignore]  # noqa: F401


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
OPTIONAL_DEPENDENCIES_TO_VALIDATE = ("pandas", "pandas-stubs")


@lru_cache(maxsize=1)
def _optional_dependency_requirements() -> Dict[str, List[Requirement]]:
    """Parse optional dependency requirements from pyproject.toml."""
    data = tomllib.loads(PYPROJECT_PATH.read_text())
    optional = data.get("project", {}).get("optional-dependencies", {})

    requirements: Dict[str, List[Requirement]] = {}
    for entries in optional.values():
        for entry in entries:
            req = Requirement(entry)
            requirements.setdefault(req.name, []).append(req)
    return requirements


def _validate_optional_dependency(package: str) -> Optional[Version]:
    """Ensure optional dependency is installed and meets version specifiers."""
    specs = _optional_dependency_requirements().get(package, [])
    if not specs:
        return None

    try:
        installed_version = Version(version(package))
    except PackageNotFoundError:
        pytest.skip(
            f"Optional dependency '{package}' is not installed; "
            "install the 'sparkless[pandas]' extra to run documentation examples.",
            allow_module_level=True,
        )

    for requirement in specs:
        if requirement.specifier and installed_version not in requirement.specifier:
            pytest.fail(
                f"Optional dependency '{package}' has version {installed_version}, "
                f"which does not satisfy constraint '{requirement.specifier}'. "
                "Update the package to meet documentation harness expectations.",
                pytrace=False,
            )

    return installed_version


@pytest.fixture(scope="session", autouse=True)  # type: ignore[untyped-decorator]
def _ensure_documentation_dependencies() -> None:
    """Fail fast (or skip) if optional docs dependencies are missing or stale."""
    versions: Dict[str, Version] = {}
    for package in OPTIONAL_DEPENDENCIES_TO_VALIDATE:
        dep_version = _validate_optional_dependency(package)
        if dep_version is not None:
            versions[package] = dep_version

    pandas_version = versions.get("pandas")
    stubs_version = versions.get("pandas-stubs")
    if pandas_version and stubs_version and pandas_version.major != stubs_version.major:
        pytest.fail(
            "Detected mismatched major versions between 'pandas' "
            f"({pandas_version}) and 'pandas-stubs' ({stubs_version}). "
            "Update the stub package to align with the installed pandas version.",
            pytrace=False,
        )


class TestExampleScripts:
    """Validate that all example scripts run without errors."""

    @pytest.mark.skipif(
        os.environ.get("MOCK_SPARK_TEST_BACKEND") == "pyspark",
        reason="Skip documentation tests in PySpark mode (subprocess interference in parallel execution)",
    )
    @pytest.mark.skipif(
        os.environ.get("PYTEST_XDIST_WORKER") is not None,
        reason="Skip subprocess example tests under pytest-xdist (subprocess interference in parallel)",
    )
    def test_basic_usage_runs(self):
        """Test that basic_usage.py runs successfully."""
        env = os.environ.copy()
        existing_path = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = (
            f"{PROJECT_ROOT}:{existing_path}" if existing_path else str(PROJECT_ROOT)
        )
        env["MOCK_SPARK_EXAMPLES_FULL"] = "0"  # Force fast mode to avoid timeout
        result = subprocess.run(
            [sys.executable, "examples/basic_usage.py"],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=PROJECT_ROOT,
            env=env,
        )
        assert result.returncode == 0, f"basic_usage.py failed: {result.stderr}"
        # Header should mention the Sparkless basic usage example
        assert "Basic Usage Example" in result.stdout
        assert "Sparkless" in result.stdout

    @pytest.mark.skipif(
        os.environ.get("MOCK_SPARK_TEST_BACKEND") == "pyspark",
        reason="Skip documentation tests in PySpark mode (subprocess interference in parallel execution)",
    )
    @pytest.mark.skipif(
        os.environ.get("PYTEST_XDIST_WORKER") is not None,
        reason="Skip subprocess example tests under pytest-xdist (subprocess interference in parallel)",
    )
    def test_comprehensive_usage_runs(self):
        """Test that comprehensive_usage.py runs successfully."""
        env = os.environ.copy()
        existing_path = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = (
            f"{PROJECT_ROOT}:{existing_path}" if existing_path else str(PROJECT_ROOT)
        )
        result = subprocess.run(
            [sys.executable, "examples/comprehensive_usage.py"],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=PROJECT_ROOT,
            env=env,
        )
        assert result.returncode == 0, f"comprehensive_usage.py failed: {result.stderr}"
        assert "Comprehensive Feature Showcase" in result.stdout

    def test_examples_show_v2_features(self):
        """Test that examples mention v2.0.0 features."""
        examples_dir = PROJECT_ROOT / "examples"

        # Check basic_usage.py
        basic_content = (examples_dir / "basic_usage.py").read_text()
        assert "515 tests" in basic_content
        assert "2.0.0" in basic_content

        # Check comprehensive_usage.py
        comp_content = (examples_dir / "comprehensive_usage.py").read_text()
        assert "515 tests" in comp_content
        assert "2.0.0" in comp_content

    def test_example_outputs_captured(self):
        """Test that example outputs are saved."""
        outputs_dir = PROJECT_ROOT / "outputs"

        # Check that outputs directory exists and has files
        assert outputs_dir.exists(), "outputs/ directory should exist"
        assert (outputs_dir / "basic_usage_output.txt").exists()
        assert (outputs_dir / "comprehensive_output.txt").exists()

        # Verify outputs contain expected content
        basic_output = (outputs_dir / "basic_usage_output.txt").read_text()
        # Basic usage output should start with the Sparkless-branded header
        assert "Sparkless - Basic Usage Example" in basic_output
