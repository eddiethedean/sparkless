#!/usr/bin/env python3
"""
Discover PySpark API availability across versions 3.0-3.5.

This script installs each PySpark version sequentially and catalogs:
- All callables in pyspark.sql.functions module
- All public methods on pyspark.sql.DataFrame class

Outputs:
- pyspark_api_matrix.json - Machine-readable data
- PYSPARK_FUNCTION_MATRIX.md - Human-readable markdown table
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

# PySpark versions to test
PYSPARK_VERSIONS = ["3.0.3", "3.1.3", "3.2.4", "3.3.4", "3.4.3", "3.5.2"]

# Python built-in objects to exclude (not PySpark-specific)
PYTHON_BUILTINS_TO_EXCLUDE = {
    # typing module objects
    "Any",
    "Callable",
    "Dict",
    "Iterable",
    "List",
    "Optional",
    "Tuple",
    "Type",
    "Union",
    # collections.abc objects
    "ValuesView",
    # Python 2 compatibility
    "basestring",
}


def discover_api(spark_version: str) -> Dict[str, List[str]]:
    """
    Install PySpark version and catalog functions, classes/types, and DataFrame methods.

    Args:
        spark_version: PySpark version to install (e.g., "3.2.4")

    Returns:
        Dictionary with 'functions', 'classes_types', and 'dataframe_methods' lists
    """
    print(f"\n{'=' * 80}")
    print(f"Discovering PySpark {spark_version} API...")
    print(f"{'=' * 80}")

    # Install specific PySpark version
    print(f"Installing pyspark=={spark_version}...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", f"pyspark=={spark_version}"],
        check=True,
    )

    # Force reimport by removing from sys.modules
    modules_to_remove = [m for m in sys.modules if m.startswith("pyspark")]
    for module in modules_to_remove:
        del sys.modules[module]

    # Import fresh
    import pyspark.sql.functions as F
    from pyspark.sql import DataFrame

    # Discover functions and classes/types separately
    functions = []
    classes_types = []

    for name in dir(F):
        if name.startswith("_"):
            continue
        # Skip Python built-in objects (not PySpark-specific)
        if name in PYTHON_BUILTINS_TO_EXCLUDE:
            continue
        attr = getattr(F, name, None)
        if attr is None:
            continue

        # Check if it's a class/type
        if isinstance(attr, type):
            classes_types.append(name)
        # Otherwise, if it's callable, it's a function
        elif callable(attr):
            functions.append(name)

    functions = sorted(functions)
    classes_types = sorted(classes_types)

    # Discover DataFrame methods
    df_methods = sorted(
        [
            name
            for name in dir(DataFrame)
            if callable(getattr(DataFrame, name, None)) and not name.startswith("_")
        ]
    )

    print(f"Found {len(functions)} functions in pyspark.sql.functions")
    print(f"Found {len(classes_types)} classes/types in pyspark.sql.functions")
    print(f"Found {len(df_methods)} public methods on DataFrame")

    return {
        "functions": functions,
        "classes_types": classes_types,
        "dataframe_methods": df_methods,
    }


def build_matrix(
    versions_data: Dict[str, Dict[str, List[str]]],
) -> Dict[str, Dict[str, Dict[str, bool]]]:
    """
    Build a matrix showing which items exist in which versions.

    Args:
        versions_data: Dict mapping version -> {functions: [...], classes_types: [...], dataframe_methods: [...]}

    Returns:
        Dict with 'functions', 'classes_types', and 'dataframe_methods' matrices
    """
    # Collect all unique items across all versions
    all_functions: set[str] = set()
    all_classes_types: set[str] = set()
    all_df_methods: set[str] = set()

    for data in versions_data.values():
        all_functions.update(data["functions"])
        all_classes_types.update(data["classes_types"])
        all_df_methods.update(data["dataframe_methods"])

    # Build matrices
    function_matrix = {}
    for func in sorted(all_functions):
        function_matrix[func] = {
            version: func in data["functions"]
            for version, data in versions_data.items()
        }

    classes_types_matrix = {}
    for class_type in sorted(all_classes_types):
        classes_types_matrix[class_type] = {
            version: class_type in data["classes_types"]
            for version, data in versions_data.items()
        }

    df_method_matrix = {}
    for method in sorted(all_df_methods):
        df_method_matrix[method] = {
            version: method in data["dataframe_methods"]
            for version, data in versions_data.items()
        }

    return {
        "functions": function_matrix,
        "classes_types": classes_types_matrix,
        "dataframe_methods": df_method_matrix,
    }


def check_mock_spark_availability(item_name: str, item_type: str) -> bool:
    """
    Check if a function, class/type, or method is available in sparkless.

    Args:
        item_name: Name of function, class/type, or method
        item_type: Either 'function', 'class_type', or 'dataframe_method'

    Returns:
        True if available in sparkless
    """
    try:
        if item_type == "function":
            import sparkless.functions as F

            return hasattr(F, item_name)
        elif item_type == "class_type":
            # Check multiple locations where classes/types might be
            # 1. sparkless (main package - for SparkContext, etc.)
            try:
                import sparkless

                if hasattr(sparkless, item_name):
                    attr = getattr(sparkless, item_name)
                    if isinstance(attr, type):
                        return True
            except Exception:
                pass

            # 2. sparkless.sql (for DataFrame, Column, etc.)
            try:
                import sparkless.sql as sql

                if hasattr(sql, item_name):
                    return True
            except Exception:
                pass

            # 3. sparkless.functions (for Column, etc.)
            try:
                import sparkless.functions as F

                if hasattr(F, item_name):
                    attr = getattr(F, item_name)
                    # Make sure it's actually a class/type
                    if isinstance(attr, type):
                        return True
            except Exception:
                pass

            # 4. sparkless.spark_types (for StringType, ArrayType, etc.)
            try:
                import sparkless.spark_types as types

                if hasattr(types, item_name):
                    return True
            except Exception:
                pass

            # 5. sparkless.sql.types (alternative location)
            try:
                from sparkless.sql import types as sql_types

                if hasattr(sql_types, item_name):
                    return True
            except Exception:
                pass

            return False
        elif item_type == "dataframe_method":
            from sparkless.dataframe.dataframe import DataFrame

            return hasattr(DataFrame, item_name)
    except Exception:
        return False

    return False


def save_json(matrix: Dict[str, Any], output_path: Path) -> None:
    """Save matrix as JSON."""
    print(f"\nSaving JSON to {output_path}...")
    with open(output_path, "w") as f:
        json.dump(matrix, f, indent=2, default=str)
    print(f"✓ Saved {output_path}")


def save_markdown(
    matrix: Dict[str, Any], output_path: Path, versions: List[str]
) -> None:
    """Save matrix as markdown table."""
    print(f"\nGenerating markdown table at {output_path}...")

    lines = [
        "# PySpark Function & Method Availability Matrix",
        "",
        "**Generated by:** `scripts/discover_pyspark_api.py`",
        f"**PySpark Versions Tested:** {', '.join(versions)}",
        "",
        "This matrix shows which functions and DataFrame methods are available in each PySpark version,",
        "and whether they are implemented in sparkless.",
        "",
        "**Note:** All sparkless features are available to everyone - there is no version gating.",
        "This matrix is provided for reference to understand PySpark version compatibility.",
        "",
        "## Legend",
        "",
        "- ✅ = Available in PySpark version (or implemented in sparkless)",
        "- ⚠️ = Deprecated in PySpark (available but modern alternative recommended)",
        "- ❌ = Not available in PySpark version (or not implemented in sparkless)",
        "",
        "**Sparkless Status:** All implemented features (marked with ✅) are available to all users regardless of PySpark version.",
        "",
    ]

    # Classes and Types table
    if "classes_types" in matrix and matrix["classes_types"]:
        lines.extend(
            [
                "## Classes and Types (pyspark.sql.functions)",
                "",
                f"Total classes/types cataloged: {len(matrix['classes_types'])}",
                "",
            ]
        )

        # Table header
        header = "| Class/Type | " + " | ".join(versions) + " | Sparkless |"
        separator = (
            "|"
            + "|".join(
                ["-" * (len(v) + 2) for v in ["Class/Type"] + versions + ["Sparkless"]]
            )
            + "|"
        )
        lines.extend([header, separator])

        # Table rows
        for class_type_name, availability in sorted(matrix["classes_types"].items()):
            mock_available = check_mock_spark_availability(
                class_type_name, "class_type"
            )
            row = f"| `{class_type_name}` |"
            for version in versions:
                row += " ✅ |" if availability[version] else " ❌ |"
            row += " ✅ |" if mock_available else " ❌ |"
            lines.append(row)

        lines.append("")  # Empty line after classes section

    # Functions table
    lines.extend(
        [
            "## Functions (pyspark.sql.functions)",
            "",
            f"Total functions cataloged: {len(matrix['functions'])}",
            "",
        ]
    )

    # Table header
    header = "| Function | " + " | ".join(versions) + " | Sparkless |"
    separator = (
        "|"
        + "|".join(
            ["-" * (len(v) + 2) for v in ["Function"] + versions + ["Sparkless"]]
        )
        + "|"
    )
    lines.extend([header, separator])

    # Table rows
    for func_name, availability in sorted(matrix["functions"].items()):
        mock_available = check_mock_spark_availability(func_name, "function")
        row = f"| `{func_name}` |"
        for version in versions:
            row += " ✅ |" if availability[version] else " ❌ |"
        row += " ✅ |" if mock_available else " ❌ |"
        lines.append(row)

    # DataFrame methods table
    lines.extend(
        [
            "",
            "## DataFrame Methods",
            "",
            f"Total methods cataloged: {len(matrix['dataframe_methods'])}",
            "",
        ]
    )

    # Table header
    header = "| Method | " + " | ".join(versions) + " | Sparkless |"
    separator = (
        "|"
        + "|".join(["-" * (len(v) + 2) for v in ["Method"] + versions + ["Sparkless"]])
        + "|"
    )
    lines.extend([header, separator])

    # Table rows
    for method_name, availability in sorted(matrix["dataframe_methods"].items()):
        mock_available = check_mock_spark_availability(method_name, "dataframe_method")
        row = f"| `{method_name}` |"
        for version in versions:
            row += " ✅ |" if availability[version] else " ❌ |"
        row += " ✅ |" if mock_available else " ❌ |"
        lines.append(row)

    # Summary statistics
    lines.extend(
        [
            "",
            "## Summary Statistics",
            "",
        ]
    )

    # Count functions per version (excluding classes/types)
    for version in versions:
        func_count = sum(1 for avail in matrix["functions"].values() if avail[version])
        method_count = sum(
            1 for avail in matrix["dataframe_methods"].values() if avail[version]
        )
        lines.append(
            f"- **PySpark {version}**: {func_count} functions, {method_count} DataFrame methods"
        )

    # Sparkless coverage
    mock_func_count = sum(
        1
        for func in matrix["functions"]
        if check_mock_spark_availability(func, "function")
    )
    mock_method_count = sum(
        1
        for method in matrix["dataframe_methods"]
        if check_mock_spark_availability(method, "dataframe_method")
    )
    lines.append(
        f"- **Sparkless**: {mock_func_count} functions, {mock_method_count} DataFrame methods"
    )

    lines.append("")
    lines.append(
        "**All sparkless features are available to everyone** - there is no version gating or compatibility restrictions."
    )
    lines.append(
        "This provides comprehensive PySpark compatibility across all supported versions!"
    )

    # Write file
    with open(output_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"✓ Saved {output_path}")


def main() -> None:
    """Main discovery process."""
    print("=" * 80)
    print("PySpark API Discovery Tool")
    print("=" * 80)

    # Discover API for each version
    versions_data = {}
    for version in PYSPARK_VERSIONS:
        versions_data[version] = discover_api(version)

    # Build matrix
    print(f"\n{'=' * 80}")
    print("Building API matrix...")
    print(f"{'=' * 80}")
    matrix = build_matrix(versions_data)

    # Save outputs
    repo_root = Path(__file__).parent.parent
    json_path = repo_root / "sparkless" / "pyspark_api_matrix.json"
    md_path = repo_root / "PYSPARK_FUNCTION_MATRIX.md"

    save_json(matrix, json_path)
    save_markdown(matrix, md_path, PYSPARK_VERSIONS)

    print(f"\n{'=' * 80}")
    print("✓ Discovery complete!")
    print(f"{'=' * 80}")
    print("\nGenerated files:")
    print(f"  - {json_path}")
    print(f"  - {md_path}")
    print("\nTotal items discovered:")
    if "classes_types" in matrix:
        print(f"  - {len(matrix['classes_types'])} classes/types")
    print(f"  - {len(matrix['functions'])} functions")
    print(f"  - {len(matrix['dataframe_methods'])} DataFrame methods")


if __name__ == "__main__":
    main()
