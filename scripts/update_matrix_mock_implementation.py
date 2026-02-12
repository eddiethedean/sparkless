#!/usr/bin/env python3
"""
Update the Sparkless column in PYSPARK_FUNCTION_MATRIX.md and pyspark_api_matrix.json
based on actual implementation.

This script scans the mock_spark codebase to identify which functions and DataFrame methods
are actually implemented and updates the matrix to reflect the actual implementation status.
"""

import json
import re
import sys
from pathlib import Path

# Add parent directory to path to allow importing mock_spark
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))


def get_implemented_functions():
    """Read the __all__ list from sparkless/functions/__init__.py"""
    init_file = Path(__file__).parent.parent / "sparkless" / "functions" / "__init__.py"

    with open(init_file) as f:
        content = f.read()

    # Extract the __all__ list
    match = re.search(r"__all__\s*=\s*\[(.*?)\]", content, re.DOTALL)
    if not match:
        return set()

    all_str = match.group(1)
    # Extract quoted strings
    functions = re.findall(r'"([^"]+)"', all_str)
    return set(functions)


def get_implemented_dataframe_methods():
    """Get all implemented DataFrame methods by checking MockDataFrame and its mixins."""
    methods = set()

    # Import MockDataFrame to check its methods
    try:
        from sparkless.dataframe.dataframe import MockDataFrame

        # Get all public methods from MockDataFrame
        for attr_name in dir(MockDataFrame):
            if not attr_name.startswith("_"):
                attr = getattr(MockDataFrame, attr_name)
                if callable(attr):
                    methods.add(attr_name)

        # Also check mixin classes
        from sparkless.dataframe.transformations import TransformationOperations
        from sparkless.dataframe.joins import JoinOperations
        from sparkless.dataframe.aggregations import AggregationOperations
        from sparkless.dataframe.display import DisplayOperations
        from sparkless.dataframe.schema import SchemaOperations
        from sparkless.dataframe.assertions import AssertionOperations
        from sparkless.dataframe.operations import MiscellaneousOperations

        mixin_classes = [
            TransformationOperations,
            JoinOperations,
            AggregationOperations,
            DisplayOperations,
            SchemaOperations,
            AssertionOperations,
            MiscellaneousOperations,
        ]

        for mixin in mixin_classes:
            for attr_name in dir(mixin):
                if not attr_name.startswith("_"):
                    attr = getattr(mixin, attr_name)
                    if callable(attr):
                        methods.add(attr_name)
    except Exception as e:
        print(f"Warning: Could not check DataFrame methods: {e}", file=sys.stderr)

    return methods


def update_matrix_markdown(functions, df_methods, matrix_file):
    """Update the markdown matrix file with implementation status"""
    with open(matrix_file) as f:
        lines = f.readlines()

    # Track changes
    updated_count = 0
    in_functions_section = False
    in_methods_section = False

    # Iterate through each line
    for i, line in enumerate(lines):
        # Detect which section we're in
        if "## Functions" in line:
            in_functions_section = True
            in_methods_section = False
            continue
        elif "## DataFrame Methods" in line:
            in_functions_section = False
            in_methods_section = True
            continue
        elif line.startswith("##"):
            # Other section
            in_functions_section = False
            in_methods_section = False
            continue

        # Check if this line is a function/method row
        # Format: | `function_name` | 3.0.3 | 3.1.3 | 3.2.4 | 3.3.4 | 3.4.3 | 3.5.2 | Sparkless |
        match = re.match(r"^\|\s+`([^`]+)`\s+\|.*$", line)
        if match:
            item_name = match.group(1)
            implemented = False

            if in_functions_section:
                implemented = item_name in functions
            elif in_methods_section:
                implemented = item_name in df_methods

            if implemented:
                # Parse the line to get the Sparkless column
                parts = line.split("|")
                if len(parts) >= 8:
                    # Sparkless column is index 7
                    current_status = parts[7].strip()

                    # Update if it's not already ✅ or 🔷
                    if current_status != "✅" and current_status != "🔷":
                        parts[7] = " ✅ "
                        lines[i] = "|".join(parts)
                        updated_count += 1
                        section = "function" if in_functions_section else "method"
                        print(
                            f"Updated {section}: {item_name} (was: '{current_status}')"
                        )

    # Write back
    with open(matrix_file, "w") as f:
        f.writelines(lines)

    return updated_count


def update_matrix_json(functions, df_methods, json_file):
    """Update the JSON matrix file with implementation status"""
    with open(json_file) as f:
        matrix = json.load(f)

    updated_count = 0

    # Update functions
    for func_name in matrix.get("functions", {}):
        if func_name in functions and (
            "sparkless" not in matrix["functions"][func_name]
            or not matrix["functions"][func_name].get("sparkless", False)
        ):
            # Add mock_spark field if not present
            matrix["functions"][func_name]["sparkless"] = True
            updated_count += 1
            print(f"Updated JSON function: {func_name}")

    # Update DataFrame methods
    for method_name in matrix.get("dataframe_methods", {}):
        if method_name in df_methods and (
            "sparkless" not in matrix["dataframe_methods"][method_name]
            or not matrix["dataframe_methods"][method_name].get("sparkless", False)
        ):
            # Add mock_spark field if not present
            matrix["dataframe_methods"][method_name]["sparkless"] = True
            updated_count += 1
            print(f"Updated JSON method: {method_name}")

    # Write back
    with open(json_file, "w") as f:
        json.dump(matrix, f, indent=2, default=str)

    return updated_count


def main():
    """Main entry point"""
    matrix_md = repo_root / "PYSPARK_FUNCTION_MATRIX.md"
    matrix_json = repo_root / "sparkless" / "pyspark_api_matrix.json"

    print("Scanning mock_spark functions...")
    functions = get_implemented_functions()
    print(f"Found {len(functions)} implemented functions")

    print("Scanning mock_spark DataFrame methods...")
    df_methods = get_implemented_dataframe_methods()
    print(f"Found {len(df_methods)} implemented DataFrame methods")

    print(f"\nUpdating {matrix_md}...")
    updated_md = update_matrix_markdown(functions, df_methods, matrix_md)
    print(f"Updated {updated_md} entries in markdown")

    print(f"\nUpdating {matrix_json}...")
    updated_json = update_matrix_json(functions, df_methods, matrix_json)
    print(f"Updated {updated_json} entries in JSON")

    return 0


if __name__ == "__main__":
    sys.exit(main())
