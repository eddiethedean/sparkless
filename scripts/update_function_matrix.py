#!/usr/bin/env python3
"""
Update PYSPARK_FUNCTION_MATRIX.md based on actual codebase implementation.

This script analyzes the sparkless codebase to determine which functions
and DataFrame methods are actually implemented, then updates the matrix.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Set


# Get all Functions class methods
def get_functions_class_methods() -> Set[str]:
    """Get all public methods from Functions class."""
    try:
        from sparkless.functions.functions import Functions

        methods = set()
        for name in dir(Functions):
            if not name.startswith("_"):
                attr = getattr(Functions, name)
                if callable(attr) and not isinstance(attr, type):
                    methods.add(name)
        return methods
    except Exception as e:
        print(f"Error getting Functions methods: {e}")
        return set()


# Get DataFrame methods
def get_dataframe_methods() -> Set[str]:
    """Get all public methods from DataFrame class."""
    try:
        from sparkless.dataframe.dataframe import DataFrame

        methods = set()
        for name in dir(DataFrame):
            if not name.startswith("_"):
                attr = getattr(DataFrame, name)
                if callable(attr):
                    methods.add(name)
        return methods
    except Exception as e:
        print(f"Error getting DataFrame methods: {e}")
        return set()


def update_matrix_file(
    matrix_path: Path, implemented_functions: Set[str], implemented_methods: Set[str]
) -> None:
    """Update the matrix file with actual implementation status."""

    with open(matrix_path) as f:
        content = f.read()

    # Update functions section
    lines = content.split("\n")
    new_lines = []
    in_functions_table = False
    in_methods_table = False
    functions_updated = 0
    methods_updated = 0

    for i, line in enumerate(lines):
        # Detect table boundaries
        if "| Function |" in line and "3.0.3" in line:
            in_functions_table = True
            in_methods_table = False
            new_lines.append(line)
            continue
        elif "| Method |" in line and "3.0.3" in line:
            in_functions_table = False
            in_methods_table = True
            new_lines.append(line)
            continue
        elif line.startswith("## ") and in_functions_table:
            in_functions_table = False
        elif line.startswith("## ") and in_methods_table:
            in_methods_table = False

        # Update function rows
        if in_functions_table and line.startswith("| `") and "|" in line:
            # Extract function name
            match = re.match(r"\| `([^`]+)` \|", line)
            if match:
                func_name = match.group(1)
                # Check if implemented
                is_implemented = func_name in implemented_functions

                # Update the Sparkless column (last column)
                parts = line.split("|")
                if len(parts) >= 8:  # Function name + 6 versions + Sparkless
                    # Replace last column
                    parts[-2] = " ✅ |" if is_implemented else " ❌ |"
                    new_line = "|".join(parts)
                    new_lines.append(new_line)
                    if is_implemented:
                        functions_updated += 1
                    continue

        # Update method rows
        if in_methods_table and line.startswith("| `") and "|" in line:
            # Extract method name
            match = re.match(r"\| `([^`]+)` \|", line)
            if match:
                method_name = match.group(1)
                # Check if implemented
                is_implemented = method_name in implemented_methods

                # Update the Sparkless column (last column)
                parts = line.split("|")
                if len(parts) >= 8:  # Method name + 6 versions + Sparkless
                    # Replace last column
                    parts[-2] = " ✅ |" if is_implemented else " ❌ |"
                    new_line = "|".join(parts)
                    new_lines.append(new_line)
                    if is_implemented:
                        methods_updated += 1
                    continue

        # Update summary statistics
        if "**Sparkless**:" in line and "functions" in line:
            # Update function count
            func_count = len(implemented_functions)
            new_lines.append(
                f"- **Sparkless**: {func_count} functions, {len(implemented_methods)} DataFrame methods"
            )
            continue

        new_lines.append(line)

    # Write updated content
    with open(matrix_path, "w") as f:
        f.write("\n".join(new_lines))

    print(f"Updated {functions_updated} functions and {methods_updated} methods")
    print(
        f"Total implemented: {len(implemented_functions)} functions, {len(implemented_methods)} methods"
    )


def main():
    """Main update process."""
    print("Analyzing sparkless codebase...")

    # Get implemented items
    implemented_functions = get_functions_class_methods()
    implemented_methods = get_dataframe_methods()

    print(f"Found {len(implemented_functions)} implemented functions")
    print(f"Found {len(implemented_methods)} implemented DataFrame methods")

    # Update matrix file
    repo_root = Path(__file__).parent.parent
    matrix_path = repo_root / "PYSPARK_FUNCTION_MATRIX.md"

    if not matrix_path.exists():
        print(f"Error: {matrix_path} not found")
        return

    print(f"\nUpdating {matrix_path}...")
    update_matrix_file(matrix_path, implemented_functions, implemented_methods)

    print("\n✓ Matrix update complete!")


if __name__ == "__main__":
    main()
