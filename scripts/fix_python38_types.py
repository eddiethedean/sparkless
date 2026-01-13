#!/usr/bin/env python3
"""
Script to convert Python 3.9+ type annotations to Python 3.8 compatible syntax.

This script:
1. Replaces built-in generic types (list[...], dict[...], tuple[...], set[...]) with typing module equivalents
2. Replaces Union syntax (int | str) with Union[int, str] or Optional[...]
3. Ensures proper imports are added

Usage:
    python scripts/fix_python38_types.py
"""

import re
import sys
from pathlib import Path
from typing import Tuple

# Patterns to match and replace
REPLACEMENTS = [
    # Built-in generics - must be done carefully to avoid false positives
    (r"\blist\[", "List["),
    (r"\bdict\[", "Dict["),
    (r"\btuple\[", "Tuple["),
    (r"\bset\[", "Set["),
    # Union syntax - need to be careful with context
    (r":\s*(\w+)\s*\|\s*(\w+)", r": Union[\1, \2]"),  # Function parameters
    (r"->\s*(\w+)\s*\|\s*(\w+)", r"-> Union[\1, \2]"),  # Return types
    (r"(\w+)\s*\|\s*None", r"Optional[\1]"),  # Optional pattern
    (r"None\s*\|\s*(\w+)", r"Optional[\1]"),  # Optional pattern reversed
]

# Files to exclude
EXCLUDE_PATTERNS = [
    "venv",
    ".venv",
    "__pycache__",
    ".git",
    "dist",
    "build",
    ".mypy_cache",
    "htmlcov",
    "spark-warehouse",
    "*.pyc",
    "*.pyo",
    "*.egg-info",
]


def should_process_file(file_path: Path) -> bool:
    """Check if file should be processed."""
    file_str = str(file_path)
    return file_path.suffix == ".py" and not any(
        exclude in file_str for exclude in EXCLUDE_PATTERNS
    )


def get_typing_imports_needed(content: str) -> set:
    """Determine which typing imports are needed."""
    imports = set()

    if re.search(r"\bList\[", content):
        imports.add("List")
    if re.search(r"\bDict\[", content):
        imports.add("Dict")
    if re.search(r"\bTuple\[", content):
        imports.add("Tuple")
    if re.search(r"\bSet\[", content):
        imports.add("Set")
    if re.search(r"\bUnion\[", content):
        imports.add("Union")
    if re.search(r"\bOptional\[", content):
        imports.add("Optional")

    return imports


def add_typing_imports(content: str, needed: set) -> str:
    """Add typing imports if not already present."""
    if not needed:
        return content

    # Check if typing is already imported
    typing_import_pattern = r"from typing import"
    existing_typing_match = re.search(typing_import_pattern, content)

    if existing_typing_match:
        # Add to existing import
        # Extract what's already imported
        after_import = content[existing_typing_match.end() :]
        # Find the end of the import line
        line_end = after_import.find("\n")
        if line_end == -1:
            line_end = len(after_import)
        existing_imports_str = after_import[:line_end].strip()

        # Parse existing imports
        existing_imports = {imp.strip() for imp in existing_imports_str.split(",")}
        all_imports = existing_imports | needed

        # Reconstruct import line
        new_import_line = f"from typing import {', '.join(sorted(all_imports))}"
        content = (
            content[: existing_typing_match.start()]
            + new_import_line
            + after_import[line_end:]
        )
    else:
        # Add new import after __future__ imports or at the top
        future_import_pattern = r"(from __future__ import[^\n]+\n)"
        future_match = re.search(future_import_pattern, content)
        if future_match:
            insert_pos = future_match.end()
            new_import = f"from typing import {', '.join(sorted(needed))}\n"
            content = content[:insert_pos] + new_import + content[insert_pos:]
        else:
            # Add at the beginning after docstring
            docstring_end = 0
            if content.startswith('"""') or content.startswith("'''"):
                # Find end of docstring
                quote = content[:3]
                end_quote = content.find(quote, 3)
                if end_quote != -1:
                    # Find next newline after docstring
                    docstring_end = content.find("\n", end_quote + 3)
                    if docstring_end == -1:
                        docstring_end = len(content)
                    else:
                        docstring_end += 1

            new_import = f"from typing import {', '.join(sorted(needed))}\n"
            content = content[:docstring_end] + new_import + content[docstring_end:]

    return content


def fix_file(file_path: Path) -> Tuple[bool, str]:
    """Fix type annotations in a single file."""
    try:
        content = file_path.read_text(encoding="utf-8")
        original_content = content

        # Apply replacements
        for pattern, replacement in REPLACEMENTS:
            content = re.sub(pattern, replacement, content)

        # Determine needed imports
        needed_imports = get_typing_imports_needed(content)

        # Add imports if needed
        if needed_imports:
            content = add_typing_imports(content, needed_imports)

        # Only write if changed
        if content != original_content:
            file_path.write_text(content, encoding="utf-8")
            return True, "Updated"
        return False, "No changes"
    except Exception as e:
        return False, f"Error: {e}"


def main():
    """Main function."""
    project_root = Path(__file__).parent.parent
    sparkless_dir = project_root / "sparkless"
    tests_dir = project_root / "tests"

    files_to_process = []
    for directory in [sparkless_dir, tests_dir]:
        if directory.exists():
            files_to_process.extend(directory.rglob("*.py"))

    # Filter files
    files_to_process = [f for f in files_to_process if should_process_file(f)]

    print(f"Found {len(files_to_process)} Python files to process")

    updated_count = 0
    error_count = 0

    for file_path in sorted(files_to_process):
        changed, message = fix_file(file_path)
        if changed:
            updated_count += 1
            print(f"✓ {file_path.relative_to(project_root)}: {message}")
        elif "Error" in message:
            error_count += 1
            print(f"✗ {file_path.relative_to(project_root)}: {message}")

    print(f"\nSummary: {updated_count} files updated, {error_count} errors")
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
