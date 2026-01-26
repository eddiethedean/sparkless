#!/usr/bin/env python3
"""Check documentation accuracy.

This script verifies:
- Version numbers match across documentation
- Links work correctly
- Feature claims are accurate
- No outdated information
"""

import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def get_package_version() -> str:
    """Get version from package.

    Returns:
        Package version string.
    """
    try:
        from sparkless import __version__
        return __version__
    except ImportError:
        try:
            import tomllib
            with open(project_root / "pyproject.toml", "rb") as f:
                data = tomllib.load(f)
                return data["project"]["version"]
        except Exception:
            return "unknown"


def check_version_numbers() -> List[Tuple[str, bool, str]]:
    """Check version numbers in documentation files.

    Returns:
        List of (file_path, is_valid, message) tuples.
    """
    results = []
    package_version = get_package_version()
    
    # Files to check
    files_to_check = [
        ("docs/getting_started.md", r"3\.\d+\.\d+"),
        ("README.md", r"3\.\d+\.\d+"),
        ("CHANGELOG.md", r"## \d+\.\d+\.\d+"),
    ]
    
    for file_path, pattern in files_to_check:
        full_path = project_root / file_path
        if not full_path.exists():
            results.append((file_path, False, "File not found"))
            continue
        
        content = full_path.read_text(encoding="utf-8")
        matches = re.findall(pattern, content)
        
        if matches:
            # Check if version matches (allowing for minor differences in format)
            version_found = matches[0] if isinstance(matches[0], str) else matches[0].split()[-1]
            # Extract just version number
            version_match = re.search(r"(\d+\.\d+\.\d+)", version_found)
            if version_match:
                found_version = version_match.group(1)
                # Compare major.minor (allow patch to differ in docs)
                pkg_major_minor = ".".join(package_version.split(".")[:2])
                found_major_minor = ".".join(found_version.split(".")[:2])
                
                if pkg_major_minor == found_major_minor:
                    results.append((file_path, True, f"Version {found_version} found (package: {package_version})"))
                else:
                    results.append((file_path, False, f"Version mismatch: found {found_version}, package {package_version}"))
            else:
                results.append((file_path, True, f"Version pattern found: {matches[0]}"))
        else:
            results.append((file_path, True, "No specific version mentioned (OK)"))
    
    return results


def check_feature_claims() -> List[Tuple[str, bool, str]]:
    """Verify feature claims in documentation.

    Returns:
        List of (claim, is_valid, message) tuples.
    """
    results = []
    
    # Check function count claim
    try:
        from sparkless.functions.functions import Functions
        func_count = len([m for m in dir(Functions) if not m.startswith("_") and callable(getattr(Functions, m))])
        results.append(("120+ functions", func_count >= 120, f"Found {func_count} functions"))
    except Exception as e:
        results.append(("120+ functions", False, f"Error: {e}"))
    
    # Check DataFrame methods count
    try:
        from sparkless.dataframe.dataframe import DataFrame
        method_count = len([m for m in dir(DataFrame) if not m.startswith("_") and callable(getattr(DataFrame, m))])
        results.append(("70+ DataFrame methods", method_count >= 70, f"Found {method_count} methods"))
    except Exception as e:
        results.append(("70+ DataFrame methods", False, f"Error: {e}"))
    
    return results


def check_links() -> List[Tuple[str, bool, str]]:
    """Check internal links in documentation.

    Returns:
        List of (link, is_valid, message) tuples.
    """
    results = []
    
    docs_dir = project_root / "docs"
    markdown_files = list(docs_dir.rglob("*.md"))
    
    link_pattern = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
    
    for md_file in markdown_files:
        content = md_file.read_text(encoding="utf-8")
        links = link_pattern.findall(content)
        
        for link_text, link_path in links:
            # Skip external links
            if link_path.startswith("http"):
                continue
            
            # Resolve relative paths
            if link_path.startswith("/"):
                target = project_root / link_path.lstrip("/")
            else:
                target = md_file.parent / link_path
            
            # Handle anchor links
            if "#" in str(target):
                target = Path(str(target).split("#")[0])
            
            if target.exists():
                results.append((f"{md_file.name}: {link_text}", True, f"Link to {link_path} works"))
            else:
                results.append((f"{md_file.name}: {link_text}", False, f"Broken link: {link_path}"))
    
    return results


def main() -> int:
    """Run documentation accuracy checks.

    Returns:
        Exit code (0 for success, non-zero for failures).
    """
    print("=" * 60)
    print("Documentation Accuracy Check")
    print("=" * 60)
    
    all_failures = 0
    all_checks = 0
    
    print("\nChecking version numbers...")
    version_results = check_version_numbers()
    all_checks += len(version_results)
    for file_path, is_valid, msg in version_results:
        status = "✅" if is_valid else "❌"
        print(f"  {status} {file_path}: {msg}")
        if not is_valid:
            all_failures += 1
    
    print("\nVerifying feature claims...")
    feature_results = check_feature_claims()
    all_checks += len(feature_results)
    for claim, is_valid, msg in feature_results:
        status = "✅" if is_valid else "❌"
        print(f"  {status} {claim}: {msg}")
        if not is_valid:
            all_failures += 1
    
    print("\nChecking links (sample)...")
    link_results = check_links()
    # Only show first 20 results to avoid spam
    sample_results = link_results[:20]
    all_checks += len(link_results)
    for link, is_valid, msg in sample_results:
        status = "✅" if is_valid else "❌"
        print(f"  {status} {link}: {msg}")
        if not is_valid:
            all_failures += 1
    
    if len(link_results) > 20:
        remaining = len(link_results) - 20
        failures_remaining = sum(1 for _, is_valid, _ in link_results[20:] if not is_valid)
        all_failures += failures_remaining
        print(f"  ... and {remaining} more links ({failures_remaining} failures)")
    
    print("\n" + "=" * 60)
    print(f"Summary: {all_checks - all_failures}/{all_checks} checks passed")
    if all_failures > 0:
        print(f"Failures: {all_failures}")
        return 1
    else:
        print("All checks passed!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
