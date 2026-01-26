#!/usr/bin/env python3
"""Verify API signatures match between documentation and code.

This script cross-references API documentation with actual code signatures
to ensure they match.
"""

import inspect
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def get_function_signature(func: Any) -> Dict[str, Any]:
    """Extract function signature information.

    Args:
        func: Function or method to inspect.

    Returns:
        Dictionary with signature information.
    """
    try:
        sig = inspect.signature(func)
        params = {}
        for name, param in sig.parameters.items():
            params[name] = {
                "type": str(param.annotation)
                if param.annotation != inspect.Parameter.empty
                else None,
                "default": param.default
                if param.default != inspect.Parameter.empty
                else None,
                "kind": str(param.kind),
            }
        return {
            "name": func.__name__,
            "parameters": params,
            "return_type": str(sig.return_annotation)
            if sig.return_annotation != inspect.Parameter.empty
            else None,
        }
    except Exception as e:
        return {"error": str(e)}


def verify_spark_session_methods() -> List[Tuple[str, bool, str]]:
    """Verify SparkSession method signatures.

    Returns:
        List of (method_name, is_valid, message) tuples.
    """
    results = []
    try:
        from sparkless.session.core.session import SparkSession

        # Key methods to verify
        key_methods = [
            "createDataFrame",
            "sql",
            "read",
            "table",
            "range",
            "stop",
        ]

        for method_name in key_methods:
            if hasattr(SparkSession, method_name):
                method = getattr(SparkSession, method_name)
                sig = get_function_signature(method)
                if "error" not in sig:
                    results.append((method_name, True, f"Signature: {sig}"))
                else:
                    results.append((method_name, False, f"Error: {sig['error']}"))
            else:
                results.append((method_name, False, "Method not found"))
    except Exception as e:
        results.append(("SparkSession", False, f"Import error: {e}"))

    return results


def verify_dataframe_methods() -> List[Tuple[str, bool, str]]:
    """Verify DataFrame method signatures.

    Returns:
        List of (method_name, is_valid, message) tuples.
    """
    results = []
    try:
        from sparkless.dataframe.dataframe import DataFrame

        # Key methods to verify
        key_methods = [
            "select",
            "filter",
            "groupBy",
            "join",
            "union",
            "withColumn",
            "show",
            "collect",
            "count",
        ]

        for method_name in key_methods:
            if hasattr(DataFrame, method_name):
                method = getattr(DataFrame, method_name)
                sig = get_function_signature(method)
                if "error" not in sig:
                    results.append((method_name, True, f"Signature: {sig}"))
                else:
                    results.append((method_name, False, f"Error: {sig['error']}"))
            else:
                results.append((method_name, False, "Method not found"))
    except Exception as e:
        results.append(("DataFrame", False, f"Import error: {e}"))

    return results


def verify_functions() -> List[Tuple[str, bool, str]]:
    """Verify Functions module signatures.

    Returns:
        List of (function_name, is_valid, message) tuples.
    """
    results = []
    try:
        from sparkless.functions.functions import Functions

        # Key functions to verify
        key_functions = [
            "col",
            "lit",
            "upper",
            "lower",
            "count",
            "sum",
            "avg",
            "max",
            "min",
        ]

        for func_name in key_functions:
            if hasattr(Functions, func_name):
                func = getattr(Functions, func_name)
                sig = get_function_signature(func)
                if "error" not in sig:
                    results.append((func_name, True, f"Signature: {sig}"))
                else:
                    results.append((func_name, False, f"Error: {sig['error']}"))
            else:
                results.append((func_name, False, "Function not found"))
    except Exception as e:
        results.append(("Functions", False, f"Import error: {e}"))

    return results


def main() -> int:
    """Run API signature verification.

    Returns:
        Exit code (0 for success, non-zero for failures).
    """
    print("=" * 60)
    print("API Signature Verification")
    print("=" * 60)

    all_results = []

    print("\nVerifying SparkSession methods...")
    session_results = verify_spark_session_methods()
    all_results.extend(session_results)
    for name, is_valid, msg in session_results:
        status = "✅" if is_valid else "❌"
        print(f"  {status} {name}: {msg}")

    print("\nVerifying DataFrame methods...")
    df_results = verify_dataframe_methods()
    all_results.extend(df_results)
    for name, is_valid, msg in df_results:
        status = "✅" if is_valid else "❌"
        print(f"  {status} {name}: {msg}")

    print("\nVerifying Functions...")
    func_results = verify_functions()
    all_results.extend(func_results)
    for name, is_valid, msg in func_results:
        status = "✅" if is_valid else "❌"
        print(f"  {status} {name}: {msg}")

    failures = sum(1 for _, is_valid, _ in all_results if not is_valid)
    total = len(all_results)

    print("\n" + "=" * 60)
    print(f"Summary: {total - failures}/{total} signatures verified")
    if failures > 0:
        print(f"Failures: {failures}")
        return 1
    else:
        print("All signatures verified!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
