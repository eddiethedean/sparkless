"""
String operation translator for Polars backend.

This module handles translation of string operations from Sparkless Column expressions
to Polars expressions.
"""

from typing import Any, Optional
import polars as pl
import re as re_module


class StringTranslator:
    """Translates string operations to Polars expressions."""

    def __init__(self, base_translator: Any):
        """Initialize StringTranslator.

        Args:
            base_translator: Reference to the main PolarsExpressionTranslator for
                           delegating translation of nested expressions.
        """
        self._base_translator = base_translator

    def translate_string_operation(
        self, expr: pl.Expr, operation: str, value: Any
    ) -> pl.Expr:
        """Translate string operations.

        Args:
            expr: Polars expression (string column)
            operation: String operation name
            value: Operation value

        Returns:
            Polars expression for string operation
        """
        if operation == "contains":
            if isinstance(value, str):
                return expr.str.contains(value)
            else:
                value_expr = self._base_translator.translate(value)
                return expr.str.contains(value_expr)
        elif operation == "startswith":
            if isinstance(value, str):
                return expr.str.starts_with(value)
            else:
                value_expr = self._base_translator.translate(value)
                return expr.str.starts_with(value_expr)
        elif operation == "endswith":
            if isinstance(value, str):
                return expr.str.ends_with(value)
            else:
                value_expr = self._base_translator.translate(value)
                return expr.str.ends_with(value_expr)
        else:
            raise ValueError(f"Unsupported string operation: {operation}")

    def translate_substring(self, col_expr: pl.Expr, op_value: Any) -> pl.Expr:
        """Translate substring operation.

        Args:
            col_expr: Polars expression (string column)
            op_value: Operation value (start) or tuple (start, length)

        Returns:
            Polars expression for substring
        """
        # substring(col, start, length) - Polars uses 0-indexed, PySpark uses 1-indexed
        if isinstance(op_value, tuple):
            start = op_value[0]
            length = op_value[1] if len(op_value) > 1 else None
            # Convert 1-indexed to 0-indexed
            start_idx = start - 1 if start > 0 else 0
            if length is not None:
                return col_expr.str.slice(start_idx, length)
            else:
                return col_expr.str.slice(start_idx)
        else:
            return col_expr.str.slice(op_value - 1 if op_value > 0 else 0)

    def translate_regexp_replace(self, col_expr: pl.Expr, op_value: Any) -> pl.Expr:
        """Translate regexp_replace operation.

        Args:
            col_expr: Polars expression (string column)
            op_value: Tuple of (pattern, replacement)

        Returns:
            Polars expression for regexp_replace
        """
        if isinstance(op_value, tuple) and len(op_value) >= 2:
            pattern = op_value[0]
            replacement = op_value[1]
            return col_expr.str.replace_all(pattern, replacement, literal=True)
        else:
            raise ValueError("regexp_replace requires (pattern, replacement) tuple")

    def translate_regexp_extract(self, col_expr: pl.Expr, op_value: Any) -> pl.Expr:
        """Translate regexp_extract operation.

        Args:
            col_expr: Polars expression (string column)
            op_value: Tuple of (pattern, idx)

        Returns:
            Polars expression for regexp_extract
        """
        if isinstance(op_value, tuple) and len(op_value) >= 2:
            pattern = op_value[0]
            idx = op_value[1] if len(op_value) > 1 else 0

            # Check if pattern contains lookahead/lookbehind assertions
            has_lookaround = False
            try:
                if re_module.search(r"\(\?[<>=!]", pattern):
                    has_lookaround = True
            except Exception:
                has_lookaround = False

            if has_lookaround:
                # Use Python re module fallback
                def regexp_extract_fallback(val: Any) -> Optional[str]:
                    """Fallback for regexp_extract with lookahead/lookbehind."""
                    if val is None or not isinstance(val, str):
                        return None
                    try:
                        match = re_module.search(pattern, val)
                        if match:
                            if idx == 0:
                                return str(match.group(0))
                            elif idx <= len(match.groups()):
                                group_result = match.group(idx)
                                return (
                                    str(group_result)
                                    if group_result is not None
                                    else None
                                )
                            else:
                                return None
                        return None
                    except Exception:
                        return None

                return col_expr.map_elements(
                    regexp_extract_fallback, return_dtype=pl.Utf8
                )
            else:
                # Try Polars native extract first
                try:
                    if idx == 0:
                        # For idx=0 (full match), use extract_all and get first element
                        extracted = col_expr.str.extract_all(pattern)

                        def get_first_match(matches: Any) -> Optional[str]:
                            if (
                                matches is None
                                or not isinstance(matches, list)
                                or len(matches) == 0
                            ):
                                return None
                            return matches[0] if matches else None

                        return extracted.map_elements(
                            get_first_match, return_dtype=pl.Utf8
                        )
                    else:
                        # For idx > 0, use Polars extract with group index
                        return col_expr.str.extract(pattern, idx)
                except pl.exceptions.ComputeError as e:
                    error_msg = str(e).lower()
                    if (
                        "look-around" in error_msg
                        or "look-ahead" in error_msg
                        or "look-behind" in error_msg
                    ):
                        # Fallback to Python re module
                        def regexp_extract_fallback(val: Any) -> Optional[str]:
                            """Fallback for regexp_extract with lookahead/lookbehind."""
                            if val is None or not isinstance(val, str):
                                return None
                            try:
                                match = re_module.search(pattern, val)
                                if match:
                                    if idx == 0:
                                        return str(match.group(0))
                                    elif idx <= len(match.groups()):
                                        group_result = match.group(idx)
                                        return (
                                            str(group_result)
                                            if group_result is not None
                                            else None
                                        )
                                    else:
                                        return None
                                return None
                            except Exception:
                                return None

                        return col_expr.map_elements(
                            regexp_extract_fallback, return_dtype=pl.Utf8
                        )
                    else:
                        raise
        else:
            raise ValueError("regexp_extract requires (pattern, idx) tuple")

    def translate_split(self, col_expr: pl.Expr, op_value: Any) -> pl.Expr:
        """Translate split operation.

        Args:
            col_expr: Polars expression (string column)
            op_value: Delimiter or tuple (delimiter, limit)

        Returns:
            Polars expression for split
        """
        if isinstance(op_value, tuple):
            delimiter, limit = op_value
        else:
            delimiter = op_value
            limit = None

        # Polars split() doesn't have limit parameter
        if limit is None or limit == -1:
            return col_expr.str.split(delimiter)
        else:
            # Use Python fallback for split with limit
            def split_with_limit(val: Any) -> Optional[list]:
                """Split string with limit using Python."""
                if val is None:
                    return None
                try:
                    s = str(val)
                    if limit == 1:
                        return [s]
                    maxsplit = limit - 1
                    parts = s.split(delimiter, maxsplit=maxsplit)
                    return parts
                except Exception:
                    return None

            return col_expr.map_elements(
                split_with_limit, return_dtype=pl.List(pl.Utf8)
            )

    def translate_rlike(self, col_expr: pl.Expr, pattern: str) -> pl.Expr:
        """Translate rlike operation (regex pattern matching).

        Args:
            col_expr: Polars expression (string column)
            pattern: Regex pattern string

        Returns:
            Polars expression for rlike
        """
        # Check if pattern contains lookahead/lookbehind assertions
        has_lookaround = False
        try:
            if re_module.search(r"\(\?[<>=!]", pattern):
                has_lookaround = True
        except Exception:
            has_lookaround = False

        if has_lookaround:
            # Use Python re module fallback
            def rlike_fallback(val: Any) -> bool:
                """Fallback for rlike with lookahead/lookbehind."""
                if val is None or not isinstance(val, str):
                    return False
                try:
                    return bool(re_module.search(pattern, val))
                except Exception:
                    return False

            return col_expr.map_elements(rlike_fallback, return_dtype=pl.Boolean)
        else:
            # Try Polars native contains first
            try:
                return col_expr.str.contains(pattern, literal=False)
            except pl.exceptions.ComputeError as e:
                error_msg = str(e).lower()
                if (
                    "look-around" in error_msg
                    or "look-ahead" in error_msg
                    or "look-behind" in error_msg
                ):
                    # Fallback to Python re module
                    def rlike_fallback(val: Any) -> bool:
                        """Fallback for rlike with lookahead/lookbehind."""
                        if val is None or not isinstance(val, str):
                            return False
                        try:
                            return bool(re_module.search(pattern, val))
                        except Exception:
                            return False

                    return col_expr.map_elements(
                        rlike_fallback, return_dtype=pl.Boolean
                    )
                else:
                    raise
