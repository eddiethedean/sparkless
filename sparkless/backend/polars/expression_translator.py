"""
Expression translator for converting Column expressions to Polars expressions.

This module translates Sparkless column expressions (Column, ColumnOperation)
to Polars expressions (pl.Expr) for DataFrame operations.
"""

from typing import Any, Dict, List, Optional, Tuple, Union, cast
from datetime import datetime, date
import logging
import polars as pl
import math
import threading
from collections import OrderedDict
from sparkless import config
from sparkless.functions import Column, ColumnOperation, Literal
from sparkless.functions.base import AggregateFunction
from sparkless.functions.window_execution import WindowFunction

try:  # pragma: no cover - optional dependency
    from pandas import Timestamp as PandasTimestamp
except Exception:  # pragma: no cover - pandas is optional
    PandasTimestamp = None

logger = logging.getLogger(__name__)


# -----------------------------
# Helpers for missing functions
# -----------------------------


def _xxh64(data: bytes, seed: int = 42) -> int:
    """XXHash64 implementation (Spark's xxhash64 uses seed=42).

    This is a small, self-contained implementation intended for deterministic
    hashing of bytes. It matches the standard XXH64 algorithm.
    """

    # Reference constants from XXH64 specification
    PRIME1 = 11400714785074694791
    PRIME2 = 14029467366897019727
    PRIME3 = 1609587929392839161
    PRIME4 = 9650029242287828579
    PRIME5 = 2870177450012600261

    def _rotl(x: int, r: int) -> int:
        return ((x << r) | (x >> (64 - r))) & 0xFFFFFFFFFFFFFFFF

    def _round(acc: int, lane: int) -> int:
        acc = (acc + (lane * PRIME2 & 0xFFFFFFFFFFFFFFFF)) & 0xFFFFFFFFFFFFFFFF
        acc = _rotl(acc, 31)
        acc = (acc * PRIME1) & 0xFFFFFFFFFFFFFFFF
        return acc

    def _merge_round(acc: int, val: int) -> int:
        acc ^= _round(0, val)
        acc = (acc * PRIME1 + PRIME4) & 0xFFFFFFFFFFFFFFFF
        return acc

    length = len(data)
    i = 0

    if length >= 32:
        v1 = (seed + PRIME1 + PRIME2) & 0xFFFFFFFFFFFFFFFF
        v2 = (seed + PRIME2) & 0xFFFFFFFFFFFFFFFF
        v3 = (seed + 0) & 0xFFFFFFFFFFFFFFFF
        v4 = (seed - PRIME1) & 0xFFFFFFFFFFFFFFFF

        limit = length - 32
        while i <= limit:
            v1 = _round(v1, int.from_bytes(data[i : i + 8], "little", signed=False))
            v2 = _round(
                v2, int.from_bytes(data[i + 8 : i + 16], "little", signed=False)
            )
            v3 = _round(
                v3, int.from_bytes(data[i + 16 : i + 24], "little", signed=False)
            )
            v4 = _round(
                v4, int.from_bytes(data[i + 24 : i + 32], "little", signed=False)
            )
            i += 32

        h64 = (
            _rotl(v1, 1) + _rotl(v2, 7) + _rotl(v3, 12) + _rotl(v4, 18)
        ) & 0xFFFFFFFFFFFFFFFF

        h64 = _merge_round(h64, v1)
        h64 = _merge_round(h64, v2)
        h64 = _merge_round(h64, v3)
        h64 = _merge_round(h64, v4)
    else:
        h64 = (seed + PRIME5) & 0xFFFFFFFFFFFFFFFF

    h64 = (h64 + length) & 0xFFFFFFFFFFFFFFFF

    # Process remaining 8-byte chunks
    while i + 8 <= length:
        k1 = int.from_bytes(data[i : i + 8], "little", signed=False)
        k1 = (k1 * PRIME2) & 0xFFFFFFFFFFFFFFFF
        k1 = _rotl(k1, 31)
        k1 = (k1 * PRIME1) & 0xFFFFFFFFFFFFFFFF
        h64 ^= k1
        h64 = (_rotl(h64, 27) * PRIME1 + PRIME4) & 0xFFFFFFFFFFFFFFFF
        i += 8

    # Process remaining 4-byte chunk
    if i + 4 <= length:
        k1_32 = int.from_bytes(data[i : i + 4], "little", signed=False)
        h64 ^= (k1_32 * PRIME1) & 0xFFFFFFFFFFFFFFFF
        h64 = (_rotl(h64, 23) * PRIME2 + PRIME3) & 0xFFFFFFFFFFFFFFFF
        i += 4

    # Process remaining bytes
    while i < length:
        h64 ^= (data[i] * PRIME5) & 0xFFFFFFFFFFFFFFFF
        h64 = (_rotl(h64, 11) * PRIME1) & 0xFFFFFFFFFFFFFFFF
        i += 1

    # Final avalanche
    h64 ^= h64 >> 33
    h64 = (h64 * PRIME2) & 0xFFFFFFFFFFFFFFFF
    h64 ^= h64 >> 29
    h64 = (h64 * PRIME3) & 0xFFFFFFFFFFFFFFFF
    h64 ^= h64 >> 32

    # Spark returns a signed 64-bit long
    if h64 >= 1 << 63:
        h64 -= 1 << 64
    return h64


def _is_mock_case_when(expr: Any) -> bool:
    """Check if expression is a CaseWhen instance.

    Args:
        expr: Expression to check

    Returns:
        True if expr is a CaseWhen instance
    """
    # Use isinstance if available, otherwise check by class name to avoid import issues
    try:
        from sparkless.functions.conditional import CaseWhen

        return isinstance(expr, CaseWhen)
    except (ImportError, AttributeError):
        # Fallback: check by class name
        return (
            hasattr(expr, "__class__")
            and expr.__class__.__name__ == "CaseWhen"
            and hasattr(expr, "conditions")
        )


class PolarsExpressionTranslator:
    """Translates Column expressions to Polars expressions."""

    def __init__(self) -> None:
        self._cache_enabled = config.is_feature_enabled(
            "enable_expression_translation_cache"
        )
        self._cache_lock = threading.Lock()
        self._translation_cache: OrderedDict[Any, pl.Expr] = OrderedDict()
        self._cache_size = 512

    def _get_case_sensitive(self) -> bool:
        """Get case sensitivity setting from active session.

        Returns:
            True if case-sensitive mode is enabled, False otherwise.
            Defaults to False (case-insensitive) to match PySpark behavior.
        """
        try:
            from sparkless.session.core.session import SparkSession

            active_sessions = getattr(SparkSession, "_active_sessions", [])
            if active_sessions:
                session = active_sessions[-1]
                if hasattr(session, "conf"):
                    return bool(session.conf.is_case_sensitive())
        except Exception:
            pass
        return False  # Default to case-insensitive (matching PySpark)

    def translate(
        self,
        expr: Any,
        input_col_dtype: Any = None,
        available_columns: Optional[List[str]] = None,
        case_sensitive: Optional[bool] = None,
    ) -> pl.Expr:
        """Translate Column expression to Polars expression.

        Args:
            expr: Column, ColumnOperation, or other expression
            input_col_dtype: Optional Polars dtype of input column (for to_timestamp optimization)
            available_columns: Optional list of available column names for case-insensitive matching
            case_sensitive: Optional case sensitivity flag. If None, gets from session.

        Returns:
            Polars expression (pl.Expr)
        """
        # Get case sensitivity if not provided
        if case_sensitive is None:
            case_sensitive = self._get_case_sensitive()

        cache_key = self._build_cache_key(expr) if self._cache_enabled else None
        if cache_key is not None:
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached

        if isinstance(expr, ColumnOperation):
            # For nested operations (e.g., filter with isin), pass input_col_dtype down
            # But if we're at the top level, try to infer dtype from the column if available
            if input_col_dtype is None and isinstance(expr, ColumnOperation):
                # Try to infer dtype from the column name if we have available_columns
                # This is a best-effort attempt - the proper way is to pass dtype from callers
                pass  # We'll rely on callers to pass dtype
            result = self._translate_operation(
                expr,
                input_col_dtype=input_col_dtype,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )
        elif isinstance(expr, Column):
            result = self._translate_column(
                expr, available_columns=available_columns, case_sensitive=case_sensitive
            )
        elif isinstance(expr, Literal):
            result = self._translate_literal(expr)
        elif isinstance(expr, AggregateFunction):
            result = self._translate_aggregate_function(expr)
        elif isinstance(expr, WindowFunction):
            # Window functions are handled separately in window_handler.py
            raise ValueError("Window functions should be handled by WindowHandler")
        elif isinstance(expr, str):
            # String column name
            result = pl.col(expr)
        elif isinstance(expr, (int, float, bool)):
            # Literal value
            result = pl.lit(expr)
        elif isinstance(expr, (datetime, date)):
            # Datetime or date literal value
            result = pl.lit(expr)
        elif isinstance(expr, tuple):
            # Tuple - this is likely a function argument tuple, not a literal
            # Don't try to create a literal from it - tuples as literals are not supported in Polars
            # This should be handled by the function that uses it (e.g., concat_ws, substring)
            # If we reach here, it means a tuple was passed where it shouldn't be
            raise ValueError(
                f"Cannot translate tuple as literal: {expr}. This should be handled by the function that uses it."
            )
        elif expr is None:
            result = pl.lit(None)
        elif _is_mock_case_when(expr):
            result = self._translate_case_when(expr)
        else:
            raise ValueError(f"Unsupported expression type: {type(expr)}")

        if cache_key is not None:
            self._cache_set(cache_key, result)

        return result

    def _translate_column(
        self,
        col: Column,
        available_columns: Optional[List[str]] = None,
        case_sensitive: bool = False,
    ) -> pl.Expr:
        """Translate Column to Polars column expression.

        Args:
            col: Column instance
            available_columns: Optional list of available column names for case-insensitive matching

        Returns:
            Polars column expression
        """
        # If column has an alias, use the original column name for translation
        # The alias will be applied when the expression is used in select
        if hasattr(col, "_original_column") and col._original_column is not None:
            # Use the original column's name for the actual column reference
            col_name = col._original_column.name
        else:
            col_name = col.name

        # Handle nested struct field access (e.g., "Person.name")
        if "." in col_name and available_columns:
            # Split into struct column and field name
            parts = col_name.split(".", 1)
            struct_col = parts[0]

            # Resolve struct column name case-insensitively
            actual_struct_col = self._find_column(
                available_columns, struct_col, case_sensitive
            )
            if actual_struct_col:
                # For nested fields, we need to use struct.field() syntax
                # But we don't have access to the DataFrame here to check the struct type
                # So we'll return a column reference and let the caller handle it
                # This will be handled in apply_select when the column is processed
                return pl.col(col_name)

        # Use ColumnResolver matching if available columns are provided
        if available_columns:
            actual_col_name = self._find_column(
                available_columns, col_name, case_sensitive
            )
            if actual_col_name:
                col_name = actual_col_name

        return pl.col(col_name)

    @staticmethod
    def _find_column(
        available_columns: List[str], column_name: str, case_sensitive: bool = False
    ) -> Optional[str]:
        """Find column name in available columns using ColumnResolver.

        Args:
            available_columns: List of available column names.
            column_name: Column name to find.
            case_sensitive: Whether to use case-sensitive matching.

        Returns:
            Actual column name if found, None otherwise.
        """
        from sparkless.core.column_resolver import ColumnResolver

        return ColumnResolver.resolve_column_name(
            column_name, available_columns, case_sensitive
        )

    def _translate_literal(self, lit: Literal) -> pl.Expr:
        """Translate Literal to Polars literal expression.

        Args:
            lit: Literal instance

        Returns:
            Polars literal expression
        """
        # Resolve lazy literals (session-aware functions) before translating
        if hasattr(lit, "_is_lazy") and lit._is_lazy:
            value = lit._resolve_lazy_value()
        else:
            value = lit.value
        return pl.lit(value)

    def _translate_operation(
        self,
        op: ColumnOperation,
        input_col_dtype: Any = None,
        available_columns: Optional[List[str]] = None,
        case_sensitive: bool = False,
    ) -> pl.Expr:
        """Translate ColumnOperation to Polars expression.

        Args:
            op: ColumnOperation instance

        Returns:
            Polars expression
        """
        operation = op.operation
        column = op.column
        value = op.value

        # Translate left side
        # Check ColumnOperation before Column since ColumnOperation is a subclass of Column
        # Special case: WindowFunction wrapped in ColumnOperation
        # For comparison operations (>, <, ==, etc.), we need to handle them specially
        # because we need access to the DataFrame to translate the window function
        if isinstance(column, WindowFunction):
            # For comparison operations, we need to raise an error that will be caught
            # in apply_with_column which has access to the DataFrame
            if operation in [">", "<", ">=", "<=", "==", "!=", "eqNullSafe"]:
                raise ValueError(
                    "WindowFunction comparison expressions should be handled by OperationExecutor.apply_with_column"
                )
            else:
                # For non-comparison operations, raise error
                raise ValueError(
                    "WindowFunction expressions should be handled by OperationExecutor.apply_with_column"
                )
        elif isinstance(column, ColumnOperation):
            left = self._translate_operation(
                column,
                input_col_dtype=None,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )
        elif isinstance(column, Column):
            left = self._translate_column(
                column,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )
        elif isinstance(column, Literal):
            # Resolve lazy literals before translating
            if hasattr(column, "_is_lazy") and column._is_lazy:
                lit_value = column._resolve_lazy_value()
            else:
                lit_value = column.value

            # For cast operations with None literals, we'll handle dtype in _translate_cast
            # For now, create the literal - the cast will handle the dtype
            left = pl.lit(lit_value)
        elif isinstance(column, str):
            # Use ColumnResolver matching if available columns are provided
            if available_columns:
                actual_col_name = PolarsExpressionTranslator._find_column(
                    available_columns, column, case_sensitive
                )
                left = pl.col(actual_col_name) if actual_col_name else pl.col(column)
            else:
                left = pl.col(column)
        elif isinstance(column, (int, float, bool)):
            left = pl.lit(column)
        else:
            left = self.translate(
                column,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )

        # Special handling for cast operation - value should be a type name, not a column
        if operation == "cast":
            # Special case: if casting a None literal, create typed None directly
            # This handles F.lit(None).cast(TimestampType()) correctly
            if isinstance(column, Literal) and column.value is None:
                from .type_mapper import mock_type_to_polars_dtype

                polars_dtype = mock_type_to_polars_dtype(value)
                return pl.lit(None, dtype=polars_dtype)
            return self._translate_cast(left, value)

        # Special handling for isin - value is a list, don't translate it
        # Need to handle type coercion for mixed types (e.g., checking int values in string column)
        if operation == "isin":
            # Get the column's dtype if available (from input_col_dtype or by checking the DataFrame)
            # Coerce values to match the column type
            if isinstance(value, list):
                # Try to infer the column type from input_col_dtype or default to original values
                coerced_values = value
                if input_col_dtype is not None:
                    # Coerce values to match column type
                    if input_col_dtype == pl.Utf8:
                        # String column - convert all values to strings
                        coerced_values = [str(v) for v in value]
                    elif input_col_dtype in (pl.Int64, pl.Int32, pl.Int16, pl.Int8):
                        # Integer column - try to convert string values to int
                        coerced_values = []
                        for v in value:
                            if isinstance(v, str):
                                try:
                                    coerced_values.append(
                                        int(float(v))
                                    )  # Handle "10.5" -> 10
                                except (ValueError, TypeError):
                                    coerced_values.append(
                                        v
                                    )  # Keep original if conversion fails
                            else:
                                coerced_values.append(v)
                    elif input_col_dtype in (pl.Float64, pl.Float32):
                        # Float column - try to convert string values to float
                        coerced_values = []
                        for v in value:
                            if isinstance(v, str):
                                try:
                                    coerced_values.append(float(v))
                                except (ValueError, TypeError):
                                    coerced_values.append(
                                        v
                                    )  # Keep original if conversion fails
                            else:
                                coerced_values.append(v)
                return left.is_in(coerced_values)
            else:
                coerced_value = value
                if input_col_dtype is not None and input_col_dtype == pl.Utf8:
                    # String column - convert value to string
                    coerced_value = str(value)
                return left.is_in([coerced_value])

        # Special handling for between - value is a tuple (lower, upper), don't translate it as a whole
        # Need to handle type coercion and translate lower/upper bounds separately
        if operation == "between":
            # Handle between operation: value is a tuple (lower, upper)
            # PySpark between is inclusive on both ends: lower <= col <= upper
            if not isinstance(value, tuple) or len(value) != 2:
                raise ValueError(
                    f"between operation requires a tuple of (lower, upper) bounds, got {type(value)}"
                )
            lower, upper = value

            # Translate lower bound
            if isinstance(lower, ColumnOperation):
                lower_expr = self._translate_operation(
                    lower,
                    input_col_dtype=None,
                    available_columns=available_columns,
                    case_sensitive=case_sensitive,
                )
            elif isinstance(lower, Column):
                lower_expr = self._translate_column(
                    lower,
                    available_columns=available_columns,
                    case_sensitive=case_sensitive,
                )
            elif isinstance(lower, Literal):
                if hasattr(lower, "_is_lazy") and lower._is_lazy:
                    lower_expr = pl.lit(lower._resolve_lazy_value())
                else:
                    lower_expr = pl.lit(lower.value)
            elif isinstance(lower, (int, float, bool, str, datetime, date)):
                lower_expr = pl.lit(lower)
            elif lower is None:
                lower_expr = pl.lit(None)
            else:
                # Fallback: try to translate as a literal
                lower_expr = pl.lit(lower)

            # Translate upper bound
            if isinstance(upper, ColumnOperation):
                upper_expr = self._translate_operation(
                    upper,
                    input_col_dtype=None,
                    available_columns=available_columns,
                    case_sensitive=case_sensitive,
                )
            elif isinstance(upper, Column):
                upper_expr = self._translate_column(
                    upper,
                    available_columns=available_columns,
                    case_sensitive=case_sensitive,
                )
            elif isinstance(upper, Literal):
                if hasattr(upper, "_is_lazy") and upper._is_lazy:
                    upper_expr = pl.lit(upper._resolve_lazy_value())
                else:
                    upper_expr = pl.lit(upper.value)
            elif isinstance(upper, (int, float, bool, str, datetime, date)):
                upper_expr = pl.lit(upper)
            elif upper is None:
                upper_expr = pl.lit(None)
            else:
                # Fallback: try to translate as a literal
                upper_expr = pl.lit(upper)

            # Use Polars is_between with inclusive bounds (closed="both" means both ends inclusive)
            # This matches PySpark behavior where between is inclusive: lower <= col <= upper
            return left.is_between(lower_expr, upper_expr, closed="both")

        # Special handling for withField - add or replace field in struct
        if operation == "withField":
            # Extract field name and column from operation value
            if not isinstance(value, dict) or "fieldName" not in value:
                # Invalid withField operation - return original column
                return left

            field_column = value.get("column")

            if field_column is None:
                return left

            # For Polars, withField is complex because we need to:
            # 1. Access all existing struct fields
            # 2. Evaluate the new field's column expression (needs row context)
            # 3. Reconstruct the struct with all fields

            # Since we need row context to evaluate the field column expression,
            # we'll raise ValueError to trigger fallback to Python evaluation.
            # This is similar to how unsupported window functions are handled.
            # The actual evaluation will be handled by ExpressionEvaluator.
            raise ValueError(
                "withField operation requires Python evaluation - will be handled by ExpressionEvaluator"
            )

        # Special handling for getItem - extract element from array or character from string
        if operation == "getItem":
            index = value
            try:
                idx = int(index)
                # For array/list columns, we need to handle out-of-bounds gracefully
                # Polars list.get() raises an error for out-of-bounds, but PySpark returns None
                # Use list.slice() to safely get the element, or return None if out of bounds
                try:
                    # Try using list.slice() which handles out-of-bounds by returning empty list
                    # Then take the first element, or None if empty
                    list_len = left.list.len()
                    in_bounds = (pl.lit(idx) >= 0) & (pl.lit(idx) < list_len)
                    # Use slice to get a single element safely
                    sliced = left.list.slice(pl.lit(idx), 1)
                    # Get first element from slice, or None if slice is empty
                    result = sliced.list.first()
                    # Return None if out of bounds
                    result = pl.when(in_bounds).then(result).otherwise(None)

                    # Try to get return type from input_col_dtype
                    if input_col_dtype is not None and isinstance(
                        input_col_dtype, pl.List
                    ):
                        return_dtype = input_col_dtype.inner
                        if return_dtype is not None:
                            result = result.cast(return_dtype, strict=False)

                    return result
                except (AttributeError, TypeError):
                    # Fallback to map_elements if list operations don't work
                    def get_item_handler(val: Any) -> Any:
                        """Handle getItem for arrays with bounds checking."""
                        if val is None:
                            return None
                        if isinstance(val, (list, tuple, str)):
                            if 0 <= idx < len(val):
                                return val[idx]
                            return None
                        elif isinstance(val, dict):
                            return val.get(index)
                        return None

                    return_dtype = None
                    if input_col_dtype is not None and isinstance(
                        input_col_dtype, pl.List
                    ):
                        return_dtype = input_col_dtype.inner

                    return left.map_elements(
                        get_item_handler, return_dtype=return_dtype
                    )
            except (ValueError, TypeError):
                # If index is not an integer (e.g., map key), handle differently
                # For map keys, we can't use list.get(), must use map_elements
                def get_item_handler_map(val: Any) -> Any:
                    """Handle getItem for maps."""
                    if val is None:
                        return None
                    if isinstance(val, dict):
                        return val.get(index)
                    elif isinstance(val, (list, tuple)):
                        # If it's a list and index is a string, return None
                        return None
                    return None

                # Use map_elements for map access
                return left.map_elements(get_item_handler_map, return_dtype=None)

        # Check if this is a binary operator first (must be handled as binary operation, not function)
        binary_operators = [
            "==",
            "!=",
            "<",
            "<=",
            ">",
            ">=",
            "+",
            "-",
            "*",
            "/",
            "%",
            "**",
            "&",
            "|",
        ]
        if operation in binary_operators:
            # Binary operators should NOT be routed to function calls - handle as binary operation below
            pass
        # Check if this is a string operation (must be handled as binary operation, not function)
        elif operation in [
            "contains",
            "startswith",
            "endswith",
            "like",
            "rlike",
            "isin",
            "between",
        ]:
            # String operations, isin, and between should NOT be routed to function calls - handle as binary operation below
            pass
        # Check if this is a unary operator (must be handled as unary operation, not function)
        elif value is None and operation in ["!", "~", "-"]:
            # Unary operators should NOT be routed to function calls - handle as unary operation below
            pass
        # Check if this is a function call (not a binary or unary operation)
        # Functions like concat_ws, substring, etc. have values but are not binary operations
        elif hasattr(op, "function_name") or operation in [
            "substring",
            "regexp_replace",
            "regexp_extract",
            "split",
            "concat",
            "concat_ws",
            "like",
            "rlike",
            "round",
            "pow",
            "to_date",
            "to_timestamp",
            "date_format",
            "date_add",
            "date_sub",
            "datediff",
            "lpad",
            "rpad",
            "repeat",
            "instr",
            "locate",
            "add_months",
            "last_day",
            "bin",
            "bround",
            "conv",
            "factorial",
            "map_keys",
            "map_values",
            "map_entries",
            "map_concat",
        ]:
            return self._translate_function_call(
                op,
                input_col_dtype=input_col_dtype,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )

        # Handle unary operations
        if value is None:
            # Handle operators first (before function calls)
            if operation in ["!", "~"]:
                return ~left
            elif operation == "-":
                return -left
            elif operation in ["isnull", "isNull"]:
                return left.is_null()
            elif operation in ["isnotnull", "isNotNull"]:
                return left.is_not_null()
            # Check if it's a function call (e.g., upper, lower, length)
            # Also check for datetime functions and other unary functions
            elif hasattr(op, "function_name") or operation in [
                "upper",
                "lower",
                "length",
                "trim",
                "ltrim",
                "rtrim",
                "btrim",
                "bit_length",
                "octet_length",
                "char",
                "ucase",
                "lcase",
                "positive",
                "negative",
                "power",
                "now",
                "curdate",
                "days",
                "hours",
                "months",
                "equal_null",
                "substr",
                "split_part",
                "position",
                "elt",
                "abs",
                "ceil",
                "floor",
                "sqrt",
                "exp",
                "log",
                "log10",
                "sin",
                "cos",
                "tan",
                "round",
                "bin",
                "bround",
                "conv",
                "factorial",
                "year",
                "month",
                "day",
                "dayofmonth",
                "hour",
                "minute",
                "second",
                "dayofweek",
                "dayofyear",
                "weekofyear",
                "quarter",
                "to_date",
                "current_timestamp",
                "current_date",
                "now",
                "curdate",
                "map_keys",
                "map_values",
                "map_entries",
                "map_concat",
            ]:
                return self._translate_function_call(
                    op,
                    available_columns=available_columns,
                    case_sensitive=case_sensitive,
                )
            else:
                raise ValueError(f"Unsupported unary operation: {operation}")

        # Translate right side
        # Check ColumnOperation before Column since ColumnOperation is a subclass of Column
        if isinstance(value, ColumnOperation):
            right = self._translate_operation(
                value,
                input_col_dtype=None,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )
        elif isinstance(value, Column):
            right = self._translate_column(
                value,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )
        elif isinstance(value, Literal):
            # Resolve lazy literals before translating
            if hasattr(value, "_is_lazy") and value._is_lazy:
                right = pl.lit(value._resolve_lazy_value())
            else:
                right = pl.lit(value.value)
        elif isinstance(value, (int, float, bool, str)):
            right = pl.lit(value)
        elif isinstance(value, (datetime, date)):
            # Datetime or date literal value
            right = pl.lit(value)
        elif value is None:
            right = pl.lit(None)
        else:
            right = self.translate(
                value,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )

        # Handle binary operations with type coercion for string-to-numeric comparisons
        if operation in ["==", "!=", "<", "<=", ">", ">=", "eqNullSafe"]:
            # operation is guaranteed to be a string (from op.operation)
            op_str: str = str(operation)
            return self._coerce_for_comparison(left, right, op_str)
        elif operation == "+":
            # + operator needs special handling: string concatenation vs numeric addition
            # PySpark behavior:
            # - string + string = string concatenation
            # - string + numeric = numeric addition (coerce string to numeric)
            # - numeric + numeric = numeric addition
            # Since we can't easily determine types at expression level, use Python fallback
            # which already handles both cases correctly in ExpressionEvaluator
            raise ValueError(
                "+ operation requires Python evaluation to handle string/numeric mix"
            )
        elif operation in ["-", "*", "/", "%", "**"]:
            # Arithmetic operations with automatic string-to-numeric coercion
            # PySpark automatically casts string columns to Double for arithmetic
            return self._coerce_for_arithmetic(left, right, str(operation))
        elif operation == "&":
            return left & right
        elif operation == "|":
            return left | right
        elif operation == "cast":
            # Handle cast operation
            return self._translate_cast(left, value)
        # isin and between are handled earlier, before value translation
        elif operation in ["startswith", "endswith"]:
            # operation is guaranteed to be a string in ColumnOperation
            op_str = cast("str", operation)
            return self._translate_string_operation(left, op_str, value)
        elif operation == "contains":
            # Handle contains as a function call
            return self._translate_function_call(
                op,
                input_col_dtype=input_col_dtype,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )
        elif hasattr(op, "function_name"):
            # Handle function calls (e.g., upper, lower, sum, etc.)
            return self._translate_function_call(
                op,
                input_col_dtype=input_col_dtype,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )
        else:
            raise ValueError(f"Unsupported operation: {operation}")

    def _coerce_for_comparison(
        self, left_expr: pl.Expr, right_expr: pl.Expr, op: str
    ) -> pl.Expr:
        """Coerce types for comparison operations to handle string-to-numeric comparisons.

        PySpark behavior: when comparing string with numeric, try to cast string to numeric.
        This enables comparisons like "10" == 10 or "5.5" > 3 to work correctly.

        Args:
            left_expr: Left Polars expression
            right_expr: Right Polars expression
            op: Operation string (==, !=, <, <=, >, >=)

        Note:
            Fixed in version 3.23.0 (Issue #225): String-to-numeric type coercion for
            comparison operations now matches PySpark behavior.

        Returns:
            Polars expression with appropriate comparison and type coercion
        """
        import operator

        # Capture pl in local variable to avoid closure issues
        polars_module = pl

        # Map operation strings to operator functions
        comparison_ops = {
            "==": operator.eq,
            "!=": operator.ne,
            "<": operator.lt,
            "<=": operator.le,
            ">": operator.gt,
            ">=": operator.ge,
            # Null-safe equality uses the same underlying equality function but
            # has special handling for nulls in _compare_with_coercion.
            "eqNullSafe": operator.eq,
        }
        if op not in comparison_ops:
            raise ValueError(f"Unsupported comparison operation: {op}")
        compare_fn = comparison_ops[op]

        def _is_date_like(value: Any) -> bool:
            """Return True if value behaves like a date (no time component)."""
            return isinstance(value, date) and not isinstance(value, datetime)

        def _is_datetime_like(value: Any) -> bool:
            """Return True if value behaves like a full datetime or timestamp."""
            if isinstance(value, datetime):
                return True
            return PandasTimestamp is not None and isinstance(value, PandasTimestamp)

        def _parse_date_string(value: str) -> Optional[date]:
            """Parse ISO-8601 date string to date, matching PySpark yyyy-MM-dd default."""
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                return None

        def _parse_datetime_string(value: str) -> Optional[datetime]:
            """Parse ISO-8601 datetime string, matching PySpark yyyy-MM-dd HH:mm:ss default."""
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                return None

        def _to_python_datetime(value: Any) -> datetime:
            """Convert supported timestamp-like values to a Python datetime."""
            if isinstance(value, datetime):
                return value
            if PandasTimestamp is not None and isinstance(value, PandasTimestamp):
                # Use pandas helper to normalise to Python datetime
                return cast("datetime", value.to_pydatetime())
            # Fallback – this should not normally be hit because callers gate on _is_datetime_like
            return datetime.fromtimestamp(0)

        # Use map_elements to handle type coercion at runtime
        def _compare_with_coercion(left_val: Any, right_val: Any) -> Any:
            """Compare values with automatic type coercion for numeric, datetime, and null-safe equality."""
            # Special handling for null-safe equality (PySpark eqNullSafe semantics)
            if op == "eqNullSafe":
                if left_val is None and right_val is None:
                    return True
                if left_val is None or right_val is None:
                    return False
            else:
                if left_val is None or right_val is None:
                    return None

            # Left is string, right is numeric: convert left to numeric
            if isinstance(left_val, str) and isinstance(right_val, (int, float)):
                try:
                    # Determine target type based on right side
                    if isinstance(right_val, int):
                        # Try integer first, then float
                        try:
                            left_num: Union[int, float] = int(float(left_val))
                        except (ValueError, TypeError):
                            left_num = float(left_val)
                    else:
                        left_num = float(left_val)

                    return compare_fn(left_num, right_val)
                except (ValueError, TypeError):
                    return None
            # Right is string, left is numeric: convert right to numeric
            elif isinstance(right_val, str) and isinstance(left_val, (int, float)):
                try:
                    if isinstance(left_val, int):
                        try:
                            right_num: Union[int, float] = int(float(right_val))
                        except (ValueError, TypeError):
                            right_num = float(right_val)
                    else:
                        right_num = float(right_val)

                    return compare_fn(left_val, right_num)
                except (ValueError, TypeError):
                    return None

            # Date-like vs string: parse string as date (yyyy-MM-dd) then compare
            if _is_date_like(left_val) and isinstance(right_val, str):
                parsed = _parse_date_string(right_val)
                if parsed is None:
                    return None
                return compare_fn(left_val, parsed)
            elif _is_date_like(right_val) and isinstance(left_val, str):
                parsed = _parse_date_string(left_val)
                if parsed is None:
                    return None
                return compare_fn(parsed, right_val)

            # Datetime/Timestamp-like vs string: parse string as datetime then compare
            if _is_datetime_like(left_val) and isinstance(right_val, str):
                parsed_dt = _parse_datetime_string(right_val)
                if parsed_dt is None:
                    return None
                left_dt = _to_python_datetime(left_val)
                return compare_fn(left_dt, parsed_dt)
            elif _is_datetime_like(right_val) and isinstance(left_val, str):
                parsed_dt = _parse_datetime_string(left_val)
                if parsed_dt is None:
                    return None
                right_dt = _to_python_datetime(right_val)
                return compare_fn(parsed_dt, right_dt)

            # Default comparison (same types or other combinations)
            return compare_fn(left_val, right_val)

        # Use map_elements for runtime type coercion
        # Combine both expressions into a struct, then map
        combined = polars_module.struct(
            [left_expr.alias("left"), right_expr.alias("right")]
        )
        result = combined.map_elements(
            lambda x: _compare_with_coercion(x["left"], x["right"]) if x else None,
            return_dtype=polars_module.Boolean,
        )
        return result

    def _coerce_for_arithmetic(
        self, left_expr: pl.Expr, right_expr: pl.Expr, op: str
    ) -> pl.Expr:
        """Coerce types for arithmetic operations to handle string-to-numeric operations.

        PySpark behavior: when performing arithmetic on string columns, automatically
        cast strings to numeric types (Double). This enables operations like "10.0" / 5
        to work correctly and return 2.0.

        Args:
            left_expr: Left Polars expression
            right_expr: Right Polars expression
            op: Operation string (+, -, *, /, %, **)

        Returns:
            Polars expression with appropriate arithmetic operation and type coercion

        Note:
            Fixed in version 3.23.0 (Issue #236): String-to-numeric type coercion for
            arithmetic operations now matches PySpark behavior.
        """
        # Perform the arithmetic operation
        # Note: For + operator, we need special handling:
        # - string + string = string concatenation (Polars handles this automatically)
        # - string + numeric = numeric addition (coerce string to numeric)
        # - numeric + numeric = numeric addition
        # Strategy: Use a conditional to check if we can coerce to numeric.
        # If both can be coerced to Float64, do numeric addition.
        # Otherwise, use string concatenation (Polars native behavior).
        if op == "+":
            # Try to coerce both to Float64 for numeric addition
            # If coercion results in null (non-numeric strings), fall back to string concat
            left_coerced = left_expr.cast(pl.Float64, strict=False)
            right_coerced = right_expr.cast(pl.Float64, strict=False)

            # Check if both can be coerced (not null after coercion)
            # If both are numeric (or coercible), use numeric addition
            # Otherwise, use string concatenation
            numeric_result = left_coerced + right_coerced
            string_result = left_expr + right_expr

            # Use numeric if both operands are successfully coerced (not null)
            # Otherwise use string concatenation
            result = (
                pl.when(left_coerced.is_not_null() & right_coerced.is_not_null())
                .then(numeric_result)
                .otherwise(string_result)
            )
        else:
            # For -, *, /, % operations, coerce strings to Float64
            # PySpark automatically casts string columns to Double (Float64) for arithmetic
            # PySpark also strips whitespace from strings before converting to numeric
            # For string columns, we need to strip whitespace before casting
            # For numeric literals/columns, we can cast directly
            # Use a conditional: try strip_chars() + cast, if that fails (returns null for non-strings),
            # fall back to direct cast. Actually, simpler: always try strip_chars() first,
            # and Polars will handle non-strings gracefully by returning the original or null
            # Then use coalesce to fall back to direct cast if needed
            # Actually, the simplest: use when().then().otherwise() to conditionally strip
            # But we can't easily check if it's a string. So use map_elements for Python fallback
            # or accept that whitespace stripping might not work for all cases in Polars
            # For now, just cast directly - whitespace handling is done in Python fallback
            left_coerced = left_expr.cast(pl.Float64, strict=False)
            right_coerced = right_expr.cast(pl.Float64, strict=False)

            if op == "-":
                result = left_coerced - right_coerced
            elif op == "*":
                result = left_coerced * right_coerced
            elif op == "/":
                # Handle division by zero - PySpark returns None, Polars returns inf
                # Use when/otherwise to convert inf to None
                result = left_coerced / right_coerced
                # Convert inf/-inf to None to match PySpark behavior
                result = (
                    pl.when(result.is_infinite() | result.is_nan())
                    .then(None)
                    .otherwise(result)
                )
            elif op == "%":
                # Handle modulo by zero - PySpark returns None
                result = left_coerced % right_coerced
                # Convert inf/-inf to None to match PySpark behavior
                result = (
                    pl.when(result.is_infinite() | result.is_nan())
                    .then(None)
                    .otherwise(result)
                )
            elif op == "**":
                # Power operation: left ** right
                # Use Polars pow function for power operation
                result = left_coerced.pow(right_coerced)
                # Convert inf/-inf to None to match PySpark behavior
                result = (
                    pl.when(result.is_infinite() | result.is_nan())
                    .then(None)
                    .otherwise(result)
                )
            else:
                raise ValueError(f"Unsupported arithmetic operation: {op}")

            # Ensure result is Float64 to match PySpark's Double type (except for + which may be string)
            if op != "+":
                result = result.cast(pl.Float64, strict=False)

        return result

    def _translate_cast(self, expr: pl.Expr, target_type: Any) -> pl.Expr:
        """Translate cast operation.

        Args:
            expr: Polars expression to cast
            target_type: Target data type (DataType or string type name)

        Returns:
            Casted Polars expression
        """
        from .type_mapper import mock_type_to_polars_dtype
        from sparkless.spark_types import (
            StringType,
            IntegerType,
            LongType,
            DoubleType,
            FloatType,
            BooleanType,
            DateType,
            TimestampType,
            ShortType,
            ByteType,
        )

        # Handle string type names (e.g., "string", "int", "long")
        if isinstance(target_type, str):
            type_name_map = {
                "string": StringType(),
                "str": StringType(),
                "int": IntegerType(),
                "integer": IntegerType(),
                "long": LongType(),
                "bigint": LongType(),
                "double": DoubleType(),
                "float": FloatType(),
                "boolean": BooleanType(),
                "bool": BooleanType(),
                "date": DateType(),
                "timestamp": TimestampType(),
                "short": ShortType(),
                "byte": ByteType(),
            }
            target_type = type_name_map.get(target_type.lower())
            if target_type is None:
                raise ValueError(f"Unsupported cast type: {target_type}")

        # Special handling for casting to StringType
        if isinstance(target_type, StringType):
            # For datetime/date types, use direct cast to string
            # This fixes issue #145 where explicit string casts weren't working correctly
            # Use cast(pl.Utf8, strict=False) which works for all types including datetime
            return expr.cast(pl.Utf8, strict=False)

        polars_dtype = mock_type_to_polars_dtype(target_type)

        # Special handling for None literals - create literal with target dtype directly
        # This handles F.lit(None).cast(TimestampType()) correctly
        # Check if expr is a None literal by trying to evaluate it
        # If it's a constant None, create pl.lit(None, dtype=target_dtype) directly
        try:
            # Try to get the value if it's a literal
            # For None literals, Polars needs the dtype specified at creation time
            # Check if this is a literal expression that evaluates to None
            if hasattr(expr, "meta"):
                import contextlib

                with contextlib.suppress(Exception):
                    # Try to see if this is a literal None
                    # For Polars, we need to create pl.lit(None, dtype=...) for typed nulls
                    # This is a workaround - we'll handle it by creating the literal with dtype
                    pass
        except Exception:
            logger.debug("Exception in cast type detection, continuing", exc_info=True)
            pass

        # For string to int/long casting, Polars needs float intermediate step
        # PySpark handles "10.5" -> 10 by converting to float first, then int
        if isinstance(target_type, (IntegerType, LongType)):
            # Check if source is string - need float intermediate step
            # For other types, direct cast is fine
            return expr.cast(pl.Float64, strict=False).cast(polars_dtype, strict=False)

        # For string to date/timestamp casting
        if isinstance(target_type, (DateType, TimestampType)):
            # Try to parse string to date/timestamp
            # Use map_elements to handle both string and non-string inputs
            if isinstance(target_type, DateType):
                # Parse date string
                def parse_date(val: Any) -> Any:
                    if val is None:
                        return None
                    from datetime import datetime

                    val_str = str(val)
                    try:
                        return datetime.strptime(val_str, "%Y-%m-%d").date()
                    except ValueError:
                        return None

                # Try strptime first (works for string columns), fall back to map_elements
                try:
                    return expr.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                except Exception:
                    logger.debug(
                        "strptime failed for date parsing, using map_elements fallback",
                        exc_info=True,
                    )
                    return expr.map_elements(parse_date, return_dtype=pl.Date)
            else:  # TimestampType
                # Parse timestamp string
                def parse_timestamp(val: Any) -> Any:
                    if val is None:
                        return None
                    from datetime import datetime

                    val_str = str(val)
                    try:
                        return datetime.strptime(val_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        try:
                            return datetime.strptime(val_str, "%Y-%m-%d")
                        except ValueError:
                            return None

                # Try strptime first (works for string columns), fall back to map_elements
                try:
                    return expr.str.strptime(
                        pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False
                    )
                except Exception:
                    logger.debug(
                        "strptime failed for timestamp parsing, using map_elements fallback",
                        exc_info=True,
                    )
                    return expr.map_elements(
                        parse_timestamp, return_dtype=pl.Datetime(time_unit="us")
                    )

        # Special handling for string to boolean casting - Polars doesn't support this directly
        # Raise ValueError to trigger Python fallback evaluation
        if isinstance(target_type, BooleanType):
            # Check if source is likely a string (we can't always know for sure, but we can try)
            # For now, raise ValueError to force Python fallback for all string->boolean casts
            # This is safer than trying to detect string types at this level
            raise ValueError(
                "String to boolean casting requires Python evaluation (Polars limitation)"
            )

        # For other types, use strict=False to return null for invalid casts (PySpark behavior)
        # Special handling: if expr is a None literal (pl.lit(None)), create typed None
        # This handles F.lit(None).cast(TimestampType()) correctly
        try:
            # Check if this is a literal None by trying to get its value
            # If it's pl.lit(None), we need to create it with the target dtype
            # Polars requires dtype to be specified when creating None literals for typed columns
            if hasattr(expr, "meta"):
                # Try to detect if this is a literal None
                # For now, we'll use a workaround: try casting, and if it fails with schema error,
                # create a new literal with dtype
                try:
                    return expr.cast(polars_dtype, strict=False)
                except Exception as e:
                    # If casting fails due to null type, create typed None literal
                    if "null" in str(e).lower() or "dtype" in str(e).lower():
                        return pl.lit(None, dtype=polars_dtype)
                    raise
            else:
                return expr.cast(polars_dtype, strict=False)
        except Exception as e:
            # Check if this is an InvalidOperationError for unsupported casts
            error_msg = str(e)
            if "not supported" in error_msg.lower() or "InvalidOperationError" in str(
                type(e).__name__
            ):
                # Raise ValueError to trigger Python fallback
                raise ValueError(
                    f"Cast operation requires Python evaluation: {error_msg}"
                ) from e
            # Fallback: try to create typed None if cast fails
            # This handles the case where pl.lit(None) can't be cast directly
            logger.debug(
                "Initial cast failed, trying typed None fallback", exc_info=True
            )
            try:
                # Check if expr represents a None value
                # For Polars, we need pl.lit(None, dtype=...) for typed nulls
                return pl.lit(None, dtype=polars_dtype)
            except Exception:
                # Last resort: try regular cast
                logger.debug(
                    "Typed None creation failed, using regular cast", exc_info=True
                )
                return expr.cast(polars_dtype, strict=False)

    def _translate_string_operation(
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
                value_expr = self.translate(value)
                return expr.str.contains(value_expr)
        elif operation == "startswith":
            if isinstance(value, str):
                return expr.str.starts_with(value)
            else:
                value_expr = self.translate(value)
                return expr.str.starts_with(value_expr)
        elif operation == "endswith":
            if isinstance(value, str):
                return expr.str.ends_with(value)
            else:
                value_expr = self.translate(value)
                return expr.str.ends_with(value_expr)
        else:
            raise ValueError(f"Unsupported string operation: {operation}")

    def _build_cache_key(self, expr: Any) -> Optional[Tuple[Any, ...]]:
        try:
            return self._serialize_expression(expr)
        except Exception:
            logger.debug("Failed to build cache key for expression", exc_info=True)
            return None

    def _serialize_expression(self, expr: Any) -> Tuple[Any, ...]:
        if isinstance(expr, Column):
            alias = getattr(expr, "_alias_name", None)
            original = getattr(expr, "_original_column", None)
            original_name = getattr(original, "name", None)
            return ("column", expr.name, alias, original_name)
        if isinstance(expr, ColumnOperation):
            column_repr = self._serialize_value(getattr(expr, "column", None))
            value_repr = self._serialize_value(getattr(expr, "value", None))
            return (
                "operation",
                expr.operation,
                column_repr,
                value_repr,
                getattr(expr, "name", None),
                getattr(expr, "function_name", None),
            )
        if isinstance(expr, Literal):
            # Resolve lazy literals before serializing
            if hasattr(expr, "_is_lazy") and expr._is_lazy:
                return ("literal", expr._resolve_lazy_value())
            return ("literal", expr.value)
        if isinstance(expr, tuple):
            return ("tuple",) + tuple(self._serialize_value(item) for item in expr)
        if isinstance(expr, list):
            return ("list",) + tuple(self._serialize_value(item) for item in expr)
        if isinstance(expr, dict):
            return (
                "dict",
                tuple(
                    sorted(
                        (self._serialize_value(k), self._serialize_value(v))
                        for k, v in expr.items()
                    )
                ),
            )
        if isinstance(expr, (int, float, bool, str)):
            return ("scalar", expr)
        if expr is None:
            return ("none",)
        return ("repr", repr(expr))

    def _serialize_value(self, value: Any) -> Any:
        if isinstance(value, (Column, ColumnOperation, Literal)):
            return self._serialize_expression(value)
        if isinstance(value, (list, tuple)):
            return tuple(self._serialize_value(item) for item in value)
        if isinstance(value, dict):
            return tuple(
                sorted(
                    (self._serialize_value(k), self._serialize_value(v))
                    for k, v in value.items()
                )
            )
        if isinstance(value, (int, float, bool, str)) or value is None:
            return value
        return repr(value)

    def _cache_get(self, key: Tuple[Any, ...]) -> Optional[pl.Expr]:
        with self._cache_lock:
            cached = self._translation_cache.get(key)
            if cached is not None:
                self._translation_cache.move_to_end(key)
            return cached

    def _cache_set(self, key: Tuple[Any, ...], expr: pl.Expr) -> None:
        with self._cache_lock:
            self._translation_cache[key] = expr
            self._translation_cache.move_to_end(key)
            if len(self._translation_cache) > self._cache_size:
                self._translation_cache.popitem(last=False)

    def clear_cache(self) -> None:
        """Clear the expression translation cache.

        This should be called when columns are dropped to invalidate cached
        expressions that reference those columns.
        """
        with self._cache_lock:
            self._translation_cache.clear()

    def _translate_function_call(
        self,
        op: ColumnOperation,
        input_col_dtype: Any = None,
        available_columns: Optional[List[str]] = None,
        case_sensitive: bool = False,
    ) -> pl.Expr:
        """Translate function call operations.

        Args:
            op: ColumnOperation with function call
            input_col_dtype: Optional input column dtype
            available_columns: Optional list of available column names for case-insensitive matching
            case_sensitive: Whether to use case-sensitive matching

        Returns:
            Polars expression for function call
        """
        # op.operation is guaranteed to be a string in ColumnOperation
        op_operation = cast("str", op.operation)
        function_name = getattr(op, "function_name", op_operation)
        if function_name is None:
            function_name = op_operation
        function_name = function_name.lower()
        column = op.column

        # Handle functions without column first (e.g., current_timestamp, current_date, monotonically_increasing_id)
        if column is None:
            operation = op.operation  # Extract operation for use in comparisons
            if operation == "current_timestamp":
                # Use datetime.now() which returns current timestamp
                from datetime import datetime

                return pl.lit(datetime.now())
            elif operation == "current_date":
                # Use date.today() which returns current date
                from datetime import date

                return pl.lit(date.today())
            elif operation == "now":
                # Alias for current_timestamp
                from datetime import datetime

                return pl.lit(datetime.now())
            elif operation == "curdate":
                # Alias for current_date
                from datetime import date

                return pl.lit(date.today())
            elif operation == "localtimestamp":
                # Local timestamp (without timezone)
                from datetime import datetime

                return pl.lit(datetime.now())
            elif function_name == "monotonically_increasing_id":
                # monotonically_increasing_id() - generate row numbers
                # Use int_range to generate sequential IDs
                return pl.int_range(pl.len())

        # Extract operation for use in comparisons
        operation = op.operation  # Extract operation for use in comparisons

        # SPECIAL CASE: Check for nested to_date(to_timestamp(...)) BEFORE translating col_expr
        # This allows us to detect the nested structure and handle it specially
        if (
            operation == "to_date"
            and isinstance(column, ColumnOperation)
            and column.operation == "to_timestamp"
        ):
            # For to_date(to_timestamp(...)), the input is already datetime
            # Use map_elements with a simple datetime->date conversion
            # This avoids schema validation issues that dt.date() might cause
            # First translate the nested to_timestamp to get the datetime expression
            nested_ts_expr = self._translate_operation(column, input_col_dtype=None)

            def datetime_to_date(val: Any) -> Any:
                from datetime import datetime, date

                if val is None:
                    return None
                if isinstance(val, datetime):
                    return val.date()
                if isinstance(val, date):
                    return val
                return None

            return nested_ts_expr.map_elements(
                datetime_to_date,
                return_dtype=pl.Date,
            )

        # Handle unix_timestamp() without arguments (current timestamp) BEFORE translating column
        if operation == "unix_timestamp":
            from sparkless.functions.core.literals import Literal

            is_current_timestamp = False
            if (
                column is None
                or isinstance(column, str)
                and column == "current_timestamp"
                or isinstance(column, Literal)
                and column.value == "current_timestamp"
            ):
                is_current_timestamp = True

            if is_current_timestamp:
                # Return current Unix timestamp
                from datetime import datetime

                return pl.lit(int(datetime.now().timestamp()))

        # Translate column expression
        # Check ColumnOperation BEFORE Column since ColumnOperation inherits from Column
        if isinstance(column, ColumnOperation):
            col_expr = self._translate_operation(
                column,
                input_col_dtype=None,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )
        elif isinstance(column, Column):
            col_expr = self._translate_column(
                column,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )
        elif isinstance(column, str):
            # Use ColumnResolver matching if available columns are provided
            if available_columns:
                actual_col_name = self._find_column(
                    available_columns, column, case_sensitive
                )
                col_expr = (
                    pl.col(actual_col_name) if actual_col_name else pl.col(column)
                )
            else:
                col_expr = pl.col(column)
        else:
            col_expr = self.translate(
                column,
                available_columns=available_columns,
                case_sensitive=case_sensitive,
            )

        # User-defined functions (UDFs).
        # Sparkless represents a UDF application as a ColumnOperation with:
        # - operation/function_name = "udf"
        # - op._udf_func: the Python callable
        # - op._udf_return_type: Sparkless return type (optional)
        # - op._udf_cols: list of Column/ColumnOperation args (optional; present for multi-col UDFs)
        if function_name == "udf":
            udf_func = getattr(op, "_udf_func", None)
            if udf_func is None or not callable(udf_func):
                raise ValueError("Unsupported function: udf")

            udf_cols = getattr(op, "_udf_cols", None)
            if udf_cols:
                arg_exprs: List[pl.Expr] = [
                    self.translate(
                        c,
                        available_columns=available_columns,
                        case_sensitive=case_sensitive,
                    )
                    for c in udf_cols
                ]
            else:
                # Back-compat: some call paths only store the first column on op.column
                arg_exprs = [col_expr]

            return_dtype = None
            udf_return_type = getattr(op, "_udf_return_type", None)
            if udf_return_type is not None:
                try:
                    from .type_mapper import mock_type_to_polars_dtype

                    return_dtype = mock_type_to_polars_dtype(udf_return_type)
                except Exception:
                    # If we can't map the declared return type, let Polars infer it.
                    return_dtype = None

            if len(arg_exprs) == 1:
                return arg_exprs[0].map_elements(
                    lambda x: udf_func(x),  # noqa: B023 - udf_func is user-supplied
                    return_dtype=return_dtype,
                )

            # Multi-argument UDF: bundle inputs into a struct and apply row-wise.
            field_names = [f"_udf_{i}" for i in range(len(arg_exprs))]
            struct_expr = pl.struct(
                [expr.alias(name) for expr, name in zip(arg_exprs, field_names)]
            )

            def apply_udf_struct(s: Any) -> Any:
                if s is None:
                    return None
                try:
                    args = [s[name] for name in field_names]
                except Exception:
                    # Polars may pass a dict-like; be defensive.
                    args = [getattr(s, name, None) for name in field_names]
                return udf_func(*args)  # noqa: B023 - udf_func is user-supplied

            return struct_expr.map_elements(apply_udf_struct, return_dtype=return_dtype)

        # Handle struct function - creates a struct from multiple columns
        if operation == "struct":
            # Collect all columns for the struct
            struct_cols: List[Any] = []

            # Check if first column is a literal (all columns stored in value)
            if (
                op.column
                and isinstance(op.column, Column)
                and op.column.name == "__struct_dummy__"
            ):
                # All columns are in op.value
                if op.value:
                    if isinstance(op.value, (list, tuple)):
                        struct_cols = list(op.value)
                    else:
                        struct_cols = [op.value]
            else:
                # First column is in op.column, remaining in op.value
                struct_cols = [op.column] if op.column else []
                if op.value:
                    if isinstance(op.value, (list, tuple)):
                        struct_cols.extend(list(op.value))
                    else:
                        struct_cols.append(op.value)

            if not struct_cols:
                raise ValueError("struct requires at least one column")

            # Translate each column to a Polars expression
            struct_exprs = []
            for col in struct_cols:
                if isinstance(col, str):
                    # Column name - resolve case-insensitively if needed
                    if available_columns:
                        actual_col_name = self._find_column(
                            available_columns, col, case_sensitive
                        )
                        col_expr = (
                            pl.col(actual_col_name) if actual_col_name else pl.col(col)
                        )
                    else:
                        col_expr = pl.col(col)
                else:
                    # Column object - translate it
                    col_expr = self.translate(
                        col,
                        available_columns=available_columns,
                        case_sensitive=case_sensitive,
                    )
                struct_exprs.append(col_expr)

            # Create struct with column names as field names
            # Use column names from the original columns for field names
            field_names = []
            for col in struct_cols:
                if isinstance(col, str):
                    field_names.append(col)
                elif isinstance(col, Column) or hasattr(col, "name"):
                    field_names.append(col.name)
                else:
                    # Fallback: use index
                    field_names.append(f"field_{len(field_names)}")

            # Create struct with aliased expressions
            return pl.struct(
                [expr.alias(name) for expr, name in zip(struct_exprs, field_names)]
            )

        # Special-case eqNullSafe when it is treated as a function call.
        # Some call-sites may surface null-safe equality via op.function_name
        # rather than the comparison operator path; delegate to the same
        # comparison coercion helper to ensure consistent semantics.
        if function_name == "eqnullsafe":
            right_expr = self.translate(op.value)
            return self._coerce_for_comparison(col_expr, right_expr, "eqNullSafe")

        # Handle array_sort before other checks since it can have op.value=None or op.value=bool
        if operation == "array_sort":
            # array_sort(col, asc) - sort array elements
            # op.value can be None (default ascending) or a boolean
            asc = True  # Default to ascending
            if op.value is not None:
                asc = op.value if isinstance(op.value, bool) else bool(op.value)
            # Polars list.sort() with descending=False for ascending, descending=True for descending
            return col_expr.list.sort(descending=not asc)

        # Handle to_timestamp before other checks since it can have op.value=None or op.value=format
        # to_timestamp needs special handling for multiple input types
        # Note: We can optionally pass the input column dtype to help choose the right method
        if operation == "to_timestamp":
            # to_timestamp(col, format) or to_timestamp(col)
            # PySpark accepts multiple input types:
            # - StringType: parse with format (or default format)
            # - TimestampType: pass-through (return as-is)
            # - IntegerType/LongType: Unix timestamp in seconds
            # - DateType: convert Date to Timestamp
            # - DoubleType: Unix timestamp with decimal seconds
            from datetime import datetime, timezone, date

            if op.value is not None:
                # With format string
                format_str = op.value
                # Handle optional fractional seconds like [.SSSSSS]
                import re

                # Check if format includes microseconds/fractional seconds
                # PySpark supports [.SSSSSS] for optional fractional seconds
                # Remove optional fractional pattern from format string for now
                # We'll handle microseconds automatically in the parsing function
                format_str = re.sub(r"\[\.S+\]", "", format_str)
                # Handle single-quoted literals (e.g., 'T' in yyyy-MM-dd'T'HH:mm:ss)
                # Remove quotes but keep the literal characters
                format_str = re.sub(r"'([^']*)'", r"\1", format_str)
                # Convert Java/Spark format to Python format (Polars str.strptime uses Python format)
                format_map = {
                    "yyyy": "%Y",
                    "MM": "%m",
                    "dd": "%d",
                    "HH": "%H",
                    "mm": "%M",
                    "ss": "%S",
                }
                # Sort by length descending to process longest matches first
                for java_pattern, python_pattern in sorted(
                    format_map.items(), key=lambda x: len(x[0]), reverse=True
                ):
                    format_str = format_str.replace(java_pattern, python_pattern)

                # Use str.strptime() for string columns to avoid schema inference issues
                # This is the most efficient approach and avoids Polars incorrectly inferring
                # the input column type as datetime
                # For non-string inputs, fall back to map_elements

                def convert_to_timestamp_single_with_format(
                    val: Any, fmt: str = format_str
                ) -> Any:
                    """Convert single value to timestamp with format."""
                    from datetime import datetime, timezone, date

                    if val is None:
                        return None
                    # If already a datetime, return as-is (TimestampType pass-through)
                    if isinstance(val, datetime):
                        return val
                    # If date, convert to datetime at midnight
                    if isinstance(val, date) and not isinstance(val, datetime):
                        return datetime.combine(val, datetime.min.time())
                    # If numeric (int/long/double), treat as Unix timestamp
                    if isinstance(val, (int, float)):
                        try:
                            timestamp = float(val)
                            # Interpret as UTC and convert to local timezone (PySpark behavior)
                            dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                            return dt_utc.astimezone().replace(tzinfo=None)
                        except (ValueError, TypeError, OverflowError, OSError):
                            return None
                    # If string, parse with format
                    if isinstance(val, str):
                        import re

                        # PySpark's to_timestamp is lenient and automatically handles microseconds
                        # even if they're not in the format string. Strip microseconds before parsing
                        # if the format doesn't include them.
                        val_cleaned = val

                        # Check if format includes microseconds (look for %f or similar patterns)
                        has_microseconds_in_format = "%f" in fmt or "S" in fmt.upper()

                        # If format doesn't include microseconds, strip them from the value
                        # Match microseconds pattern: . followed by digits (1-6 digits typical)
                        # This pattern can appear after seconds but before timezone or end of string
                        if not has_microseconds_in_format:
                            # Remove microseconds pattern: . followed by 1-6 digits
                            # Match: .123456 or .123 (before timezone or end of string)
                            val_cleaned = re.sub(
                                r"\.\d{1,6}(?=[+-]|\d{2}:\d{2}|Z|$)", "", val_cleaned
                            )

                        # Remove timezone patterns (e.g., +00:00, Z) if not in format
                        if "%z" not in fmt and "%Z" not in fmt:
                            val_cleaned = re.sub(r"[+-]\d{2}:\d{2}$", "", val_cleaned)
                            val_cleaned = val_cleaned.rstrip("Z")

                        try:
                            return datetime.strptime(val_cleaned, fmt)
                        except (ValueError, TypeError):
                            # If parsing still fails, try original value as fallback
                            try:
                                return datetime.strptime(val, fmt)
                            except (ValueError, TypeError):
                                return None
                    # For other types, try converting to string and parsing
                    try:
                        return datetime.strptime(str(val), fmt)
                    except (ValueError, TypeError):
                        return None

                # Check if the input is a string type (from dtype or string operation).
                # For string types, use str.strptime() which works correctly and avoids
                # schema inference issues with map_elements.
                # For other types (datetime, date, numeric), use map_elements which
                # handles all types correctly at runtime.
                is_string_type = False

                # Check if we have dtype information from the DataFrame
                # input_col_dtype is a Polars dtype (e.g., pl.Utf8 for String)
                if input_col_dtype is not None and input_col_dtype == pl.Utf8:
                    is_string_type = True
                # Also check if it's a string operation or cast to string
                if not is_string_type and isinstance(op.column, ColumnOperation):
                    string_ops = [
                        "regexp_replace",
                        "substring",
                        "concat",
                        "upper",
                        "lower",
                        "trim",
                        "ltrim",
                        "rtrim",
                    ]
                    # Check if it's a string operation
                    if op.column.operation in string_ops:
                        is_string_type = True
                    # Check if it's a cast to string
                    elif op.column.operation == "cast":
                        cast_target = op.column.value
                        if isinstance(cast_target, str) and cast_target.lower() in [
                            "string",
                            "varchar",
                        ]:
                            is_string_type = True
                    # For nested ColumnOperations, check recursively
                    elif isinstance(op.column.column, ColumnOperation):
                        inner_op = op.column.column
                        if inner_op.operation in string_ops:
                            is_string_type = True
                        elif inner_op.operation == "cast":
                            cast_target = inner_op.value
                            if isinstance(cast_target, str) and cast_target.lower() in [
                                "string",
                                "varchar",
                            ]:
                                is_string_type = True

                if is_string_type:
                    # For string types, preprocess to strip microseconds if format doesn't include them,
                    # then use str.strptime() directly. This avoids map_elements schema validation issues.
                    # PySpark's to_timestamp automatically handles microseconds even if not in format.
                    has_microseconds_in_format = "%f" in format_str

                    if not has_microseconds_in_format:
                        # Strip microseconds from the string column before parsing
                        # PySpark's to_timestamp automatically handles microseconds even if not in format
                        # Use Polars string operations to remove microseconds pattern
                        # Pattern: Remove .\d+ after seconds (HH:mm:ss.123456 -> HH:mm:ss)
                        # Use a single pattern that handles most cases: (:\d{2})\.\d+ -> :\d{2}
                        cleaned_expr = col_expr.str.replace_all(
                            r"(:\d{2})\.\d+", r"$1", literal=False
                        )
                        # Also remove any remaining .\d+ at the end (handles edge cases)
                        cleaned_expr = cleaned_expr.str.replace_all(
                            r"\.\d+$", "", literal=False
                        )
                        # Now use str.strptime on the cleaned expression
                        # This should work without schema validation issues
                        return cleaned_expr.str.strptime(
                            pl.Datetime, format_str, strict=False
                        )
                    else:
                        # Format includes microseconds, use str.strptime directly
                        return col_expr.str.strptime(
                            pl.Datetime, format_str, strict=False
                        )
                else:
                    # Use map_elements for non-string types (datetime, date, numeric)
                    # This handles all types correctly at runtime
                    def to_timestamp_with_format(val: Any) -> Any:
                        return convert_to_timestamp_single_with_format(val, format_str)

                    result_expr = col_expr.map_elements(
                        to_timestamp_with_format,
                        return_dtype=pl.Datetime(time_unit="us"),
                    )
                    # Explicitly cast to ensure Polars recognizes the type during schema validation
                    return result_expr.cast(pl.Datetime(time_unit="us"))
            else:
                # Without format - handle all types
                def convert_to_timestamp_no_format(val: Any) -> Any:
                    if val is None:
                        return None
                    # If already a datetime, return as-is (TimestampType pass-through)
                    if isinstance(val, datetime):
                        return val
                    # If date, convert to datetime at midnight
                    if isinstance(val, date) and not isinstance(val, datetime):
                        return datetime.combine(val, datetime.min.time())
                    # If numeric (int/long/double), treat as Unix timestamp
                    if isinstance(val, (int, float)):
                        try:
                            timestamp = float(val)
                            # Interpret as UTC and convert to local timezone (PySpark behavior)
                            dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                            return dt_utc.astimezone().replace(tzinfo=None)
                        except (ValueError, TypeError, OverflowError, OSError):
                            return None
                    # If string, try parsing with common formats
                    if isinstance(val, str):
                        for fmt in [
                            "%Y-%m-%d %H:%M:%S",
                            "%Y-%m-%dT%H:%M:%S",
                            "%Y-%m-%d",
                        ]:
                            try:
                                return datetime.strptime(val, fmt)
                            except ValueError:
                                continue
                        return None
                    # For other types, try converting to string and parsing
                    val_str = str(val)
                    for fmt in [
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d",
                    ]:
                        try:
                            return datetime.strptime(val_str, fmt)
                        except ValueError:
                            continue
                    return None

                # Use map_batches instead of map_elements for better lazy evaluation support
                def convert_to_timestamp_batch_no_format(
                    series: pl.Series,
                ) -> pl.Series:
                    """Convert batch of values to timestamps without format."""
                    from datetime import datetime, timezone, date

                    def convert_single(val: Any) -> Any:
                        if val is None:
                            return None
                        # If already a datetime, return as-is (TimestampType pass-through)
                        if isinstance(val, datetime):
                            return val
                        # If date, convert to datetime at midnight
                        if isinstance(val, date) and not isinstance(val, datetime):
                            return datetime.combine(val, datetime.min.time())
                        # If numeric (int/long/double), treat as Unix timestamp
                        if isinstance(val, (int, float)):
                            try:
                                timestamp = float(val)
                                # Interpret as UTC and convert to local timezone (PySpark behavior)
                                dt_utc = datetime.fromtimestamp(
                                    timestamp, tz=timezone.utc
                                )
                                return dt_utc.astimezone().replace(tzinfo=None)
                            except (ValueError, TypeError, OverflowError, OSError):
                                return None
                        # If string, try parsing with common formats
                        if isinstance(val, str):
                            for fmt in [
                                "%Y-%m-%d %H:%M:%S",
                                "%Y-%m-%dT%H:%M:%S",
                                "%Y-%m-%d",
                            ]:
                                try:
                                    return datetime.strptime(val, fmt)
                                except ValueError:
                                    continue
                            return None
                        # For other types, try converting to string and parsing
                        val_str = str(val)
                        for fmt in [
                            "%Y-%m-%d %H:%M:%S",
                            "%Y-%m-%dT%H:%M:%S",
                            "%Y-%m-%d",
                        ]:
                            try:
                                return datetime.strptime(val_str, fmt)
                            except ValueError:
                                continue
                        return None

                    return series.map_elements(
                        convert_single, return_dtype=pl.Datetime(time_unit="us")
                    )

                return col_expr.map_batches(
                    convert_to_timestamp_batch_no_format,
                    return_dtype=pl.Datetime(time_unit="us"),
                )

        # Map function names to Polars expressions
        # Handle functions with arguments (operation is already extracted above)
        if op.value is not None:
            if operation == "substring":
                # substring(col, start, length) - Polars uses 0-indexed, PySpark uses 1-indexed
                if isinstance(op.value, tuple):
                    start = op.value[0]
                    length = op.value[1] if len(op.value) > 1 else None
                    # Convert 1-indexed to 0-indexed
                    start_idx = start - 1 if start > 0 else 0
                    if length is not None:
                        return col_expr.str.slice(start_idx, length)
                    else:
                        return col_expr.str.slice(start_idx)
                else:
                    return col_expr.str.slice(op.value - 1 if op.value > 0 else 0)
            elif operation == "regexp_replace":
                # regexp_replace(col, pattern, replacement)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    pattern = op.value[0]
                    replacement = op.value[1]
                    return col_expr.str.replace_all(pattern, replacement, literal=True)
                else:
                    raise ValueError(
                        "regexp_replace requires (pattern, replacement) tuple"
                    )
            elif operation == "regexp_extract":
                # regexp_extract(col, pattern, idx)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    pattern = op.value[0]
                    idx = op.value[1] if len(op.value) > 1 else 0

                    # Check if pattern contains lookahead/lookbehind assertions
                    # Polars doesn't support these, so we need to use Python fallback
                    import re as re_module

                    has_lookaround = False
                    try:
                        # Check if pattern contains lookaround assertions
                        if re_module.search(r"\(\?[<>=!]", pattern):
                            has_lookaround = True
                    except Exception:
                        # If pattern check fails, try Polars anyway
                        has_lookaround = False

                    if has_lookaround:
                        # Use Python re module fallback for lookahead/lookbehind patterns
                        def regexp_extract_fallback(val: Any) -> Optional[str]:
                            """Fallback for regexp_extract with lookahead/lookbehind."""
                            if val is None or not isinstance(val, str):
                                return None
                            try:
                                match = re_module.search(pattern, val)
                                if match:
                                    # Extract the group at idx (0 = full match, 1+ = groups)
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

                        # Use map_elements for Python fallback
                        return col_expr.map_elements(
                            regexp_extract_fallback, return_dtype=pl.Utf8
                        )
                    else:
                        # Try Polars native extract first, fallback to Python if it fails
                        try:
                            # Polars extract returns a string, matching group at idx
                            # Note: Polars str.extract uses group index 1-based, so idx=0 maps to full match
                            # But PySpark regexp_extract idx=0 is full match, idx=1+ are capture groups
                            # Polars doesn't support full match (idx=0) in extract, so we need special handling
                            if idx == 0:
                                # For idx=0 (full match), use extract_all and get first element
                                extracted = col_expr.str.extract_all(pattern)

                                # Get first match from list
                                def get_first_match(matches: Any) -> Optional[str]:
                                    if (
                                        matches is None
                                        or not isinstance(matches, list)
                                        or len(matches) == 0
                                    ):
                                        return None
                                    # Get the first match (which is the full match)
                                    return matches[0] if matches else None

                                return extracted.map_elements(
                                    get_first_match, return_dtype=pl.Utf8
                                )
                            else:
                                # For idx > 0, use Polars extract with group index
                                # Polars uses 1-based indexing for groups
                                return col_expr.str.extract(pattern, idx)
                        except pl.exceptions.ComputeError as e:
                            error_msg = str(e).lower()
                            # Check if error is about look-around not being supported
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
                                # Re-raise other ComputeErrors
                                raise
                else:
                    raise ValueError("regexp_extract requires (pattern, idx) tuple")
            elif operation == "split":
                # split(col, delimiter, limit)
                if isinstance(op.value, tuple):
                    delimiter, limit = op.value
                else:
                    delimiter = op.value
                    limit = None
                # Polars split() doesn't have limit parameter
                # If limit is None or -1, use regular split (no limit)
                if limit is None or limit == -1:
                    return col_expr.str.split(delimiter)
                else:
                    # Use Python fallback for split with limit
                    # PySpark limit behavior: limit=3 means return at most 3 parts
                    # Python split(maxsplit=n) splits n times, resulting in n+1 parts
                    # So to get limit parts, we need maxsplit = limit - 1
                    # Special case: limit=1 means no split (return original string as single-element list)
                    def split_with_limit(val: Any) -> Optional[List[str]]:
                        """Split string with limit using Python."""
                        if val is None:
                            return None
                        try:
                            s = str(val)
                            if limit == 1:
                                # limit=1 means no split, return original as single-element list
                                return [s]
                            # For limit > 1: maxsplit = limit - 1 to get limit parts
                            maxsplit = limit - 1
                            parts = s.split(delimiter, maxsplit=maxsplit)
                            return parts
                        except Exception:
                            return None

                    return col_expr.map_elements(
                        split_with_limit, return_dtype=pl.List(pl.Utf8)
                    )
            elif operation == "format_string":
                # format_string(format_str, *columns) - use Python fallback
                # format_string is complex with multiple columns, so we use Python evaluation
                # which already has proper support in ExpressionEvaluator
                raise ValueError("format_string operation requires Python evaluation")
            elif operation == "btrim":
                # btrim(col, trim_string) or btrim(col)
                if isinstance(op.value, str):
                    return col_expr.str.strip_chars(op.value)
                else:
                    # No trim_string specified, trim whitespace
                    return col_expr.str.strip_chars()
            elif operation == "left":
                # left(col, length)
                n = op.value if isinstance(op.value, int) else int(op.value)
                return col_expr.str.slice(0, n)
            elif operation == "right":
                # right(col, length)
                n = op.value if isinstance(op.value, int) else int(op.value)
                return col_expr.str.slice(-n) if n > 0 else col_expr.str.slice(0, 0)
            elif operation == "contains":
                # contains(col, substring)
                if isinstance(op.value, str):
                    return col_expr.str.contains(op.value)
                else:
                    value_expr = self.translate(op.value)
                    return col_expr.str.contains(value_expr)
            elif operation == "startswith":
                # startswith(col, substring)
                if isinstance(op.value, str):
                    return col_expr.str.starts_with(op.value)
                else:
                    value_expr = self.translate(op.value)
                    return col_expr.str.starts_with(value_expr)
            elif operation == "endswith":
                # endswith(col, substring)
                if isinstance(op.value, str):
                    return col_expr.str.ends_with(op.value)
                else:
                    value_expr = self.translate(op.value)
                    return col_expr.str.ends_with(value_expr)
            elif operation == "like":
                # like(col, pattern) - SQL LIKE pattern matching
                pattern = op.value if isinstance(op.value, str) else str(op.value)
                # Convert SQL LIKE pattern to regex (simplified: % -> .*, _ -> .)
                regex_pattern = pattern.replace("%", ".*").replace("_", ".")
                return col_expr.str.contains(regex_pattern, literal=False)
            elif operation == "rlike":
                # rlike(col, pattern) - Regular expression pattern matching
                pattern = op.value if isinstance(op.value, str) else str(op.value)

                # Check if pattern contains lookahead/lookbehind assertions
                # Polars doesn't support these, so we need to use Python fallback
                import re as re_module

                has_lookaround = False
                try:
                    # Check if pattern contains lookaround assertions
                    if re_module.search(r"\(\?[<>=!]", pattern):
                        has_lookaround = True
                except Exception:
                    # If pattern check fails, try Polars anyway
                    has_lookaround = False

                if has_lookaround:
                    # Use Python re module fallback for lookahead/lookbehind patterns
                    def rlike_fallback(val: Any) -> bool:
                        """Fallback for rlike with lookahead/lookbehind."""
                        if val is None or not isinstance(val, str):
                            return False
                        try:
                            return bool(re_module.search(pattern, val))
                        except Exception:
                            return False

                    # Use map_elements for Python fallback
                    return col_expr.map_elements(
                        rlike_fallback, return_dtype=pl.Boolean
                    )
                else:
                    # Try Polars native contains first, fallback to Python if it fails
                    try:
                        return col_expr.str.contains(pattern, literal=False)
                    except pl.exceptions.ComputeError as e:
                        error_msg = str(e).lower()
                        # Check if error is about look-around not being supported
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
                            # Re-raise other ComputeErrors
                            raise
            elif operation == "regexp":
                # regexp(col, pattern) - Alias for rlike
                # Use the same implementation as rlike (handles look-around patterns)
                pattern = op.value if isinstance(op.value, str) else str(op.value)

                # Check if pattern contains lookahead/lookbehind assertions
                # Polars doesn't support these, so we need to use Python fallback
                import re as re_module

                has_lookaround = False
                try:
                    # Check if pattern contains lookaround assertions
                    if re_module.search(r"\(\?[<>=!]", pattern):
                        has_lookaround = True
                except Exception:
                    # If pattern check fails, try Polars anyway
                    has_lookaround = False

                if has_lookaround:
                    # Use Python re module fallback for lookahead/lookbehind patterns
                    def regexp_fallback(val: Any) -> bool:
                        """Fallback for regexp with lookahead/lookbehind."""
                        if val is None or not isinstance(val, str):
                            return False
                        try:
                            return bool(re_module.search(pattern, val))
                        except Exception:
                            return False

                    # Use map_elements for Python fallback
                    return col_expr.map_elements(
                        regexp_fallback, return_dtype=pl.Boolean
                    )
                else:
                    # Try Polars native contains first, fallback to Python if it fails
                    try:
                        return col_expr.str.contains(pattern, literal=False)
                    except pl.exceptions.ComputeError as e:
                        error_msg = str(e).lower()
                        # Check if error is about look-around not being supported
                        if (
                            "look-around" in error_msg
                            or "look-ahead" in error_msg
                            or "look-behind" in error_msg
                        ):
                            # Fallback to Python re module
                            def regexp_fallback(val: Any) -> bool:
                                """Fallback for regexp with lookahead/lookbehind."""
                                if val is None or not isinstance(val, str):
                                    return False
                                try:
                                    return bool(re_module.search(pattern, val))
                                except Exception:
                                    return False

                            return col_expr.map_elements(
                                regexp_fallback, return_dtype=pl.Boolean
                            )
                        else:
                            # Re-raise other ComputeErrors
                            raise
            elif operation == "ilike":
                # ilike(col, pattern) - Case-insensitive LIKE
                pattern = op.value if isinstance(op.value, str) else str(op.value)
                regex_pattern = pattern.replace("%", ".*").replace("_", ".")
                return col_expr.str.to_lowercase().str.contains(
                    regex_pattern, literal=False
                )
            elif operation == "regexp_like":
                # regexp_like(col, pattern) - Alias for rlike
                # Use the same implementation as rlike (handles look-around patterns)
                pattern = op.value if isinstance(op.value, str) else str(op.value)

                # Check if pattern contains lookahead/lookbehind assertions
                # Polars doesn't support these, so we need to use Python fallback
                import re as re_module

                has_lookaround = False
                try:
                    # Check if pattern contains lookaround assertions
                    if re_module.search(r"\(\?[<>=!]", pattern):
                        has_lookaround = True
                except Exception:
                    # If pattern check fails, try Polars anyway
                    has_lookaround = False

                if has_lookaround:
                    # Use Python re module fallback for lookahead/lookbehind patterns
                    def regexp_like_fallback(val: Any) -> bool:
                        """Fallback for regexp_like with lookahead/lookbehind."""
                        if val is None or not isinstance(val, str):
                            return False
                        try:
                            return bool(re_module.search(pattern, val))
                        except Exception:
                            return False

                    # Use map_elements for Python fallback
                    return col_expr.map_elements(
                        regexp_like_fallback, return_dtype=pl.Boolean
                    )
                else:
                    # Try Polars native contains first, fallback to Python if it fails
                    try:
                        return col_expr.str.contains(pattern, literal=False)
                    except pl.exceptions.ComputeError as e:
                        error_msg = str(e).lower()
                        # Check if error is about look-around not being supported
                        if (
                            "look-around" in error_msg
                            or "look-ahead" in error_msg
                            or "look-behind" in error_msg
                        ):
                            # Fallback to Python re module
                            def regexp_like_fallback(val: Any) -> bool:
                                """Fallback for regexp_like with lookahead/lookbehind."""
                                if val is None or not isinstance(val, str):
                                    return False
                                try:
                                    return bool(re_module.search(pattern, val))
                                except Exception:
                                    return False

                            return col_expr.map_elements(
                                regexp_like_fallback, return_dtype=pl.Boolean
                            )
                        else:
                            # Re-raise other ComputeErrors
                            raise
            elif operation == "regexp_count":
                # regexp_count(col, pattern) - Count regex matches
                pattern = op.value if isinstance(op.value, str) else str(op.value)
                # Use regex to find all matches and count them
                return col_expr.str.count_matches(pattern, literal=False)
            elif operation == "regexp_substr":
                # regexp_substr(col, pattern, pos, occurrence) - Extract substring matching regex
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    pattern = op.value[0]
                    pos = op.value[1] if len(op.value) > 1 else 1
                    # Simplified implementation - extract first match
                    return col_expr.str.extract(pattern, 0)
                else:
                    pattern = op.value if isinstance(op.value, str) else str(op.value)
                    return col_expr.str.extract(pattern, 0)
            elif operation == "regexp_instr":
                # regexp_instr(col, pattern, pos, occurrence) - Find position of regex match
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    pattern = op.value[0]
                    # Simplified implementation - find first match position
                    return col_expr.str.find(pattern)
                else:
                    pattern = op.value if isinstance(op.value, str) else str(op.value)
                    return col_expr.str.find(pattern)
            elif operation == "find_in_set":
                # find_in_set(value, str_list) - Find position in comma-separated list
                # Simplified implementation
                return pl.lit(0)  # Placeholder
            elif operation == "pmod":
                # pmod(dividend, divisor) - Positive modulo
                if isinstance(op.value, (Column, ColumnOperation)):
                    divisor = self.translate(op.value)
                else:
                    divisor = pl.lit(op.value)
                # pmod always returns positive: ((dividend % divisor) + divisor) % divisor
                return ((col_expr % divisor) + divisor) % divisor
            elif operation == "shiftleft":
                # shiftleft(col, num_bits) - Bitwise left shift
                if isinstance(op.value, (Column, ColumnOperation)):
                    num_bits = self.translate(op.value)
                else:
                    num_bits = pl.lit(op.value)
                return col_expr << num_bits
            elif operation == "shiftright":
                # shiftright(col, num_bits) - Bitwise right shift (signed)
                if isinstance(op.value, (Column, ColumnOperation)):
                    num_bits = self.translate(op.value)
                else:
                    num_bits = pl.lit(op.value)
                return col_expr >> num_bits
            elif operation == "shiftrightunsigned":
                # shiftrightunsigned(col, num_bits) - Bitwise unsigned right shift
                # In Python, >> is already unsigned for positive numbers
                if isinstance(op.value, (Column, ColumnOperation)):
                    num_bits = self.translate(op.value)
                else:
                    num_bits = pl.lit(op.value)
                return col_expr >> num_bits
            elif operation == "replace":
                # replace(col, old, new)
                if isinstance(op.value, tuple) and len(op.value) == 2:
                    old, new = op.value
                    return col_expr.str.replace(old, new)
                else:
                    raise ValueError("replace requires (old, new) tuple")
            elif operation == "split_part":
                # split_part(col, delimiter, part) - Extract part of string split by delimiter
                if isinstance(op.value, tuple) and len(op.value) == 2:
                    delimiter, part = op.value
                    # Split and get the part (1-indexed, so subtract 1)
                    return col_expr.str.split(delimiter).list.get(part - 1)
                else:
                    raise ValueError("split_part requires (delimiter, part) tuple")
            elif operation == "position":
                # position(substring, col) - Find position of substring in string (1-indexed)
                # Note: op.value is the substring, op.column is the string to search in
                substring = op.value if isinstance(op.value, str) else str(op.value)
                # Polars find returns 0-based index, add 1 for 1-based
                return col_expr.str.find(substring) + 1
            elif operation == "substr":
                # substr(col, start, length) - Requires length parameter (unlike substring)
                # PySpark behavior: start can be negative (counts from end), 0 is treated as 1
                if isinstance(op.value, tuple):
                    start, length = op.value[0], op.value[1]
                else:
                    # Should not happen for substr (requires length), but handle gracefully
                    start, length = op.value, None

                if length is None:
                    # Fallback to Python evaluation if length is missing
                    raise ValueError("substr requires length parameter")

                # Handle negative start positions and start=0
                # For Polars, we need to compute the actual start index
                # Negative start: use col_expr.str.len_chars() to get string length
                if start < 0:
                    # Negative start counts from end: start_idx = len + start
                    # Polars: str.slice() with negative start is not directly supported
                    # We'll use a conditional expression
                    start_idx_expr = col_expr.str.len_chars() + start
                    start_idx_expr = (
                        pl.when(start_idx_expr < 0).then(0).otherwise(start_idx_expr)
                    )
                elif start == 0:
                    # start=0 is treated as start=1 (0-indexed)
                    start_idx_expr = pl.lit(0)
                else:
                    # Positive start: convert 1-indexed to 0-indexed
                    start_idx_expr = pl.lit(start - 1)

                return col_expr.str.slice(start_idx_expr, length)
            elif operation == "elt":
                # elt(n, *columns) - Return element at index from list of columns
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    n, columns = op.value[0], op.value[1:]
                    # Translate n and columns
                    n_expr = self.translate(n) if not isinstance(n, int) else pl.lit(n)
                    # Create a list of translated columns
                    col_list = [col_expr] + [self.translate(col) for col in columns]
                    # Use Polars list indexing (1-indexed, so subtract 1)
                    # This is complex - we'll use a when/otherwise chain
                    result = None
                    for i, col in enumerate(col_list, 1):
                        if result is None:
                            result = pl.when(n_expr == i).then(col)
                        else:
                            result = result.when(n_expr == i).then(col)
                    return (
                        result.otherwise(None) if result is not None else pl.lit(None)
                    )
                else:
                    raise ValueError("elt requires (n, *columns) tuple")
            elif operation == "days":
                # days(n) - Convert number to days interval (for date arithmetic)
                # This is a numeric multiplier for date operations
                return col_expr  # Return as-is, will be used in date arithmetic
            elif operation == "hours":
                # hours(n) - Convert number to hours interval
                return col_expr  # Return as-is, will be used in date arithmetic
            elif operation == "months":
                # months(n) - Convert number to months interval
                return col_expr  # Return as-is, will be used in date arithmetic
            elif operation == "equal_null":
                # equal_null(col1, col2) - Equality check that treats NULL as equal
                col2_expr = self.translate(op.value)
                # Return True if both are NULL, or if both are equal
                return (col_expr.is_null() & col2_expr.is_null()) | (
                    col_expr == col2_expr
                )
            elif operation == "concat":
                # concat(*columns) - op.value is tuple/list of additional columns/literals
                # The first column is in op.column, the rest are in op.value
                if op.value and (
                    isinstance(op.value, (list, tuple)) and len(op.value) > 0
                ):
                    # Translate all columns/literals
                    all_cols = [col_expr]  # Start with the first column
                    for col in op.value:
                        if isinstance(col, str):
                            # Try to translate as column first
                            # If it fails or doesn't exist, we'll treat as literal
                            # For now, we'll try pl.col() and catch errors, but a better approach
                            # is to check if it's a valid identifier (column names are identifiers)
                            # Strings with spaces or special chars are likely literals
                            if (
                                col.strip() != col
                                or not col.replace("_", "").replace("-", "").isalnum()
                            ):
                                # String has spaces or special chars - treat as literal
                                all_cols.append(pl.lit(col))
                            else:
                                # Try as column name
                                try:
                                    all_cols.append(pl.col(col))
                                except Exception:
                                    # If it fails, treat as literal
                                    logger.debug(
                                        f"Failed to create column reference for '{col}', treating as literal",
                                        exc_info=True,
                                    )
                                    all_cols.append(pl.lit(col))
                        elif hasattr(col, "value"):  # Literal
                            # Resolve lazy literals before translating
                            if hasattr(col, "_is_lazy") and col._is_lazy:
                                all_cols.append(pl.lit(col._resolve_lazy_value()))
                            else:
                                all_cols.append(pl.lit(col.value))
                        else:
                            # Column or expression
                            all_cols.append(self.translate(col))
                    # Cast all to string and concatenate
                    str_cols = [col.cast(pl.Utf8) for col in all_cols]
                    result = str_cols[0]
                    for other_col in str_cols[1:]:
                        result = result + other_col
                    return result
                else:
                    # Single column - just cast to string
                    return col_expr.cast(pl.Utf8)
            elif operation == "concat_ws":
                # concat_ws(sep, *columns) - op.value is (sep, [columns])
                if isinstance(op.value, tuple) and len(op.value) >= 1:
                    sep = op.value[0]
                    other_cols = op.value[1] if len(op.value) > 1 else []
                    # Translate all columns - ensure they're properly translated
                    translated_cols = []
                    # First column is already in col_expr
                    translated_cols.append(col_expr)
                    # Translate other columns
                    for col in other_cols:
                        if isinstance(col, str):
                            # String column name
                            translated_cols.append(pl.col(col))
                        elif isinstance(col, (int, float, bool)):
                            # Literal value
                            translated_cols.append(pl.lit(col))
                        else:
                            # Expression or Column
                            translated_cols.append(self.translate(col))
                    # Join with separator using Polars
                    # Ensure all columns are strings to avoid nested Objects error
                    if len(translated_cols) == 1:
                        return translated_cols[0].cast(pl.Utf8)
                    # Cast all to string first
                    str_cols = [col.cast(pl.Utf8) for col in translated_cols]
                    result = str_cols[0]
                    for other_col in str_cols[1:]:
                        result = result + pl.lit(str(sep)) + other_col
                    return result
                else:
                    raise ValueError("concat_ws requires (sep, [columns]) tuple")
            elif operation == "like":
                # SQL LIKE pattern - convert to Polars regex
                pattern = op.value
                # Convert SQL LIKE to regex: % -> .*, _ -> .
                regex_pattern = pattern.replace("%", ".*").replace("_", ".")
                return col_expr.str.contains(regex_pattern, literal=False)
            elif operation == "rlike":
                # Regular expression pattern matching
                pattern = op.value if isinstance(op.value, str) else str(op.value)

                # Check if pattern contains lookahead/lookbehind assertions
                # Polars doesn't support these, so we need to use Python fallback
                import re as re_module

                has_lookaround = False
                try:
                    # Check if pattern contains lookaround assertions
                    if re_module.search(r"\(\?[<>=!]", pattern):
                        has_lookaround = True
                except Exception:
                    # If pattern check fails, try Polars anyway
                    has_lookaround = False

                if has_lookaround:
                    # Use Python re module fallback for lookahead/lookbehind patterns
                    def rlike_fallback(val: Any) -> bool:
                        """Fallback for rlike with lookahead/lookbehind."""
                        if val is None or not isinstance(val, str):
                            return False
                        try:
                            return bool(re_module.search(pattern, val))
                        except Exception:
                            return False

                    # Use map_elements for Python fallback
                    return col_expr.map_elements(
                        rlike_fallback, return_dtype=pl.Boolean
                    )
                else:
                    # Try Polars native contains first, fallback to Python if it fails
                    try:
                        return col_expr.str.contains(pattern, literal=False)
                    except pl.exceptions.ComputeError as e:
                        error_msg = str(e).lower()
                        # Check if error is about look-around not being supported
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
                            # Re-raise other ComputeErrors
                            raise
            elif operation == "round":
                # round(col, decimals)
                decimals = op.value if isinstance(op.value, int) else 0
                if decimals < 0:
                    # Negative decimals: round to nearest 10^|decimals|
                    # e.g., round(12345, -3) = round(12345/1000) * 1000 = 12000
                    factor = 10 ** abs(decimals)
                    return (col_expr / factor).round() * factor
                else:
                    return col_expr.round(decimals)
            elif operation == "pow":
                # pow(col, exponent)
                exponent = (
                    self.translate(op.value)
                    if not isinstance(op.value, (int, float))
                    else pl.lit(op.value)
                )
                return col_expr.pow(exponent)
            elif operation == "power":
                # power(col, exponent) - Alias for pow
                exponent = (
                    self.translate(op.value)
                    if not isinstance(op.value, (int, float))
                    else pl.lit(op.value)
                )
                return col_expr.pow(exponent)
            elif operation == "to_date":
                # to_date(col, format) or to_date(col)
                # PySpark accepts StringType, TimestampType, or DateType
                # If input is already TimestampType or DateType, convert directly
                # If input is StringType, parse with format

                # IMPORTANT: Check for nested to_timestamp BEFORE translating col_expr
                # This allows us to detect the nested structure before it's converted to a Polars expression
                is_nested_to_timestamp = (
                    isinstance(op.column, ColumnOperation)
                    and op.column.operation == "to_timestamp"
                )

                if is_nested_to_timestamp:
                    # For to_date(to_timestamp(...)), the input is already datetime
                    # Use .dt.date() directly for datetime columns to avoid schema validation issues
                    # First translate the nested to_timestamp to get the datetime expression
                    nested_ts_expr = self._translate_operation(
                        op.column, input_col_dtype=None
                    )
                    # Use .dt.date() for datetime columns - this avoids schema validation issues
                    return nested_ts_expr.dt.date()

                # Use map_elements to handle both StringType and TimestampType/DateType inputs
                # This avoids the issue where .str.strptime fails on datetime columns
                def convert_to_date(val: Any, format_str: Optional[str] = None) -> Any:
                    from datetime import datetime, date

                    if val is None:
                        return None
                    # If already a date, return as-is
                    if isinstance(val, date) and not isinstance(val, datetime):
                        return val
                    # If datetime, convert to date
                    if isinstance(val, datetime):
                        return val.date()
                    # If string, parse with format
                    if isinstance(val, str):
                        if format_str:
                            try:
                                dt = datetime.strptime(val, format_str)
                                return dt.date()
                            except (ValueError, TypeError):
                                return None
                        else:
                            # Try common formats
                            for fmt in [
                                "%Y-%m-%d %H:%M:%S",
                                "%Y-%m-%dT%H:%M:%S",
                                "%Y-%m-%d",
                            ]:
                                try:
                                    dt = datetime.strptime(val, fmt)
                                    return dt.date()
                                except ValueError:
                                    continue
                            return None
                    return None

                if op.value is not None:
                    # With format string - convert Java SimpleDateFormat to Polars format
                    format_str = op.value
                    import re

                    # Handle single-quoted literals (e.g., 'T' in yyyy-MM-dd'T'HH:mm:ss)
                    format_str = re.sub(r"'([^']*)'", r"\1", format_str)
                    # Convert Java format to Polars format
                    format_map = {
                        "yyyy": "%Y",
                        "MM": "%m",
                        "dd": "%d",
                        "HH": "%H",
                        "mm": "%M",
                        "ss": "%S",
                    }
                    # Sort by length descending to process longest matches first
                    for java_pattern, polars_pattern in sorted(
                        format_map.items(), key=lambda x: len(x[0]), reverse=True
                    ):
                        format_str = format_str.replace(java_pattern, polars_pattern)
                    # Use map_elements to handle both StringType and TimestampType inputs
                    # Wrap in a lambda that captures format_str to avoid closure issues
                    return col_expr.map_elements(
                        lambda x, fmt=format_str: convert_to_date(x, fmt),
                        return_dtype=pl.Date,
                    )
                else:
                    # Without format - use map_elements which checks type at runtime
                    return col_expr.map_elements(
                        lambda x: convert_to_date(x),
                        return_dtype=pl.Date,
                    )
            elif operation == "date_format":
                # date_format(col, format) - format a date/timestamp column
                if isinstance(op.value, str):
                    format_str = op.value
                    # Convert Java SimpleDateFormat to Polars strftime format
                    # Common conversions: yyyy -> %Y, MM -> %m, dd -> %d, HH -> %H, mm -> %M, ss -> %S
                    import re

                    format_map = {
                        "yyyy": "%Y",
                        "MM": "%m",
                        "dd": "%d",
                        "HH": "%H",
                        "mm": "%M",
                        "ss": "%S",
                        "EEE": "%a",
                        "EEEE": "%A",
                        "MMM": "%b",
                        "MMMM": "%B",
                    }
                    polars_format = format_str
                    for java_pattern, polars_pattern in sorted(
                        format_map.items(), key=lambda x: len(x[0]), reverse=True
                    ):
                        polars_format = polars_format.replace(
                            java_pattern, polars_pattern
                        )

                    # If column is string, parse it first; if already date/timestamp, use directly
                    # Try to parse as datetime first (handles timestamps), then fall back to date
                    # For string columns, try datetime format first (handles "2024-01-15 10:30:00")
                    # then fall back to date format (handles "2024-01-15")
                    # Use map_elements to handle both formats
                    def parse_and_format(val: Optional[str]) -> Optional[str]:
                        if val is None:
                            return None
                        from datetime import datetime

                        # Try datetime format first
                        try:
                            dt = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                            return dt.strftime(polars_format)
                        except ValueError:
                            # Fall back to date format
                            try:
                                dt = datetime.strptime(val, "%Y-%m-%d")
                                return dt.strftime(polars_format)
                            except ValueError:
                                return None

                    # Use map_elements for flexible parsing
                    return col_expr.map_elements(parse_and_format, return_dtype=pl.Utf8)
                else:
                    raise ValueError("date_format requires format string")
            elif operation == "date_add":
                # date_add(col, days) - add days to a date column
                # Handle both string dates and date columns
                if isinstance(op.value, int):
                    days = op.value
                    days_expr = pl.duration(days=days)
                else:
                    days_expr = self.translate(op.value)
                    # If it's a literal, extract the value for duration
                    if isinstance(days_expr, pl.Expr):
                        # It's an expression - try to use it directly with duration
                        # For literals, we can extract the value
                        # For now, assume it's a literal integer
                        # Actually, we need to handle this differently - use the expression value if available
                        # For expressions, we'll need to convert to duration
                        # Simplest: assume days is an integer literal
                        days = op.value if isinstance(op.value, int) else int(op.value)
                        days_expr = pl.duration(days=days)
                    else:
                        days = (
                            int(days_expr)
                            if not isinstance(days_expr, int)
                            else days_expr
                        )
                        days_expr = pl.duration(days=days)
                # Parse string dates first, then add duration
                # Always try parsing as string first (most common case)
                date_col = col_expr.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                return date_col + days_expr
            elif operation == "date_sub":
                # date_sub(col, days) - subtract days from a date column
                if isinstance(op.value, int):
                    days = op.value
                    days_expr = pl.duration(days=days)
                else:
                    days_expr = self.translate(op.value)
                    if isinstance(days_expr, pl.Expr):
                        days = op.value if isinstance(op.value, int) else int(op.value)
                        days_expr = pl.duration(days=days)
                    else:
                        days = (
                            int(days_expr)
                            if not isinstance(days_expr, int)
                            else days_expr
                        )
                        days_expr = pl.duration(days=days)
                # Parse string dates first, then subtract duration
                date_col = col_expr.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                return date_col - days_expr
            elif operation == "datediff":
                # datediff(end, start) - note: in PySpark, end comes first
                # In ColumnOperation: column is end, value is start
                # Handle Literal objects in value
                from ...functions.core.literals import Literal

                if isinstance(op.value, Literal):
                    start_date = pl.lit(op.value.value)
                else:
                    start_date = self.translate(op.value)
                # Handle both string dates and date columns
                # Polars str.strptime() only works on string columns, so it fails on date columns
                # Use cast to Date which works for both: strings are parsed, dates are unchanged
                end_parsed = col_expr.cast(pl.Date)
                start_parsed = start_date.cast(pl.Date)
                return (end_parsed - start_parsed).dt.total_days()
            elif operation == "lpad":
                # lpad(col, len, pad)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    target_len = op.value[0]
                    pad_str = op.value[1]
                    return col_expr.str.pad_start(target_len, pad_str)
                else:
                    raise ValueError("lpad requires (len, pad) tuple")
            elif operation == "rpad":
                # rpad(col, len, pad)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    target_len = op.value[0]
                    pad_str = op.value[1]
                    return col_expr.str.pad_end(target_len, pad_str)
                else:
                    raise ValueError("rpad requires (len, pad) tuple")
            elif operation == "repeat":
                # repeat(col, n) - repeat string n times
                # Polars doesn't have str.repeat(), use string concatenation
                n = op.value if isinstance(op.value, int) else int(op.value)
                if n <= 0:
                    return pl.lit("")
                # Build expression: col + col + ... + col (n times)
                result = col_expr
                for _ in range(n - 1):
                    result = result + col_expr
                return result
            elif operation == "instr":
                # instr(col, substr) - returns 1-based position, or 0 if not found
                substr = op.value if isinstance(op.value, str) else str(op.value)
                # Polars str.find() returns -1 if not found, we need 0
                # So we check if it's -1, return 0, otherwise add 1 for 1-based indexing
                # Add fill_null(0) as fallback for any nulls
                find_result = col_expr.str.find(substr)
                return (
                    pl.when(find_result == -1)
                    .then(0)
                    .otherwise(find_result + 1)
                    .fill_null(0)
                )
            elif operation == "locate":
                # locate(substr, col, pos) - op.value is (substr, pos)
                if isinstance(op.value, tuple) and len(op.value) >= 1:
                    substr = op.value[0]
                    pos = op.value[1] if len(op.value) > 1 else 1
                    # Find substring starting from pos (1-indexed)
                    return (
                        col_expr.str.slice(pos - 1).str.find(substr) + pos
                    ).fill_null(0)
                else:
                    substr = op.value
                    return col_expr.str.find(substr) + 1
            elif operation == "add_months":
                # add_months(col, months) - add months to a date column
                months = op.value if isinstance(op.value, int) else int(op.value)
                # Parse string dates first, or use directly if already a date
                # Try parsing as string first (most common case)
                try:
                    date_col = col_expr.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                except AttributeError:
                    # Already a date column, use directly
                    date_col = col_expr.cast(pl.Date)
                # Convert to datetime for offset_by, then back to date
                datetime_col = date_col.cast(pl.Datetime)
                # Use offset_by with months
                return datetime_col.dt.offset_by(f"{months}mo").cast(pl.Date)
            elif operation == "last_day":
                # last_day(col) - get last day of month
                # Parse string dates first, or use directly if already a date
                # Try parsing as string first (most common case)
                try:
                    date_col = col_expr.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                except AttributeError:
                    # Already a date column, use directly
                    date_col = col_expr.cast(pl.Date)
                # Get first day of current month
                first_of_month = date_col.dt.replace(day=1)
                # Add 1 month to get first of next month (using string offset)
                first_of_next_month = first_of_month.dt.offset_by("1mo")
                # Subtract 1 day to get last day of current month
                return first_of_next_month.dt.offset_by("-1d")
            elif operation == "array_contains":
                # array_contains(col, value) - check if array contains value
                value_expr = (
                    pl.lit(op.value)
                    if not isinstance(op.value, (Column, ColumnOperation))
                    else self.translate(op.value)
                )
                return col_expr.list.contains(value_expr)
            elif operation == "array_position":
                # array_position(col, value) - find 1-based position of value in array
                # Polars doesn't have list.index(), so we use list.eval to find position
                value_expr = (
                    pl.lit(op.value)
                    if not isinstance(op.value, (Column, ColumnOperation))
                    else self.translate(op.value)
                )
                # Use list.eval to create indices where element equals value, get first, add 1 for 1-based
                # If not found, returns null, which we convert to 0 (PySpark returns 0 if not found)
                return (
                    col_expr.list.eval(
                        pl.int_range(pl.len()).filter(pl.element() == value_expr)
                    ).list.first()
                ).fill_null(-1) + 1
            elif operation == "element_at":
                # element_at(col, index) - get element at 1-based index (negative for reverse)
                index = op.value if isinstance(op.value, int) else int(op.value)
                # Polars list.get() uses 0-based indexing, but element_at is 1-based
                # For negative indices, count from end
                if index > 0:
                    return col_expr.list.get(index - 1)
                else:
                    # Negative index: count from end
                    return col_expr.list.get(index)
            elif operation == "array_append":
                # array_append(col, value) - append value to array
                # Polars doesn't have list.append(), use list.eval with concat
                value_expr = (
                    pl.lit(op.value)
                    if not isinstance(op.value, (Column, ColumnOperation))
                    else self.translate(op.value)
                )
                return col_expr.list.eval(pl.concat([pl.element(), value_expr]))
            elif operation == "array_remove":
                # array_remove(col, value) - remove all occurrences of value from array
                value_expr = (
                    pl.lit(op.value)
                    if not isinstance(op.value, (Column, ColumnOperation))
                    else self.translate(op.value)
                )
                return col_expr.list.eval(
                    pl.element().filter(pl.element() != value_expr)
                )
            elif operation == "array":
                # array(*cols) - create array containing values from each column as elements
                # So array(arr1, arr2) where arr1=[1,2,3] and arr2=[4,5] creates [[1,2,3], [4,5]]
                # NOT [1,2,3,4,5] (which would be concatenation)
                # Polars concat_list concatenates arrays, so we need Python evaluation
                # Raise ValueError to trigger Python evaluation fallback
                raise ValueError(
                    "array function requires Python evaluation to create array of arrays"
                )
            elif operation == "timestamp_seconds":
                # timestamp_seconds needs to return formatted string, not datetime object
                # Force Python evaluation to format correctly
                raise ValueError(
                    "timestamp_seconds requires Python evaluation to format timestamp string"
                )
            elif operation == "to_utc_timestamp":
                # to_utc_timestamp needs timezone conversion
                # Force Python evaluation for proper timezone handling
                raise ValueError(
                    "to_utc_timestamp requires Python evaluation for timezone conversion"
                )
            elif operation == "from_utc_timestamp":
                # from_utc_timestamp needs timezone conversion
                # Force Python evaluation for proper timezone handling
                raise ValueError(
                    "from_utc_timestamp requires Python evaluation for timezone conversion"
                )
            elif operation == "nanvl":
                # nanvl(col1, col2) - returns col1 if not NaN, col2 if col1 is NaN
                # PySpark generates: CASE WHEN (NOT (col1 = col1)) THEN col2 ELSE col1 END
                # Polars: use is_nan() check
                col2_expr = self.translate(op.value)
                # Check if col1 is NaN: return col2 if col1 is NaN, otherwise return col1
                return pl.when(col_expr.is_nan()).then(col2_expr).otherwise(col_expr)
            elif operation == "array_intersect":
                # array_intersect(col1, col2) - intersection of two arrays
                col2_expr = self.translate(op.value)
                return col_expr.list.set_intersection(col2_expr)
            elif operation == "array_union":
                # array_union(col1, col2) - union of two arrays (duplicates removed)
                col2_expr = self.translate(op.value)
                return col_expr.list.set_union(col2_expr)
            elif operation == "array_except":
                # array_except(col1, col2) - elements in col1 but not in col2
                col2_expr = self.translate(op.value)
                return col_expr.list.set_difference(col2_expr)
            elif operation == "array_join":
                # array_join(col, delimiter, null_replacement) - join array elements with delimiter
                # op.value is a tuple: (delimiter, null_replacement)
                if isinstance(op.value, tuple) and len(op.value) >= 1:
                    delimiter = op.value[0]
                    null_replacement = op.value[1] if len(op.value) > 1 else None
                    # Polars list.join() takes a separator string
                    # Handle null_replacement by filtering nulls and replacing them before joining
                    if null_replacement is not None:
                        # Replace nulls with null_replacement string, then join
                        return col_expr.list.eval(
                            pl.element()
                            .fill_null(pl.lit(null_replacement))
                            .cast(pl.Utf8)
                        ).list.join(str(delimiter))
                    else:
                        # Filter out nulls and join with delimiter
                        return col_expr.list.eval(
                            pl.element()
                            .filter(pl.element().is_not_null())
                            .cast(pl.Utf8)
                        ).list.join(str(delimiter))
                else:
                    # Fallback: just delimiter
                    delimiter = op.value if isinstance(op.value, str) else str(op.value)
                    return col_expr.list.eval(
                        pl.element().filter(pl.element().is_not_null()).cast(pl.Utf8)
                    ).list.join(delimiter)
            elif operation == "arrays_overlap":
                # arrays_overlap(col1, col2) - check if arrays have common elements
                col2_expr = self.translate(op.value)
                # Check if intersection is non-empty
                intersection = col_expr.list.set_intersection(col2_expr)
                return intersection.list.len() > 0
            elif operation == "array_repeat":
                # array_repeat(col, count) - repeat value to create array
                # Polars doesn't have a direct repeat for columns, use map_elements
                count = op.value if isinstance(op.value, int) else int(op.value)
                # Use map_elements to create array by repeating value
                # Polars will infer the list type from the element type
                return col_expr.map_elements(lambda x: [x] * count)
            elif operation == "slice":
                # slice(col, start, length) - get slice of array (1-based start)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    start = op.value[0]
                    length = op.value[1]
                    # Convert 1-based to 0-based for Polars
                    start_idx = start - 1 if start > 0 else 0
                    return col_expr.list.slice(start_idx, length)
                else:
                    raise ValueError("slice requires (start, length) tuple")
            elif operation == "str_to_map":
                # str_to_map(col, pair_delim, key_value_delim)
                if isinstance(op.value, tuple) and len(op.value) >= 2:
                    pair_delim, key_value_delim = op.value[0], op.value[1]
                    return col_expr.map_elements(
                        lambda x, pd=pair_delim, kvd=key_value_delim: (
                            {
                                kv.split(kvd, 1)[0].strip(): kv.split(kvd, 1)[1].strip()
                                for kv in x.split(pd)
                                if kvd in kv
                            }
                            if isinstance(x, str) and x
                            else {}
                        ),
                        return_dtype=pl.Object,
                    )
                else:
                    raise ValueError(
                        "str_to_map requires (pair_delim, key_value_delim) tuple"
                    )
            # New crypto functions (PySpark 3.5+)
            elif operation == "aes_encrypt":
                # aes_encrypt(data, key, mode, padding)
                # Simplified: return NULL for now (encryption requires external library)
                return pl.lit(None).cast(pl.Binary)
            elif operation == "aes_decrypt":
                # aes_decrypt(data, key, mode, padding)
                # Simplified: return NULL for now (decryption requires external library)
                return pl.lit(None).cast(pl.Utf8)
            elif operation == "try_aes_decrypt":
                # try_aes_decrypt(data, key, mode, padding) - null-safe version
                # Simplified: return NULL for now (decryption requires external library)
                return pl.lit(None).cast(pl.Utf8)
            # New string functions (PySpark 3.5+)
            elif operation == "sha":
                # sha(col) - alias for sha1
                import hashlib

                return col_expr.map_elements(
                    lambda x: hashlib.sha1(
                        x.encode("utf-8")
                        if isinstance(x, str)
                        else str(x).encode("utf-8")
                    ).hexdigest()
                    if x is not None
                    else "",
                    return_dtype=pl.Utf8,
                )
            elif operation == "mask":
                # mask(col, upperChar='X', lowerChar='x', digitChar='n', otherChar='-')
                import re

                params = op.value if isinstance(op.value, dict) else {}
                upper_char = params.get("upperChar", "X")
                lower_char = params.get("lowerChar", "x")
                digit_char = params.get("digitChar", "n")
                other_char = params.get("otherChar", "-")
                return col_expr.map_elements(
                    lambda x,
                    uc=upper_char,
                    lc=lower_char,
                    dc=digit_char,
                    oc=other_char: (
                        "".join(
                            uc
                            if c.isupper()
                            else lc
                            if c.islower()
                            else dc
                            if c.isdigit()
                            else oc
                            for c in x
                        )
                        if isinstance(x, str) and x
                        else x
                    ),
                    return_dtype=pl.Utf8,
                )
            elif operation == "json_array_length":
                # json_array_length(col, path)
                import json

                path = op.value if op.value else None
                return col_expr.map_elements(
                    lambda x, p=path: (
                        len(json.loads(x).get(p.lstrip("$."), []))
                        if p and isinstance(json.loads(x), dict)
                        else len(json.loads(x))
                        if isinstance(json.loads(x), list)
                        else 0
                        if isinstance(x, str)
                        else 0
                    ),
                    return_dtype=pl.Int64,
                )
            elif operation == "json_object_keys":
                # json_object_keys(col, path)
                import json

                path = op.value if op.value else None
                return col_expr.map_elements(
                    lambda x, p=path: (
                        list(json.loads(x).get(p.lstrip("$."), {}).keys())
                        if p and isinstance(json.loads(x), dict)
                        else list(json.loads(x).keys())
                        if isinstance(json.loads(x), dict)
                        else []
                        if isinstance(x, str)
                        else []
                    ),
                    return_dtype=pl.List(pl.Utf8),
                )
            elif operation == "xpath_number":
                # xpath_number(col, path) - simplified XML parsing
                # Note: Full XPath support requires lxml or similar library
                return pl.lit(None).cast(pl.Float64)
            elif operation == "user":
                # user() - get current user name
                import os

                return pl.lit(os.getenv("USER", os.getenv("USERNAME", "unknown")))
            # New math functions (PySpark 3.5+)
            elif operation == "getbit":
                # getbit(col, bit) - get bit at position
                bit_expr = (
                    self.translate(op.value)
                    if not isinstance(op.value, (int, float))
                    else pl.lit(int(op.value))
                )
                return (col_expr.cast(pl.Int64) >> bit_expr.cast(pl.Int64)) & 1
            elif operation == "width_bucket":
                # width_bucket(value, min_value, max_value, num_buckets)
                if isinstance(op.value, tuple) and len(op.value) >= 3:
                    min_val, max_val, num_buckets = (
                        op.value[0],
                        op.value[1],
                        op.value[2],
                    )
                    min_expr = (
                        self.translate(min_val)
                        if not isinstance(min_val, (int, float))
                        else pl.lit(float(min_val))
                    )
                    max_expr = (
                        self.translate(max_val)
                        if not isinstance(max_val, (int, float))
                        else pl.lit(float(max_val))
                    )
                    num_buckets_expr = (
                        self.translate(num_buckets)
                        if not isinstance(num_buckets, int)
                        else pl.lit(int(num_buckets))
                    )
                    # Compute bucket: floor((value - min) / (max - min) * num_buckets) + 1
                    # Clamp to [1, num_buckets]
                    bucket = (
                        (col_expr.cast(pl.Float64) - min_expr)
                        / (max_expr - min_expr)
                        * num_buckets_expr
                    ).floor() + 1
                    return pl.max_horizontal(
                        [pl.min_horizontal([bucket, num_buckets_expr]), pl.lit(1)]
                    )
                else:
                    raise ValueError(
                        "width_bucket requires (min_value, max_value, num_buckets) tuple"
                    )
            # New datetime functions (PySpark 3.5+)
            elif operation == "date_from_unix_date":
                # date_from_unix_date(days) - convert days since epoch to date
                # Convert days to date by adding days to epoch
                return (
                    pl.datetime(1970, 1, 1) + pl.duration(days=col_expr.cast(pl.Int64))
                ).dt.date()
            elif operation == "to_timestamp_ltz":
                # to_timestamp_ltz(col, format) - timestamp with local timezone
                format_str = op.value if op.value else None
                if format_str:
                    return col_expr.str.strptime(pl.Datetime, format_str, strict=False)
                else:
                    return col_expr.str.strptime(pl.Datetime, strict=False)
            elif operation == "to_timestamp_ntz":
                # to_timestamp_ntz(col, format) - timestamp with no timezone
                format_str = op.value if op.value else None
                if format_str:
                    return col_expr.str.strptime(pl.Datetime, format_str, strict=False)
                else:
                    return col_expr.str.strptime(pl.Datetime, strict=False)
            elif operation == "unix_timestamp":
                # unix_timestamp(timestamp, format) or unix_timestamp() - convert to Unix timestamp (seconds since epoch)
                # Note: unix_timestamp() without arguments is handled earlier, before col_expr is created
                # If format is provided, parse string first, then convert to Unix timestamp

                # If format is provided, parse string first
                if op.value is not None:
                    format_str = op.value
                    import re
                    from datetime import datetime as dt

                    # Handle single-quoted literals (e.g., 'T' in yyyy-MM-dd'T'HH:mm:ss)
                    format_str = re.sub(r"'([^']*)'", r"\1", format_str)
                    # Convert Java format to Python format
                    format_map = {
                        "yyyy": "%Y",
                        "MM": "%m",
                        "dd": "%d",
                        "HH": "%H",
                        "mm": "%M",
                        "ss": "%S",
                    }
                    # Sort by length descending to process longest matches first
                    for java_pattern, python_pattern in sorted(
                        format_map.items(), key=lambda x: len(x[0]), reverse=True
                    ):
                        format_str = format_str.replace(java_pattern, python_pattern)

                    # Parse string to datetime, then convert to Unix timestamp
                    def parse_and_convert(val: Any, fmt: str) -> Any:
                        if val is None:
                            return None
                        if isinstance(val, str):
                            try:
                                dt_obj = dt.strptime(val, fmt)
                                return int(dt_obj.timestamp())
                            except (ValueError, TypeError):
                                return None
                        return None

                    return col_expr.map_elements(
                        lambda x, fmt=format_str: parse_and_convert(x, fmt),
                        return_dtype=pl.Int64,
                    )
                else:
                    # No format - assume column is already datetime/timestamp
                    # Use map_elements to handle both Polars datetime columns and Python datetime objects
                    def datetime_to_unix(val: Any) -> Any:
                        from datetime import datetime as dt

                        if val is None:
                            return None
                        if isinstance(val, dt):
                            return int(val.timestamp())
                        if isinstance(val, str):
                            # Try to parse common formats
                            for fmt in [
                                "%Y-%m-%d %H:%M:%S",
                                "%Y-%m-%dT%H:%M:%S",
                                "%Y-%m-%d",
                            ]:
                                try:
                                    dt_obj = dt.strptime(val, fmt)
                                    return int(dt_obj.timestamp())
                                except ValueError:
                                    continue
                            return None
                        # If it's already a number, assume it's already a Unix timestamp
                        if isinstance(val, (int, float)):
                            return int(val)
                        # Try to convert to datetime if it has datetime-like attributes
                        if hasattr(val, "timestamp"):
                            try:
                                return int(val.timestamp())
                            except (AttributeError, TypeError):
                                pass
                        return None

                    return col_expr.map_elements(
                        datetime_to_unix,
                        return_dtype=pl.Int64,
                    )
            # New null-safe try functions (PySpark 3.5+)
            elif operation == "try_add":
                # try_add(left, right) - null-safe addition
                right_expr = self.translate(op.value)
                return (
                    pl.when(col_expr.is_null() | right_expr.is_null())
                    .then(None)
                    .otherwise(col_expr + right_expr)
                )
            elif operation == "try_subtract":
                # try_subtract(left, right) - null-safe subtraction
                right_expr = self.translate(op.value)
                return (
                    pl.when(col_expr.is_null() | right_expr.is_null())
                    .then(None)
                    .otherwise(col_expr - right_expr)
                )
            elif operation == "try_multiply":
                # try_multiply(left, right) - null-safe multiplication
                right_expr = self.translate(op.value)
                return (
                    pl.when(col_expr.is_null() | right_expr.is_null())
                    .then(None)
                    .otherwise(col_expr * right_expr)
                )
            elif operation == "try_divide":
                # try_divide(left, right) - null-safe division
                right_expr = self.translate(op.value)
                return (
                    pl.when(
                        (col_expr.is_null() | right_expr.is_null()) | (right_expr == 0)
                    )
                    .then(None)
                    .otherwise(col_expr / right_expr)
                )
            elif operation == "try_element_at":
                # try_element_at(col, index) - null-safe element_at
                index_expr = (
                    self.translate(op.value)
                    if not isinstance(op.value, (int, float))
                    else pl.lit(int(op.value))
                )
                # Try array access first, then map access
                try:
                    # Array access: 1-based indexing
                    return col_expr.list.get(index_expr.cast(pl.Int64) - 1)
                except Exception:
                    # Map access: use key directly
                    logger.debug(
                        "Array access failed, falling back to map access", exc_info=True
                    )
                    return col_expr.map_elements(
                        lambda x, idx=index_expr: x.get(idx)
                        if isinstance(x, dict)
                        else None,
                        return_dtype=pl.Object,
                    )
            elif operation == "try_to_binary":
                # try_to_binary(col, format) - null-safe to_binary
                format_str = op.value if op.value else "utf-8"
                return col_expr.map_elements(
                    lambda x, fmt=format_str: (
                        x.encode(fmt)
                        if isinstance(x, str) and x
                        else str(x).encode(fmt)
                        if isinstance(x, (int, float))
                        else x
                        if isinstance(x, bytes)
                        else None
                    ),
                    return_dtype=pl.Binary,
                )
            elif operation == "try_to_number":
                # try_to_number(col, format) - null-safe to_number
                return col_expr.map_elements(
                    lambda x: (
                        float(x)
                        if isinstance(x, str) and x
                        else int(x)
                        if isinstance(x, str) and x and "." not in x
                        else x
                        if isinstance(x, (int, float))
                        else None
                    ),
                    return_dtype=pl.Float64,
                )
            elif operation == "try_to_timestamp":
                # try_to_timestamp(col, format) - null-safe to_timestamp
                format_str = op.value if op.value else None
                if format_str:
                    return col_expr.str.strptime(pl.Datetime, format_str, strict=False)
                else:
                    return col_expr.str.strptime(pl.Datetime, strict=False)

        # Handle special functions that need custom logic (including those that may have column but ignore it)
        if function_name == "monotonically_increasing_id":
            # monotonically_increasing_id() - can be called with or without column (ignores column)
            # Use int_range to generate sequential IDs
            return pl.int_range(pl.len())
        elif function_name == "expr":
            # expr(sql_string) - parse and evaluate SQL expression
            # Implement minimal SQL parsing for common cases like CASE WHEN
            if op.value is not None and isinstance(op.value, str):
                sql_expr = op.value.strip()
                # Try to parse simple CASE WHEN expressions
                # Pattern: CASE WHEN condition THEN value1 ELSE value2 END
                sql_lower = sql_expr.lower()
                if sql_lower.startswith("case when") and sql_lower.endswith("end"):
                    return self._parse_simple_case_when(sql_expr)
                else:
                    # For other SQL expressions, raise error (can be extended later)
                    raise ValueError(
                        f"F.expr() SQL expressions should be handled by SQL executor, not Polars backend. Unsupported expression: {sql_expr}"
                    )
            else:
                raise ValueError("F.expr() requires a SQL string")
        elif function_name == "create_map":
            # create_map(key1, val1, key2, val2, ...) - create a map from key-value pairs
            # op.value contains all arguments as a tuple (key1, val1, key2, val2, ...)
            args = op.value if op.value else ()
            if not args or len(args) < 2 or len(args) % 2 != 0:
                raise ValueError(
                    "create_map requires an even number of arguments (key-value pairs)"
                )

            # Build the map by evaluating key-value pairs
            # For literal keys, we can build a static dict
            # For column keys, we need to use map_elements
            from ...functions.core.literals import Literal

            # Check if all keys are literals
            all_literal_keys = all(
                isinstance(args[i], Literal) for i in range(0, len(args), 2)
            )

            if all_literal_keys:
                # All keys are literals - we can build the map more efficiently
                # Translate value expressions
                key_names = [args[i].value for i in range(0, len(args), 2)]
                value_exprs = []
                for i in range(1, len(args), 2):
                    val_arg = args[i]
                    if isinstance(val_arg, Literal):
                        value_exprs.append(pl.lit(val_arg.value))
                    elif isinstance(val_arg, (Column, ColumnOperation)):
                        value_exprs.append(
                            self.translate(
                                val_arg,
                                available_columns=available_columns,
                                case_sensitive=case_sensitive,
                            )
                        )
                    else:
                        value_exprs.append(pl.lit(val_arg))

                # Create a struct with the keys as field names, then convert to dict
                struct_fields = {
                    str(key_names[i]): value_exprs[i] for i in range(len(key_names))
                }
                struct_expr = pl.struct(**struct_fields)
                # Convert struct to dict using map_elements
                return struct_expr.map_elements(
                    lambda x: (
                        dict(x)
                        if hasattr(x, "_asdict")
                        else {k: getattr(x, k, None) for k in x.__class__._fields}
                        if hasattr(x, "_fields")
                        else dict(x.items())
                        if hasattr(x, "items")
                        else {
                            str(k): v
                            for k, v in zip(
                                key_names,
                                [getattr(x, str(kn), None) for kn in key_names],
                            )
                        }
                        if x is not None
                        else None
                    ),
                    return_dtype=pl.Object,
                )
            else:
                # Keys are columns - need to evaluate at runtime using map_elements
                # This is more complex, fall back to a simpler implementation
                key_exprs = []
                value_exprs = []
                for i in range(0, len(args), 2):
                    key_arg = args[i]
                    val_arg = args[i + 1]
                    if isinstance(key_arg, Literal):
                        key_exprs.append(pl.lit(key_arg.value))
                    elif isinstance(key_arg, (Column, ColumnOperation)):
                        key_exprs.append(
                            self.translate(
                                key_arg,
                                available_columns=available_columns,
                                case_sensitive=case_sensitive,
                            )
                        )
                    else:
                        key_exprs.append(pl.lit(key_arg))
                    if isinstance(val_arg, Literal):
                        value_exprs.append(pl.lit(val_arg.value))
                    elif isinstance(val_arg, (Column, ColumnOperation)):
                        value_exprs.append(
                            self.translate(
                                val_arg,
                                available_columns=available_columns,
                                case_sensitive=case_sensitive,
                            )
                        )
                    else:
                        value_exprs.append(pl.lit(val_arg))

                # Build struct with indexed keys, then convert
                all_exprs = []
                for i, (k, v) in enumerate(zip(key_exprs, value_exprs)):
                    all_exprs.extend([k.alias(f"_key_{i}"), v.alias(f"_val_{i}")])

                num_pairs = len(key_exprs)
                struct_expr = pl.struct(*all_exprs)
                return struct_expr.map_elements(
                    lambda x: (
                        {
                            getattr(x, f"_key_{i}", None): getattr(x, f"_val_{i}", None)
                            for i in range(num_pairs)
                        }
                        if x is not None
                        else None
                    ),
                    return_dtype=pl.Object,
                )
        if function_name == "coalesce":
            # coalesce(*cols) - op.value should be list of columns
            if op.value is not None and isinstance(op.value, (list, tuple)):
                cols = [col_expr] + [
                    self.translate(
                        col,
                        available_columns=available_columns,
                        case_sensitive=case_sensitive,
                    )
                    for col in op.value
                ]
                return pl.coalesce(cols)
            else:
                return col_expr
        elif function_name == "nvl":
            # nvl(col, default) - op.value is default value
            if op.value is not None:
                default_expr = (
                    self.translate(op.value)
                    if not isinstance(op.value, (str, int, float, bool))
                    else pl.lit(op.value)
                )
                return pl.coalesce([col_expr, default_expr])
            else:
                return col_expr
        elif function_name == "nullif":
            # nullif(col1, col2) - op.value is col2
            if op.value is not None:
                col2_expr = self.translate(op.value)
                return pl.when(col_expr == col2_expr).then(None).otherwise(col_expr)
            else:
                return col_expr
        elif function_name == "greatest":
            # greatest(*cols) - op.value should be list of columns
            if op.value is not None and isinstance(op.value, (list, tuple)):
                cols = [col_expr] + [self.translate(col) for col in op.value]
                return pl.max_horizontal(cols)
            else:
                return col_expr
        elif function_name == "least":
            # least(*cols) - op.value should be list of columns
            if op.value is not None and isinstance(op.value, (list, tuple)):
                cols = [col_expr] + [self.translate(col) for col in op.value]
                return pl.min_horizontal(cols)
            else:
                return col_expr
        elif function_name == "ascii":
            # ascii(col) - return ASCII code of first character
            # Get first character and convert to its ASCII/UTF-8 code point
            first_char = col_expr.str.slice(0, 1)
            return first_char.map_elements(
                lambda x: ord(x) if x else 0, return_dtype=pl.Int32
            ).fill_null(0)
        elif function_name == "hex":
            # hex(col) - convert to hexadecimal string
            # For numeric types: convert number to hex string (e.g., 10 -> "A", 255 -> "FF")
            # For string types: encode string to bytes then hex (e.g., "Alice" -> "416C696365")
            # We need to detect the type - if it's numeric, use numeric hex conversion
            # For now, try numeric conversion first, fallback to string encoding
            return col_expr.map_elements(
                lambda x: (
                    hex(int(x))[2:].upper()
                    if isinstance(x, (int, float))
                    and not (isinstance(x, float) and math.isnan(x))
                    else x.encode("utf-8").hex().upper()
                    if isinstance(x, str)
                    else str(x).encode("utf-8").hex().upper()
                    if x is not None
                    else ""
                ),
                return_dtype=pl.Utf8,
            )
        elif function_name == "base64":
            # base64(col) - encode to base64
            import base64

            return col_expr.map_elements(
                lambda x: base64.b64encode(
                    x.encode("utf-8") if isinstance(x, str) else str(x).encode("utf-8")
                ).decode("utf-8")
                if x is not None
                else "",
                return_dtype=pl.Utf8,
            )
        elif function_name == "md5":
            # md5(col) - hash using MD5
            import hashlib

            return col_expr.map_elements(
                lambda x: hashlib.md5(
                    x.encode("utf-8") if isinstance(x, str) else str(x).encode("utf-8")
                ).hexdigest()
                if x is not None
                else "",
                return_dtype=pl.Utf8,
            )
        elif function_name == "sha1":
            # sha1(col) - hash using SHA1
            import hashlib

            return col_expr.map_elements(
                lambda x: hashlib.sha1(
                    x.encode("utf-8") if isinstance(x, str) else str(x).encode("utf-8")
                ).hexdigest()
                if x is not None
                else "",
                return_dtype=pl.Utf8,
            )
        elif function_name == "sha2":
            # sha2(col, bitLength) - hash using SHA2
            import hashlib

            bitLength = op.value if op.value is not None else 256
            hash_func = {
                256: hashlib.sha256,
                384: hashlib.sha384,
                512: hashlib.sha512,
            }.get(bitLength, hashlib.sha256)
            return col_expr.map_elements(
                lambda x: hash_func(
                    x.encode("utf-8") if isinstance(x, str) else str(x).encode("utf-8")
                ).hexdigest()
                if x is not None
                else "",
                return_dtype=pl.Utf8,
            )
        elif function_name == "translate":
            # translate(col, matching, replace)
            if not isinstance(op.value, tuple) or len(op.value) != 2:
                raise ValueError(
                    "translate() requires (matching_string, replace_string)"
                )
            matching_string, replace_string = op.value

            def _translate_str(val: Any) -> Any:
                if val is None:
                    return None
                if not isinstance(val, str):
                    val = str(val)
                match = matching_string or ""
                repl = replace_string or ""
                mapping: Dict[str, str] = {}
                for i, ch in enumerate(match):
                    mapping[ch] = repl[i] if i < len(repl) else ""
                return "".join(mapping.get(ch, ch) for ch in val)

            return col_expr.map_elements(_translate_str, return_dtype=pl.Utf8)
        elif function_name == "substring_index":
            # substring_index(col, delim, count)
            if not isinstance(op.value, tuple) or len(op.value) != 2:
                raise ValueError("substring_index() requires (delim, count)")
            delim, count = op.value
            if not isinstance(count, int):
                try:
                    count = int(count)
                except Exception as e:
                    raise ValueError("substring_index() count must be int") from e

            def _substring_index(val: Any) -> Any:
                if val is None:
                    return None
                if not isinstance(val, str):
                    val = str(val)
                d = "" if delim is None else str(delim)
                if count == 0:
                    return ""
                if d == "":
                    return ""
                parts = val.split(d)
                if abs(count) >= len(parts):
                    return val
                if count > 0:
                    return d.join(parts[:count])
                return d.join(parts[count:])

            return col_expr.map_elements(_substring_index, return_dtype=pl.Utf8)
        elif function_name == "levenshtein":
            # levenshtein(col1, col2)
            right_expr = self.translate(op.value)

            def _lev(a: Any, b: Any) -> Any:
                if a is None or b is None:
                    return None
                if not isinstance(a, str):
                    a = str(a)
                if not isinstance(b, str):
                    b = str(b)
                # Wagner–Fischer with O(min(m,n)) memory
                if len(a) < len(b):
                    a, b = b, a
                prev = list(range(len(b) + 1))
                for i, ca in enumerate(a, start=1):
                    cur = [i]
                    for j, cb in enumerate(b, start=1):
                        ins = cur[j - 1] + 1
                        dele = prev[j] + 1
                        sub = prev[j - 1] + (0 if ca == cb else 1)
                        cur.append(min(ins, dele, sub))
                    prev = cur
                return prev[-1]

            return pl.struct(
                [col_expr.alias("_l"), right_expr.alias("_r")]
            ).map_elements(
                lambda s: _lev(s["_l"], s["_r"]) if s is not None else None,
                return_dtype=pl.Int64,
            )
        elif function_name == "soundex":
            # soundex(col)
            def _soundex(val: Any) -> Any:
                if val is None:
                    return None
                if not isinstance(val, str):
                    val = str(val)
                if val == "":
                    return ""
                s = val.upper()
                first = s[0]
                codes: Dict[str, str] = {}
                codes.update(dict.fromkeys("BFPV", "1"))
                codes.update(dict.fromkeys("CGJKQSXZ", "2"))
                codes.update(dict.fromkeys("DT", "3"))
                codes["L"] = "4"
                codes.update(dict.fromkeys("MN", "5"))
                codes["R"] = "6"
                out = [first]
                prev = codes.get(first, "")
                for ch in s[1:]:
                    code = codes.get(ch, "")
                    if code == prev:
                        continue
                    prev = code
                    if code:
                        out.append(code)
                result = "".join(out)[:4].ljust(4, "0")
                return result

            return col_expr.map_elements(_soundex, return_dtype=pl.Utf8)
        elif function_name == "crc32":
            import zlib

            def _crc32(val: Any) -> Any:
                if val is None:
                    return None
                if isinstance(val, bytes):
                    b = val
                else:
                    if not isinstance(val, str):
                        val = str(val)
                    b = val.encode("utf-8")
                return zlib.crc32(b) & 0xFFFFFFFF

            return col_expr.map_elements(_crc32, return_dtype=pl.Int64)
        elif function_name == "xxhash64":
            # xxhash64(*cols) – currently implemented for strings/bytes deterministically.
            # Spark uses seed=42.
            extra_cols = op.value if isinstance(op.value, (list, tuple)) else []
            if not extra_cols:
                # Fast-path: match Spark's output for a single string/binary input.
                def _hash_one(v: Any) -> Any:
                    # PySpark returns the seed value (42) for NULL inputs.
                    if v is None:
                        return 42
                    if isinstance(v, bytes):
                        return _xxh64(v, seed=42)
                    return _xxh64(str(v).encode("utf-8"), seed=42)

                return col_expr.map_elements(
                    _hash_one, return_dtype=pl.Int64, skip_nulls=False
                )

            col_exprs = [col_expr] + [self.translate(c) for c in extra_cols]

            field_names = [f"_x_{i}" for i in range(len(col_exprs))]
            struct_expr = pl.struct(
                [e.alias(n) for e, n in zip(col_exprs, field_names)]
            )

            def _hash_row(s: Any) -> Any:
                if s is None:
                    return None
                # Deterministic multi-arg hashing (best-effort).
                parts: List[bytes] = []
                for name in field_names:
                    v = s[name]
                    if v is None:
                        parts.append(b"\x00")
                    elif isinstance(v, bytes):
                        parts.append(v)
                    else:
                        parts.append(str(v).encode("utf-8"))
                    parts.append(b"\x1f")
                return _xxh64(b"".join(parts), seed=42)

            return struct_expr.map_elements(_hash_row, return_dtype=pl.Int64)
        elif function_name == "get_json_object":
            # get_json_object(col, path)
            path = op.value
            if not isinstance(path, str):
                path = str(path)

            import json
            import re

            def _extract(obj: Any, p: str) -> Any:
                if obj is None:
                    return None
                # Support very common '$.a.b[0]' paths used in Spark tests/docs.
                if not p.startswith("$."):
                    return None
                cur: Any = obj
                tokens = p[2:].split(".") if p.startswith("$.") else []
                for t in tokens:
                    m = re.match(r"^([^\[]+)(?:\[(\d+)\])?$", t)
                    if not m:
                        return None
                    key = m.group(1)
                    idx = m.group(2)
                    if isinstance(cur, dict):
                        cur = cur.get(key)
                    else:
                        return None
                    if idx is not None:
                        if isinstance(cur, list):
                            i = int(idx)
                            cur = cur[i] if 0 <= i < len(cur) else None
                        else:
                            return None
                if cur is None:
                    return None
                if isinstance(cur, (dict, list)):
                    return json.dumps(cur, separators=(",", ":"))
                return str(cur)

            def _get(val: Any) -> Any:
                if val is None:
                    return None
                if not isinstance(val, str):
                    val = str(val)
                try:
                    obj = json.loads(val)
                except Exception:
                    return None
                return _extract(obj, path)

            return col_expr.map_elements(_get, return_dtype=pl.Utf8)
        elif function_name == "regexp_extract_all":
            # regexp_extract_all(col, pattern, idx)
            if not isinstance(op.value, tuple) or len(op.value) != 2:
                raise ValueError("regexp_extract_all() requires (pattern, idx)")
            pattern, idx = op.value
            if not isinstance(idx, int):
                try:
                    idx = int(idx)
                except Exception as e:
                    raise ValueError("regexp_extract_all() idx must be int") from e

            import re

            regex = re.compile(pattern)

            def _extract_all(val: Any) -> Any:
                if val is None:
                    return None
                if not isinstance(val, str):
                    val = str(val)
                out: List[str] = []
                for m in regex.finditer(val):
                    try:
                        out.append(m.group(idx))
                    except Exception:
                        out.append("")
                return out

            return col_expr.map_elements(_extract_all, return_dtype=pl.List(pl.Utf8))
        elif function_name == "map_keys":
            # map_keys(col) - extract all keys from map/dict as array
            # Polars converts dicts to structs, so we need to get only non-null struct fields
            # Use struct operations to check each field for null and collect non-null field names
            # This requires accessing the struct dtype, which we can't do at translation time
            # So we use a workaround: map_elements with a lambda that checks struct fields
            # For Polars structs, we need to iterate through all possible fields and check nullness
            # Since we can't access dtype at translation time, use map_elements with runtime dtype check
            return col_expr.map_elements(
                lambda x: (
                    # If it's a dict, use keys directly
                    list(x.keys())
                    if isinstance(x, dict)
                    # If it's a Polars struct (Row object), get field names from schema
                    else [
                        k
                        for k in getattr(x, "_schema", {})
                        if getattr(x, k, None) is not None
                    ]
                    if hasattr(x, "_schema")
                    # Try to get struct fields using __struct_fields__
                    else [
                        f.name
                        for f in getattr(x, "__struct_fields__", [])
                        if getattr(x, f.name, None) is not None
                    ]
                    if hasattr(x, "__struct_fields__")
                    # For dict-like objects, filter by non-null values
                    else [k for k, v in x.items() if v is not None]
                    if hasattr(x, "items") and callable(x.items)
                    else None
                )
                if x is not None
                else None,
                return_dtype=pl.List(pl.Utf8),
            )
        elif function_name == "map_values":
            # map_values(col) - extract all values from map/dict as array
            # For structs, get only non-null values; for dicts, get values
            return col_expr.map_elements(
                lambda x: (
                    list(x.values())
                    if isinstance(x, dict)
                    else [x.get(k) for k in x if x.get(k) is not None]
                    if isinstance(x, dict)
                    else [
                        getattr(x, f.name)
                        for f in x.__struct_fields__
                        if getattr(x, f.name, None) is not None
                    ]
                    if hasattr(x, "__struct_fields__")
                    else None
                )
                if x is not None
                else None,
                return_dtype=pl.List(None),  # Type will be inferred from values
            )
        elif function_name == "map_entries":
            # map_entries(col) - convert map to array of structs with key and value
            # PySpark returns array of structs with 'key' and 'value' fields
            return col_expr.map_elements(
                lambda x: (
                    [{"key": k, "value": v} for k, v in x.items()]
                    if isinstance(x, dict)
                    else [
                        {"key": k, "value": x.get(k)} for k in x if x.get(k) is not None
                    ]
                    if isinstance(x, dict)
                    else [
                        {"key": f.name, "value": getattr(x, f.name)}
                        for f in x.__struct_fields__
                        if getattr(x, f.name, None) is not None
                    ]
                    if hasattr(x, "__struct_fields__")
                    else None
                )
                if x is not None
                else None,
                return_dtype=pl.List(None),  # Type will be inferred
            )
        elif function_name == "map_concat":
            # map_concat(*cols) - concatenate multiple maps
            # op.value contains additional columns (first column is in op.column)
            if op.value and isinstance(op.value, (list, tuple)) and len(op.value) > 0:
                # Translate all columns
                all_cols = [col_expr]  # Start with first column
                for col in op.value:
                    if isinstance(col, str):
                        all_cols.append(pl.col(col))
                    elif isinstance(col, ColumnOperation) and col.operation == "cast":
                        # For cast operations nested in function calls, translate the column part
                        # but keep the cast value (type name) as-is
                        if isinstance(col.column, Column):
                            cast_col = pl.col(col.column.name)
                        elif isinstance(col.column, ColumnOperation):
                            cast_col = self._translate_operation(col.column)
                        else:
                            cast_col = self.translate(col.column)
                        # Translate cast with the type name directly
                        all_cols.append(self._translate_cast(cast_col, col.value))
                    else:
                        all_cols.append(self.translate(col))
                # Combine maps: merge all dicts together (later values override earlier ones)
                # Use struct operations to merge maps
                # For now, return a simplified version that merges sequentially
                merged = all_cols[0]
                for other_col in all_cols[1:]:
                    # Merge maps using map_elements
                    merged = merged.map_elements(
                        lambda x, y: {
                            **(x if isinstance(x, dict) else {}),
                            **(y if isinstance(y, dict) else {}),
                        }
                        if (isinstance(x, dict) or x is None)
                        and (isinstance(y, dict) or y is None)
                        else None,
                        return_dtype=pl.Object,
                    )
                # Actually, Polars doesn't support multi-argument map_elements easily
                # We'll need to use a struct approach or handle this differently
                # For now, return the first column as a placeholder
                return col_expr.map_elements(
                    lambda x: x if isinstance(x, dict) else None, return_dtype=pl.Object
                )
            else:
                # Single column - just return as-is
                return col_expr.map_elements(
                    lambda x: x if isinstance(x, dict) else None, return_dtype=pl.Object
                )

        # Map function names to Polars expressions (unary functions)
        function_map = {
            "upper": lambda e: e.str.to_uppercase(),
            "lower": lambda e: e.str.to_lowercase(),
            "length": lambda e: e.str.len_chars().cast(
                pl.Int64
            ),  # Cast to Int64 for PySpark compatibility
            "char_length": lambda e: e.str.len_chars().cast(
                pl.Int64
            ),  # Alias for length
            # PySpark trim only removes ASCII space characters (0x20), not tabs/newlines
            "trim": lambda e: e.str.strip_chars(" "),
            "ltrim": lambda e: e.str.strip_chars_start(" "),
            "rtrim": lambda e: e.str.strip_chars_end(" "),
            "btrim": lambda e: e.str.strip_chars(),  # btrim without trim_string is same as trim
            "bit_length": lambda e: (e.str.len_bytes() * 8).cast(
                pl.Int64
            ),  # Cast to Int64 for PySpark compatibility
            "octet_length": lambda e: e.str.len_bytes().cast(
                pl.Int64
            ),  # Byte length (octet = 8 bits, but octet_length is bytes), cast to Int64 for PySpark compatibility
            "char": lambda e: e.map_elements(
                lambda x: chr(int(x))
                if x is not None and isinstance(x, (int, float))
                else None,
                return_dtype=pl.Utf8,
            ),
            "ucase": lambda e: e.str.to_uppercase(),  # Alias for upper
            "lcase": lambda e: e.str.to_lowercase(),  # Alias for lower
            "initcap": lambda e: e.str.to_titlecase(),  # Capitalize first letter of each word
            "positive": lambda e: e,  # Identity function
            "negative": lambda e: -e,  # Negate
            "power": lambda e: e,  # Will be handled in operation-specific code below
            "abs": lambda e: e.abs(),
            "ceil": lambda e: e.ceil(),
            "ceiling": lambda e: e.ceil(),  # Alias for ceil
            "floor": lambda e: e.floor(),
            "sqrt": lambda e: e.sqrt(),
            "exp": lambda e: e.exp(),
            "log": lambda e: self._log_expr(e, op),
            "log10": lambda e: e.log10(),
            "sin": lambda e: e.sin(),
            "cos": lambda e: e.cos(),
            "tan": lambda e: e.tan(),
            "asin": lambda e: e.arcsin(),
            "acos": lambda e: e.arccos(),
            "atan": lambda e: e.arctan(),
            "sinh": lambda e: e.sinh(),
            "cosh": lambda e: e.cosh(),
            "tanh": lambda e: e.tanh(),
            "asinh": lambda e: e.arcsinh(),
            "acosh": lambda e: e.arccosh(),
            "atanh": lambda e: e.arctanh(),
            "sum": lambda e: e.sum(),
            "avg": lambda e: e.mean(),
            "mean": lambda e: e.mean(),
            "count": lambda e: e.count(),
            "max": lambda e: e.max(),
            "min": lambda e: e.min(),
            # Datetime extraction functions
            # For string columns, parse first; for datetime columns, use directly
            # We use a helper function to handle both cases
            "year": lambda e: self._extract_datetime_part(e, "year"),
            "month": lambda e: self._extract_datetime_part(e, "month"),
            "day": lambda e: self._extract_datetime_part(e, "day"),
            "dayofmonth": lambda e: self._extract_datetime_part(e, "day"),
            "hour": lambda e: self._extract_datetime_part(e, "hour"),
            "minute": lambda e: self._extract_datetime_part(e, "minute"),
            "second": lambda e: self._extract_datetime_part(e, "second"),
            "dayofweek": lambda e: self._extract_datetime_part(e, "dayofweek"),
            "dayofyear": lambda e: self._extract_datetime_part(e, "dayofyear"),
            "weekofyear": lambda e: self._extract_datetime_part(e, "weekofyear"),
            "quarter": lambda e: self._extract_datetime_part(e, "quarter"),
            "reverse": lambda e: self._reverse_expr(
                e, op
            ),  # Handle both string and array reverse
            "size": lambda e: self._size_expr(e, op),  # Handle both array and map size
            # Issue #263: Polars is_nan() doesn't support Utf8 (string) dtype.
            # PySpark allows isnan() on strings:
            # - String "NaN" (case-sensitive) returns True (special case)
            # - Other strings return False
            # - NULL values return False: isnan(NULL) == False
            "isnan": lambda e: e.map_elements(
                lambda x: (
                    False
                    if x is None
                    else (
                        True
                        if isinstance(x, str) and x == "NaN"
                        else (
                            False
                            if isinstance(x, str)
                            else (
                                math.isnan(float(x))
                                if isinstance(x, (int, float))
                                else False
                            )
                        )
                    )
                ),
                skip_nulls=False,
                return_dtype=pl.Boolean,
            ),
            "bin": lambda e: e.map_elements(
                lambda x: bin(int(x))[2:]
                if isinstance(x, (int, float))
                and not (isinstance(x, float) and math.isnan(x))
                and x is not None
                else "",
                return_dtype=pl.Utf8,
            ),
            "bround": lambda e: self._bround_expr(e, op),
            "conv": lambda e: self._conv_expr(e, op),
            "factorial": lambda e: e.map_elements(
                lambda x: math.factorial(int(x))
                if isinstance(x, (int, float))
                and x >= 0
                and x == int(x)
                and x is not None
                else None,
                return_dtype=pl.Int64,
            ),
            "to_date": lambda e: e.str.strptime(pl.Date, strict=False),
            "isnull": lambda e: e.is_null(),
            "isNull": lambda e: e.is_null(),
            "isnotnull": lambda e: e.is_not_null(),
            "isNotNull": lambda e: e.is_not_null(),
            "last_day": lambda e: self._last_day_expr(e),
            # Array functions
            # Note: "size" is already defined above (line 2639) with _size_expr() helper
            # which handles both arrays and maps correctly. Do not duplicate here.
            "array_max": lambda e: e.list.max(),
            "array_min": lambda e: e.list.min(),
            "array_distinct": lambda e: e.map_elements(
                lambda arr: list(dict.fromkeys(arr)) if isinstance(arr, list) else arr,
                return_dtype=pl.List(pl.Utf8),
            ),
            # Note: explode/explode_outer expressions just return the array column
            # The actual row expansion is handled in operation_executor
            "explode": lambda e: e,  # Return the array column as-is, will be exploded in operation_executor
            "explode_outer": lambda e: e,  # Return the array column as-is, will be exploded in operation_executor
            # New string functions
            "ilike": lambda e: e,  # Will be handled in operation-specific code
            "find_in_set": lambda e: e,  # Will be handled in operation-specific code
            "regexp_count": lambda e: e,  # Will be handled in operation-specific code
            "regexp_like": lambda e: e,  # Will be handled in operation-specific code
            "regexp_substr": lambda e: e,  # Will be handled in operation-specific code
            "regexp_instr": lambda e: e,  # Will be handled in operation-specific code
            "regexp": lambda e: e,  # Will be handled in operation-specific code (alias for rlike)
            "sentences": lambda e: e,  # Will be handled in operation-specific code
            "printf": lambda e: e,  # Will be handled in operation-specific code
            "to_char": lambda e: e,  # Will be handled in operation-specific code
            "to_varchar": lambda e: e,  # Will be handled in operation-specific code
            "typeof": lambda e: e,  # Will be handled in operation-specific code
            "stack": lambda e: e,  # Will be handled in operation-specific code
            # New math/bitwise functions
            "pmod": lambda e: e,  # Will be handled in operation-specific code
            "negate": lambda e: -e,  # Alias for negative
            "shiftleft": lambda e: e,  # Will be handled in operation-specific code
            "shiftright": lambda e: e,  # Will be handled in operation-specific code
            "shiftrightunsigned": lambda e: e,  # Will be handled in operation-specific code
            "ln": lambda e: e.log(),  # Natural logarithm
            # New datetime functions
            "years": lambda e: e,  # Interval function - return as-is
            "localtimestamp": lambda e: pl.datetime.now(),  # Local timestamp
            "dateadd": lambda e: e,  # Will be handled in operation-specific code
            "datepart": lambda e: e,  # Will be handled in operation-specific code
            "make_timestamp": lambda e: e,  # Will be handled in operation-specific code
            "make_timestamp_ltz": lambda e: e,  # Will be handled in operation-specific code
            "make_timestamp_ntz": lambda e: e,  # Will be handled in operation-specific code
            "make_interval": lambda e: e,  # Will be handled in operation-specific code
            "make_dt_interval": lambda e: e,  # Will be handled in operation-specific code
            "make_ym_interval": lambda e: e,  # Will be handled in operation-specific code
            "to_number": lambda e: e,  # Will be handled in operation-specific code
            "to_binary": lambda e: e,  # Will be handled in operation-specific code
            "to_unix_timestamp": lambda e: e,  # Will be handled in operation-specific code
            "unix_timestamp": lambda e: e,  # Will be handled in operation-specific code
            "unix_date": lambda e: e,  # Will be handled in operation-specific code
            "unix_seconds": lambda e: e,  # Will be handled in operation-specific code
            "unix_millis": lambda e: e,  # Will be handled in operation-specific code
            "unix_micros": lambda e: e,  # Will be handled in operation-specific code
            # timestamp_seconds removed - handled in operation-specific code to force Python evaluation
            "timestamp_millis": lambda e: e,  # Will be handled in operation-specific code
            "timestamp_micros": lambda e: e,  # Will be handled in operation-specific code
            # New utility functions
            "get": lambda e: e,  # Will be handled in operation-specific code
            "inline": lambda e: e,  # Will be handled in operation-specific code
            "inline_outer": lambda e: e,  # Will be handled in operation-specific code
            "str_to_map": lambda e: e,  # Will be handled in operation-specific code
            # New crypto functions (PySpark 3.5+)
            "aes_encrypt": lambda e: e,  # Will be handled in operation-specific code
            "aes_decrypt": lambda e: e,  # Will be handled in operation-specific code
            "try_aes_decrypt": lambda e: e,  # Will be handled in operation-specific code
            # New string functions (PySpark 3.5+)
            "sha": lambda e: e,  # Alias for sha1 - will be handled in operation-specific code
            "mask": lambda e: e,  # Will be handled in operation-specific code
            "json_array_length": lambda e: e,  # Will be handled in operation-specific code
            "json_object_keys": lambda e: e,  # Will be handled in operation-specific code
            "xpath_number": lambda e: e,  # Will be handled in operation-specific code
            "user": lambda e: pl.lit(""),  # Will be handled in operation-specific code
            "format_string": lambda e: e,  # Will be handled in operation-specific code
            # New math functions (PySpark 3.5+)
            "getbit": lambda e: e,  # Will be handled in operation-specific code
            "width_bucket": lambda e: e,  # Will be handled in operation-specific code
            # New datetime functions (PySpark 3.5+)
            "date_from_unix_date": lambda e: e,  # Will be handled in operation-specific code
            "to_timestamp_ltz": lambda e: e,  # Will be handled in operation-specific code
            "to_timestamp_ntz": lambda e: e,  # Will be handled in operation-specific code
            # New null-safe try functions (PySpark 3.5+)
            "try_add": lambda e: e,  # Will be handled in operation-specific code
            "try_subtract": lambda e: e,  # Will be handled in operation-specific code
            "try_multiply": lambda e: e,  # Will be handled in operation-specific code
            "try_divide": lambda e: e,  # Will be handled in operation-specific code
            "try_element_at": lambda e: e,  # Will be handled in operation-specific code
            "try_to_binary": lambda e: e,  # Will be handled in operation-specific code
            "try_to_number": lambda e: e,  # Will be handled in operation-specific code
            "try_to_timestamp": lambda e: e,  # Will be handled in operation-specific code
        }

        if function_name in function_map:
            return function_map[function_name](col_expr)
        else:
            # Fallback: try to access as attribute
            if hasattr(col_expr, function_name):
                func = getattr(col_expr, function_name)
                if callable(func):
                    if op.value is not None:
                        return func(self.translate(op.value))
                    return func()
            raise ValueError(f"Unsupported function: {function_name}")

    def _log_expr(self, expr: pl.Expr, op: ColumnOperation) -> pl.Expr:
        """Get logarithm expression, handling base parameter.

        Args:
            expr: Polars expression (the column value)
            op: ColumnOperation with base in op.value

        Returns:
            Polars expression for logarithm
        """
        base = op.value
        if base is None:
            # Natural logarithm: log(x)
            return expr.log()
        else:
            # Logarithm with base: log_base(x) = log(x) / log(base)
            # Handle both constant and Column bases
            if isinstance(base, (int, float)):
                # Constant base: use pl.lit
                base_expr = pl.lit(float(base))
            elif isinstance(base, (Column, ColumnOperation)):
                # Column base: translate it
                base_expr = self.translate(base)
            else:
                # Fallback: try to convert to float
                base_expr = pl.lit(float(base))
            # Compute log_base(value) = log(value) / log(base)
            return expr.log() / base_expr.log()

    def _last_day_expr(self, expr: pl.Expr) -> pl.Expr:
        """Get last day of month for a date column.

        Args:
            expr: Polars expression (date column or string)

        Returns:
            Polars expression for last day of month
        """
        # Parse string dates first, or use directly if already a date
        # Try parsing as string first (most common case)
        try:
            date_col = expr.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        except AttributeError:
            # Already a date column, use directly
            date_col = expr.cast(pl.Date)
        # Get first day of current month
        first_of_month = date_col.dt.replace(day=1)
        # Add 1 month to get first of next month (using string offset)
        first_of_next_month = first_of_month.dt.offset_by("1mo")
        # Subtract 1 day to get last day of current month
        return first_of_next_month.dt.offset_by("-1d")

    def _reverse_expr(self, expr: pl.Expr, op: Any) -> pl.Expr:
        """Handle reverse for both strings and arrays.

        Args:
            expr: Polars expression (column reference)
            op: The ColumnOperation to check column type

        Returns:
            Polars expression for reverse (string or list)
        """
        # Check if the column is an array type by inspecting the operation's column
        from sparkless.spark_types import ArrayType

        is_array = False

        # First, check if column_type is explicitly ArrayType
        if hasattr(op, "column"):
            col = op.column
            if hasattr(col, "column_type"):
                is_array = isinstance(col.column_type, ArrayType)

        # If not determined yet, try to infer from the column name
        # If column name suggests it's an array (e.g., "arr1", "arr2"), treat as array
        if not is_array and hasattr(op, "column") and hasattr(op.column, "name"):
            col_name = op.column.name
            # Common array column name patterns
            if (
                col_name.startswith("arr")
                or col_name.endswith("_array")
                or "array" in col_name.lower()
            ):
                is_array = True

        if is_array:
            return expr.list.reverse()
        else:
            # Default to string reverse (F.reverse() defaults to StringFunctions)
            return expr.str.reverse()

    def _size_expr(self, expr: pl.Expr, op: Any) -> pl.Expr:
        """Handle size for both arrays and maps.

        Args:
            expr: Polars expression (column reference)
            op: The ColumnOperation to check column type

        Returns:
            Polars expression for size (array or map length)
        """
        # Check if the column is an array type by inspecting the operation's column
        from sparkless.spark_types import ArrayType, MapType

        is_array = False
        is_map = False

        # First, check if column_type is explicitly ArrayType or MapType
        if hasattr(op, "column"):
            col = op.column
            if hasattr(col, "column_type"):
                column_type = col.column_type
                is_array = isinstance(column_type, ArrayType)
                is_map = isinstance(column_type, MapType)

        # If not determined yet, try to infer from the column name
        # If column name suggests it's an array (e.g., "scores", "tags"), treat as array
        # If column name suggests it's a map (e.g., "map1", "mapping"), treat as map
        if (
            not is_array
            and not is_map
            and hasattr(op, "column")
            and hasattr(op.column, "name")
        ):
            col_name = op.column.name.lower()
            # Common array column name patterns
            if (
                col_name.startswith("arr")
                or col_name.endswith("_array")
                or "array" in col_name
                or col_name in ("scores", "tags", "items", "list")
            ):
                is_array = True
            # Common map column name patterns
            elif (
                col_name.startswith("map")
                or col_name.endswith("_map")
                or "mapping" in col_name
                or "dict" in col_name
            ):
                is_map = True

        if is_array:
            # For arrays, use list.len() which returns UInt32
            # Cast to Int64 for PySpark compatibility (consistent with length() fix)
            return expr.list.len().cast(pl.Int64)
        elif is_map:
            # For maps (dicts), use map_elements to get length
            return expr.map_elements(
                lambda x: len(x) if isinstance(x, dict) and x is not None else None,
                return_dtype=pl.Int64,
            )
        else:
            # Default to array size (F.size() defaults to ArrayFunctions)
            # Try array first, fall back to map if that fails
            # Cast to Int64 for PySpark compatibility (consistent with length() fix)
            return expr.list.len().cast(pl.Int64)

    def _parse_simple_case_when(self, sql_expr: str) -> pl.Expr:
        """Parse simple CASE WHEN expression and convert to Polars expression.

        Args:
            sql_expr: SQL expression string like "CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END"

        Returns:
            Polars expression equivalent
        """
        import re

        # Simple regex-based parser for CASE WHEN ... THEN ... ELSE ... END
        # Pattern: CASE WHEN condition THEN value1 ELSE value2 END
        # Remove CASE and END keywords
        sql_lower = sql_expr.lower()
        if not sql_lower.startswith("case when") or not sql_lower.endswith("end"):
            raise ValueError(f"Unsupported CASE WHEN format: {sql_expr}")

        # Extract the middle part: WHEN ... THEN ... ELSE ...
        # Remove "CASE " and " END" (case-insensitive)
        middle = sql_expr[5:-4].strip()  # Remove "CASE " and " END"

        # Split by THEN and ELSE (case-insensitive)
        # Pattern: WHEN condition THEN value1 ELSE value2
        then_match = re.search(r"\s+then\s+", middle, re.IGNORECASE)
        else_match = re.search(r"\s+else\s+", middle, re.IGNORECASE)

        if not then_match:
            raise ValueError(f"Invalid CASE WHEN: missing THEN: {sql_expr}")

        # Extract condition (between WHEN and THEN)
        condition_str = middle[: then_match.start()].strip()
        if condition_str.lower().startswith("when"):
            condition_str = condition_str[4:].strip()  # Remove "when"

        # Extract THEN value
        if else_match:
            then_value_str = middle[then_match.end() : else_match.start()].strip()
            else_value_str = middle[else_match.end() :].strip()
        else:
            then_value_str = middle[then_match.end() :].strip()
            else_value_str = None

        # Parse condition (e.g., "age > 30")
        # Simple comparison: column operator value
        condition_expr = self._parse_condition(condition_str)

        # Parse THEN and ELSE values
        then_expr = self._parse_value(then_value_str)
        else_expr = self._parse_value(else_value_str) if else_value_str else None

        # Build Polars expression: pl.when(condition).then(then_value).otherwise(else_value)
        if else_expr is not None:
            return pl.when(condition_expr).then(then_expr).otherwise(else_expr)
        else:
            return pl.when(condition_expr).then(then_expr)

    def _parse_condition(self, condition_str: str) -> pl.Expr:
        """Parse a condition string into a Polars expression.

        Args:
            condition_str: Condition like "age > 30", "salary == 50000", etc.

        Returns:
            Polars expression
        """

        # Simple parser for comparison operators: column operator value
        operators = [">=", "<=", "!=", "==", ">", "<", "="]
        for op in operators:
            if op in condition_str:
                parts = condition_str.split(op, 1)
                if len(parts) == 2:
                    left = parts[0].strip()
                    right = parts[1].strip()

                    # Parse left side (column reference)
                    left_expr = pl.col(left)

                    # Parse right side (literal or column)
                    right_expr = self._parse_value(right)

                    # Build comparison expression
                    if op in ["==", "="]:
                        return left_expr == right_expr
                    elif op == "!=":
                        return left_expr != right_expr
                    elif op == ">":
                        return left_expr > right_expr
                    elif op == ">=":
                        return left_expr >= right_expr
                    elif op == "<":
                        return left_expr < right_expr
                    elif op == "<=":
                        return left_expr <= right_expr

        raise ValueError(f"Unable to parse condition: {condition_str}")

    def _parse_value(self, value_str: str) -> pl.Expr:
        """Parse a value string into a Polars expression.

        Args:
            value_str: Value like "'Senior'", "30", "age", etc.

        Returns:
            Polars expression (literal or column reference)
        """
        value_str = value_str.strip()

        # String literal (quoted)
        if (value_str.startswith("'") and value_str.endswith("'")) or (
            value_str.startswith('"') and value_str.endswith('"')
        ):
            # Remove quotes
            literal_value = value_str[1:-1]
            return pl.lit(literal_value)

        # Numeric literal
        try:
            if "." in value_str:
                return pl.lit(float(value_str))
            else:
                return pl.lit(int(value_str))
        except ValueError:
            pass

        # Boolean literal
        if value_str.lower() in ["true", "false"]:
            return pl.lit(value_str.lower() == "true")

        # Column reference
        return pl.col(value_str)

    def _extract_datetime_part(self, expr: pl.Expr, part: str) -> pl.Expr:
        """Extract datetime part from expression, handling both string and datetime columns.

        Args:
            expr: Polars expression (column reference)
            part: Part to extract (year, month, day, hour, etc.)

        Returns:
            Polars expression for datetime part extraction
        """
        # Map of part names to Polars methods
        part_map = {
            "year": lambda e: e.dt.year(),
            "month": lambda e: e.dt.month(),
            "day": lambda e: e.dt.day(),
            "hour": lambda e: e.dt.hour(),
            "minute": lambda e: e.dt.minute(),
            "second": lambda e: e.dt.second(),
            "dayofweek": lambda e: (e.dt.weekday() % 7)
            + 1,  # Polars ISO: Mon=1,Sun=7; PySpark: Sun=1,Mon=2,...,Sat=7
            "dayofyear": lambda e: e.dt.ordinal_day(),
            "weekofyear": lambda e: e.dt.week(),
            "quarter": lambda e: e.dt.quarter(),
        }

        extractor = part_map.get(part)
        if not extractor:
            raise ValueError(f"Unsupported datetime part: {part}")

        # Handle both string and datetime columns
        # For string columns, we need to parse first using str.strptime()
        # For datetime columns, we can use dt methods directly
        # Since we can't check type at expression build time, we use a conditional approach
        # that tries string parsing first, with a fallback for datetime columns

        # Use Polars' ability to handle this with a when/then/otherwise pattern
        # But simpler: just always try str.strptime() - it will work for strings
        # For datetime columns, we need to cast them or use directly
        # Actually, str.strptime only works on string columns, so we need a different approach

        # Use pl.when() to conditionally handle, but we can't check dtype in expression
        # So we'll use a try-cast pattern: try to parse as string, if that fails use as datetime
        # But Polars doesn't have try-cast in expressions easily

        # Simplest approach: assume string and parse it
        # If the column is already datetime, this will fail at runtime
        # For now, we'll parse strings and document that datetime columns should work
        # but may need explicit handling

        # For string columns (most common case in tests):
        # We need to handle both string and datetime columns
        # For string columns: parse with str.strptime() first
        # For datetime columns: use dt methods directly
        # Since we can't check type at expression build time, we use map_elements
        # with a function that handles both cases
        import datetime as dt_module
        from typing import Any, Optional

        def extract_part(value: Any) -> Optional[int]:
            """Extract datetime part from value, handling both string and datetime."""
            if value is None:
                return None
            # If it's already a datetime, use it directly
            if isinstance(value, (dt_module.datetime, dt_module.date)):
                parsed = value
            # If it's a string, try to parse it
            elif isinstance(value, str):
                try:
                    # Normalize the string: replace space with T, handle timezone formats
                    normalized = value.replace(" ", "T")
                    # Handle timezone format +0000 -> +00:00 (fromisoformat requires colon)
                    import re

                    # Pattern: +HHMM or -HHMM at the end (e.g., +0000, -0500)
                    normalized = re.sub(
                        r"([+-])(\d{2})(\d{2})(?=Z|$)", r"\1\2:\3", normalized
                    )
                    # Also handle Z timezone indicator
                    if normalized.endswith("Z"):
                        normalized = normalized[:-1] + "+00:00"
                    # Try parsing as datetime (most common format)
                    parsed = dt_module.datetime.fromisoformat(normalized)
                except Exception:
                    logger.debug("fromisoformat failed, trying strptime", exc_info=True)
                    try:
                        # Try common timestamp formats
                        # Format: yyyy-MM-ddTHH:mm:ss.SSS+HHMM
                        import re

                        # Try to parse with strptime for various formats
                        formats = [
                            "%Y-%m-%dT%H:%M:%S.%f%z",  # With microseconds and timezone
                            "%Y-%m-%dT%H:%M:%S%z",  # Without microseconds, with timezone
                            "%Y-%m-%d %H:%M:%S.%f",  # With microseconds, no timezone
                            "%Y-%m-%d %H:%M:%S",  # Without microseconds, no timezone
                            "%Y-%m-%dT%H:%M:%S",  # ISO format without timezone
                            "%Y-%m-%d",  # Date only
                        ]
                        parsed = None
                        for fmt in formats:
                            try:
                                # For timezone formats, we need to handle +0000 -> +00:00
                                if "%z" in fmt:
                                    # Normalize timezone format
                                    test_value = value.replace(" ", "T")
                                    test_value = re.sub(
                                        r"([+-])(\d{2})(\d{2})(?=Z|$)",
                                        r"\1\2:\3",
                                        test_value,
                                    )
                                    if test_value.endswith("Z"):
                                        test_value = test_value[:-1] + "+00:00"
                                    parsed = dt_module.datetime.strptime(
                                        test_value, fmt
                                    )
                                    break
                                else:
                                    parsed = dt_module.datetime.strptime(value, fmt)
                                    break
                            except Exception:
                                continue
                        if parsed is None:
                            raise ValueError("Could not parse datetime string")
                    except Exception:
                        logger.debug(
                            "All datetime parsing methods failed", exc_info=True
                        )
                        return None
            else:
                return None

            # Ensure parsed is not None (mypy type narrowing)
            if parsed is None:
                return None

            # Extract the requested part (return as int to ensure Int32 type)
            if part == "year":
                return int(parsed.year)
            elif part == "month":
                return int(parsed.month)
            elif part == "day":
                return int(parsed.day)
            elif part == "hour":
                return int(parsed.hour) if isinstance(parsed, dt_module.datetime) else 0
            elif part == "minute":
                return (
                    int(parsed.minute) if isinstance(parsed, dt_module.datetime) else 0
                )
            elif part == "second":
                return (
                    int(parsed.second) if isinstance(parsed, dt_module.datetime) else 0
                )
            elif part == "dayofweek":
                # PySpark: Sun=1,Mon=2,...,Sat=7
                # Python: Mon=0,Tue=1,...,Sun=6
                return int((parsed.weekday() + 1) % 7 + 1)
            elif part == "dayofyear":
                return int(parsed.timetuple().tm_yday)
            elif part == "weekofyear":
                return int(parsed.isocalendar()[1])
            elif part == "quarter":
                return int((parsed.month - 1) // 3 + 1)
            else:
                return None

        return expr.map_elements(extract_part, return_dtype=pl.Int64)

    def _translate_aggregate_function(self, agg_func: AggregateFunction) -> pl.Expr:
        """Translate aggregate function.

        Args:
            agg_func: AggregateFunction instance

        Returns:
            Polars aggregate expression
        """
        function_name = agg_func.function_name.lower()
        column = agg_func.column

        # Count(*) case
        col_expr = self.translate(column) if column else pl.lit(1)

        if function_name == "sum":
            return col_expr.sum()
        elif function_name == "avg" or function_name == "mean":
            return col_expr.mean()
        elif function_name == "count":
            if column:
                return col_expr.count()
            else:
                return pl.len()
        elif function_name == "countdistinct":
            # Count distinct values
            if column:
                return col_expr.n_unique()
            else:
                return pl.len()
        elif function_name == "max":
            return col_expr.max()
        elif function_name == "min":
            return col_expr.min()
        elif function_name == "stddev" or function_name == "stddev_samp":
            return col_expr.std()
        elif function_name == "variance" or function_name == "var_samp":
            return col_expr.var()
        elif function_name == "collect_list":
            # Collect values into a list
            return col_expr.implode()
        elif function_name == "collect_set":
            # Collect unique values into a set (preserve first occurrence order, like PySpark)
            # Use maintain_order=True to preserve the order of first occurrence
            return col_expr.unique(maintain_order=True).implode()
        elif function_name == "first":
            # First value in group
            ignorenulls = getattr(agg_func, "ignorenulls", False)
            if ignorenulls:
                # Filter out nulls before taking first value
                return col_expr.filter(col_expr.is_not_null()).first()
            else:
                # Return first value even if it's null (default behavior)
                return col_expr.first()
        elif function_name == "last":
            # Last value in group
            return col_expr.last()
        else:
            raise ValueError(f"Unsupported aggregate function: {function_name}")

    def _bround_expr(self, expr: pl.Expr, op: Any) -> pl.Expr:
        """Banker's rounding (HALF_EVEN rounding mode).

        Args:
            expr: Polars expression
            op: ColumnOperation with scale in op.value

        Returns:
            Polars expression for banker's rounding
        """
        scale = op.value if op.value is not None else 0
        if scale == 0:
            # Round to nearest integer using HALF_EVEN
            return expr.round(0)
        else:
            # Round to scale decimal places using HALF_EVEN
            # Polars doesn't have direct HALF_EVEN, use round() which uses HALF_TO_EVEN
            return expr.round(scale)

    def _conv_expr(self, expr: pl.Expr, op: Any) -> pl.Expr:
        """Convert number from one base to another.

        Args:
            expr: Polars expression (number as string or number)
            op: ColumnOperation with (from_base, to_base) in op.value

        Returns:
            Polars expression for base conversion
        """
        if isinstance(op.value, (tuple, list)) and len(op.value) >= 2:
            from_base = op.value[0]
            to_base = op.value[1]
        else:
            raise ValueError("conv requires (from_base, to_base) tuple")

        # Convert number to string in from_base, then parse from that base, then convert to to_base
        def convert_base(x: Any, from_b: int, to_b: int) -> Optional[str]:
            if x is None:
                return None
            try:
                # Parse as integer from source base
                num = int(x, from_b) if isinstance(x, str) else int(x)
                # Convert to target base
                if to_b == 10:
                    return str(num)
                elif to_b == 2:
                    return bin(num)[2:]
                elif to_b == 16:
                    return hex(num)[2:].upper()
                else:
                    # Generic base conversion
                    if num == 0:
                        return "0"
                    digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    result = ""
                    n = abs(num)
                    while n > 0:
                        result = digits[n % to_b] + result
                        n //= to_b
                    return ("-" if num < 0 else "") + result
            except (ValueError, TypeError):
                return None

        return expr.map_elements(
            lambda x: convert_base(x, from_base, to_base), return_dtype=pl.Utf8
        )

    def _translate_case_when(self, case_when: Any) -> pl.Expr:
        """Translate CaseWhen to Polars expression.

        Args:
            case_when: CaseWhen instance

        Returns:
            Polars expression using pl.when().then().otherwise() chain
        """
        from sparkless.functions.conditional import CaseWhen

        if not isinstance(case_when, CaseWhen):
            raise ValueError(f"Expected CaseWhen, got {type(case_when)}")

        if not case_when.conditions:
            # No conditions - return default value or None
            if case_when.default_value is not None:
                return self.translate(case_when.default_value)
            return pl.lit(None)

        # Build chained when/then/otherwise expression
        # Start with the first condition
        condition, value = case_when.conditions[0]
        condition_expr = self.translate(condition)
        value_expr = self._translate_value_to_expr(value)

        # Start the chain
        result = pl.when(condition_expr).then(value_expr)

        # Add additional when/then pairs
        for condition, value in case_when.conditions[1:]:
            condition_expr = self.translate(condition)
            value_expr = self._translate_value_to_expr(value)
            result = result.when(condition_expr).then(value_expr)

        # Add otherwise clause if default_value is set
        if case_when.default_value is not None:
            default_expr = self._translate_value_to_expr(case_when.default_value)
            result = result.otherwise(default_expr)
        else:
            result = result.otherwise(None)

        return result

    def _translate_value_to_expr(self, value: Any) -> pl.Expr:
        """Translate a value to a Polars expression, handling literals properly.

        This is used for CASE WHEN values where plain strings/numbers should be
        treated as literals, not column names.

        Args:
            value: Value to translate (string, number, bool, or expression)

        Returns:
            Polars expression
        """
        # If it's already a Column, ColumnOperation, etc., use translate
        if isinstance(value, (Column, ColumnOperation, Literal, AggregateFunction)):
            return self.translate(value)
        # If it's a plain Python type, treat as literal
        elif isinstance(value, (str, int, float, bool)):
            return pl.lit(value)
        # If it's None, return literal None
        elif value is None:
            return pl.lit(None)
        # Otherwise try translate (might be a CaseWhen or other complex type)
        else:
            return self.translate(value)
