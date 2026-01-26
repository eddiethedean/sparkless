"""
Type casting translator for Polars backend.

This module handles translation of type casting operations from Sparkless Column expressions
to Polars expressions.
"""

from typing import Any
import contextlib
import logging
import polars as pl
from sparkless.spark_types import (
    IntegerType,
    LongType,
    DateType,
    TimestampType,
)

logger = logging.getLogger(__name__)


class TypeTranslator:
    """Translates type casting operations to Polars expressions."""

    def translate_cast(self, expr: pl.Expr, target_type: Any) -> pl.Expr:
        """Translate cast operation.

        Args:
            expr: Polars expression to cast
            target_type: Target data type (DataType or string type name)

        Returns:
            Casted Polars expression
        """
        from ..type_mapper import mock_type_to_polars_dtype

        # Try to detect target type
        polars_dtype = None
        try:
            # Try to get Polars dtype from target_type
            if hasattr(target_type, "simpleString"):
                type_name = target_type.simpleString().lower()
            elif isinstance(target_type, str):
                type_name = target_type.lower()
            else:
                type_name = str(target_type).lower()

            # Map Spark type names to Polars dtypes
            if "string" in type_name or "varchar" in type_name:
                polars_dtype = pl.Utf8
            elif (
                ("int" in type_name and "long" not in type_name)
                or "long" in type_name
                or "bigint" in type_name
            ):
                polars_dtype = pl.Int64
            elif "double" in type_name:
                polars_dtype = pl.Float64
            elif "float" in type_name:
                polars_dtype = pl.Float32
            elif "boolean" in type_name or "bool" in type_name:
                polars_dtype = pl.Boolean
            elif "date" in type_name:
                polars_dtype = pl.Date
            elif "timestamp" in type_name:
                polars_dtype = pl.Datetime(time_unit="us")
            elif "short" in type_name:
                polars_dtype = pl.Int16
            elif "byte" in type_name:
                polars_dtype = pl.Int8
            else:
                # Try using type mapper
                with contextlib.suppress(Exception):
                    polars_dtype = mock_type_to_polars_dtype(target_type)
        except (ValueError, TypeError, AttributeError):
            # Specific exceptions for type detection failures
            logger.debug("Exception in cast type detection, continuing", exc_info=True)
            pass
        except Exception as e:
            # Log unexpected exceptions but continue
            logger.warning(
                f"Unexpected exception in cast type detection: {type(e).__name__}: {e}",
                exc_info=True,
            )
            pass

        # For string to int/long casting, Polars needs float intermediate step
        if isinstance(target_type, (IntegerType, LongType)):
            return expr.cast(pl.Float64, strict=False).cast(polars_dtype, strict=False)

        # For string to date/timestamp casting
        if isinstance(target_type, (DateType, TimestampType)):
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

                return expr.map_elements(parse_date, return_dtype=pl.Date)
            else:
                # Parse timestamp string
                def parse_timestamp(val: Any) -> Any:
                    if val is None:
                        return None
                    from datetime import datetime

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

                return expr.map_elements(
                    parse_timestamp, return_dtype=pl.Datetime(time_unit="us")
                )

        # Default: use type mapper or direct cast
        if polars_dtype is None:
            try:
                polars_dtype = mock_type_to_polars_dtype(target_type)
            except Exception:
                # Fallback: try to infer from string
                if isinstance(target_type, str):
                    type_name = target_type.lower()
                    if "string" in type_name:
                        polars_dtype = pl.Utf8
                    elif "int" in type_name:
                        polars_dtype = pl.Int64
                    elif "double" in type_name or "float" in type_name:
                        polars_dtype = pl.Float64
                    elif "boolean" in type_name:
                        polars_dtype = pl.Boolean
                    elif "date" in type_name:
                        polars_dtype = pl.Date
                    elif "timestamp" in type_name:
                        polars_dtype = pl.Datetime(time_unit="us")
                    else:
                        polars_dtype = pl.Utf8  # Default fallback

        if polars_dtype is not None:
            try:
                # Check if expr is a None literal that needs typed None
                # This handles cases where pl.lit(None) can't be cast directly
                try:
                    return expr.cast(polars_dtype, strict=False)
                except (pl.exceptions.ComputeError, TypeError, ValueError) as e:
                    # If casting fails due to null type, create typed None literal
                    error_msg = str(e).lower()
                    if "null" in error_msg or "dtype" in error_msg:
                        return pl.lit(None, dtype=polars_dtype)
                    raise
            except (pl.exceptions.ComputeError, TypeError, ValueError) as e:
                # Check if this is an InvalidOperationError for unsupported casts
                error_msg = str(e)
                if (
                    "not supported" in error_msg.lower()
                    or "InvalidOperationError" in str(type(e).__name__)
                ):
                    # Raise ValueError to trigger Python fallback
                    raise ValueError(
                        f"Cast operation requires Python evaluation: {error_msg}"
                    ) from e
                # Fallback: try to create typed None if cast fails
                logger.debug(
                    "Initial cast failed, trying typed None fallback", exc_info=True
                )
                try:
                    return pl.lit(None, dtype=polars_dtype)
                except (TypeError, ValueError) as fallback_error:
                    logger.debug(
                        f"Typed None fallback failed: {fallback_error}", exc_info=True
                    )
                    # Last resort: try regular cast
                    return expr.cast(polars_dtype, strict=False)
        else:
            # No dtype found - try direct cast
            return expr.cast(pl.Utf8, strict=False)
