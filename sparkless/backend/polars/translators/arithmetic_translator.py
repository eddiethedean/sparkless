"""
Arithmetic operation translator for Polars backend.

This module handles translation of arithmetic operations from Sparkless Column expressions
to Polars expressions.
"""

from typing import Any
import polars as pl


class ArithmeticTranslator:
    """Translates arithmetic operations to Polars expressions."""

    def __init__(self, base_translator: Any):
        """Initialize ArithmeticTranslator.

        Args:
            base_translator: Reference to the main PolarsExpressionTranslator for
                           delegating translation of nested expressions.
        """
        self._base_translator = base_translator

    def translate_arithmetic(
        self, left: pl.Expr, right: pl.Expr, operation: str
    ) -> pl.Expr:
        """Translate arithmetic operations.

        Args:
            left: Left Polars expression
            right: Right Polars expression
            operation: Operation string (+, -, *, /, %, **)

        Returns:
            Polars expression for arithmetic operation
        """
        if operation == "+":
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
            return self._base_translator._coerce_for_arithmetic(left, right, operation)
        else:
            raise ValueError(f"Unsupported arithmetic operation: {operation}")

    def translate_unary_arithmetic(self, expr: pl.Expr, operation: str) -> pl.Expr:
        """Translate unary arithmetic operations.

        Args:
            expr: Polars expression
            operation: Operation string (-, !, ~)

        Returns:
            Polars expression for unary operation
        """
        if operation == "-":
            return -expr
        elif operation in ["!", "~"]:
            return ~expr
        else:
            raise ValueError(f"Unsupported unary arithmetic operation: {operation}")
