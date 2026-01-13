"""
Lambda Expression Parser for Sparkless.

This module provides AST parsing and translation of Python lambda expressions.
The parser is used by Polars backend for lambda evaluation. The `to_duckdb_lambda()`
method is DuckDB-specific and used only for DuckDB/SQL backends (legacy).

Key Features:
    - Parse Python lambda expressions using AST module
    - Translate to DuckDB lambda syntax: `x -> expression` (DuckDB backend only)
    - Support 1-arg and 2-arg lambdas
    - Handle arithmetic, comparison, and logical operators
    - Type-safe with comprehensive error handling

Note: `to_duckdb_lambda()` is for DuckDB backend only. Polars backend uses `parse()`.

Example:
    >>> from sparkless.functions.core.lambda_parser import LambdaParser
    >>> parser = LambdaParser(lambda x: x * 2)
    >>> parser.to_duckdb_lambda()  # DuckDB backend only
    'x -> (x * 2)'
"""

import ast
import inspect
import textwrap
from typing import Any, Callable, List


class LambdaTranslationError(Exception):
    """Raised when a lambda expression cannot be translated to DuckDB syntax."""

    pass


class LambdaParser:
    """Parser for Python lambda expressions to DuckDB lambda syntax.

    Parses Python lambda functions using the AST module and translates them
    to DuckDB-compatible lambda syntax for use in LIST_TRANSFORM, LIST_FILTER,
    and other higher-order array/map functions.

    Attributes:
        lambda_func: The Python lambda function to parse.
        ast_node: The parsed AST Lambda node.
        param_names: List of parameter names from the lambda.
    """

    def __init__(self, lambda_func: Callable[..., Any]):
        """Initialize LambdaParser.

        Args:
            lambda_func: A Python lambda function to parse.

        Raises:
            LambdaTranslationError: If the function is not a lambda or cannot be parsed.
        """
        self.lambda_func = lambda_func

        # Get the source code of the lambda
        try:
            source = inspect.getsource(lambda_func)
            source = textwrap.dedent(source)
        except (OSError, TypeError) as e:
            raise LambdaTranslationError(f"Cannot get source for lambda: {e}")

        # Parse the lambda expression or a simple def with a return expression
        try:
            # Prefer fast path: direct lambda in source
            lambda_start = -1
            for pattern in ["lambda ", "lambda:"]:
                idx = source.find(pattern)
                if idx != -1:
                    lambda_start = idx
                    break

            if lambda_start != -1:
                # Extract from 'lambda' onward
                lambda_expr = source[lambda_start:]

                # Trim at the end of the lambda expression
                paren_depth = 0
                end_idx = len(lambda_expr)
                seen_colon = False
                for i, char in enumerate(lambda_expr):
                    if char == ":":
                        seen_colon = True
                    elif char == "(":
                        paren_depth += 1
                    elif char == ")":
                        if paren_depth == 0:
                            end_idx = i
                            break
                        paren_depth -= 1
                    elif char in [",", "\n"] and paren_depth == 0 and seen_colon:
                        end_idx = i
                        break

                lambda_expr = lambda_expr[:end_idx].strip()
                tree = ast.parse(lambda_expr, mode="eval")
                if not isinstance(tree.body, ast.Lambda):
                    raise LambdaTranslationError("Parsed expression is not a lambda")
                self.ast_node = tree.body
            else:
                # Fallback: allow regular function definitions with a single return expression
                mod = ast.parse(source)
                func_node = None
                for n in mod.body:
                    if isinstance(n, ast.FunctionDef):
                        func_node = n
                        break
                if func_node is None:
                    raise LambdaTranslationError("Not a lambda function")

                # Validate body is a simple single return statement
                if (
                    len(func_node.body) != 1
                    or not isinstance(func_node.body[0], ast.Return)
                    or func_node.body[0].value is None
                ):
                    raise LambdaTranslationError(
                        "Only simple functions with a single return expression are supported"
                    )

                # Construct an equivalent Lambda node from the function signature and return value
                self.ast_node = ast.Lambda(
                    args=func_node.args, body=func_node.body[0].value
                )

        except SyntaxError as e:
            raise LambdaTranslationError(f"Cannot parse lambda: {e}")

        # Extract parameter names
        self.param_names = self.get_param_names()

    def get_param_names(self) -> List[str]:
        """Extract parameter names from the lambda.

        Returns:
            List of parameter names.
        """
        args = self.ast_node.args
        param_names = []

        for arg in args.args:
            param_names.append(arg.arg)

        return param_names

    def to_duckdb_lambda(self) -> str:
        """Translate the Python lambda to DuckDB lambda syntax.

        NOTE: This method is DuckDB-backend specific (legacy).
        Polars backend uses parse() method instead.

        Returns:
            DuckDB lambda expression as a string.

        Example:
            Python: lambda x: x * 2
            DuckDB: x -> (x * 2)

            Python: lambda x, y: x + y
            DuckDB: (x, y) -> (x + y)
        """
        # Format parameters
        if len(self.param_names) == 1:
            params = self.param_names[0]
        else:
            params = f"({', '.join(self.param_names)})"

        # Translate body
        body_expr = self._translate_expression(self.ast_node.body)

        return f"{params} -> {body_expr}"

    def _translate_expression(self, node: ast.expr) -> str:
        """Recursively translate an AST expression node to DuckDB SQL.

        Args:
            node: AST expression node.

        Returns:
            DuckDB SQL expression string.

        Raises:
            LambdaTranslationError: If the expression type is not supported.
        """
        if isinstance(node, ast.Name):
            # Variable reference (parameter name)
            return node.id

        elif isinstance(node, ast.Constant):
            # Literal value (numbers, strings, etc.)
            if isinstance(node.value, str):
                return f"'{node.value}'"
            return str(node.value)

        elif isinstance(node, ast.Num):  # Python 3.7 compatibility
            return str(node.n)

        elif isinstance(node, ast.BinOp):
            # Binary operation (x + y, x * y, etc.)
            left = self._translate_expression(node.left)
            right = self._translate_expression(node.right)
            op = self._translate_operator(node.op)
            return f"({left} {op} {right})"

        elif isinstance(node, ast.Compare):
            # Comparison (x > 10, x == 5, etc.)
            left = self._translate_expression(node.left)

            if len(node.ops) == 1 and len(node.comparators) == 1:
                op = self._translate_comparison(node.ops[0])
                right = self._translate_expression(node.comparators[0])
                return f"({left} {op} {right})"
            else:
                # Multiple comparisons (x > 0 and x < 100)
                parts = [left]
                for cmp_op, comp in zip(node.ops, node.comparators):
                    op_str = self._translate_comparison(cmp_op)
                    right = self._translate_expression(comp)
                    parts.append(f"{op_str} {right}")
                return f"({' '.join(parts)})"

        elif isinstance(node, ast.BoolOp):
            # Boolean operation (and, or)
            op = self._translate_bool_op(node.op)
            values = [self._translate_expression(v) for v in node.values]
            return f"({f' {op} '.join(values)})"

        elif isinstance(node, ast.UnaryOp):
            # Unary operation (-x, not x)
            operand = self._translate_expression(node.operand)
            op = self._translate_unary_op(node.op)
            return f"({op}{operand})"

        elif isinstance(node, ast.Call):
            # Function call - may be a Spark function
            # For now, raise an error - we'll add support later
            raise LambdaTranslationError(
                "Function calls in lambdas not yet supported. "
                "Lambda body must be a simple expression."
            )

        else:
            raise LambdaTranslationError(
                f"Unsupported expression type: {type(node).__name__}"
            )

    def _translate_operator(self, op: ast.operator) -> str:
        """Translate Python operator to DuckDB operator.

        Args:
            op: AST operator node.

        Returns:
            DuckDB operator string.
        """
        operator_map = {
            ast.Add: "+",
            ast.Sub: "-",
            ast.Mult: "*",
            ast.Div: "/",
            ast.FloorDiv: "//",
            ast.Mod: "%",
            ast.Pow: "**",
        }

        op_type = type(op)
        if op_type in operator_map:
            return operator_map[op_type]

        raise LambdaTranslationError(f"Unsupported operator: {op_type.__name__}")

    def _translate_comparison(self, op: ast.cmpop) -> str:
        """Translate Python comparison operator to DuckDB.

        Args:
            op: AST comparison operator node.

        Returns:
            DuckDB comparison operator string.
        """
        comparison_map = {
            ast.Eq: "=",  # DuckDB uses = for equality
            ast.NotEq: "!=",
            ast.Lt: "<",
            ast.LtE: "<=",
            ast.Gt: ">",
            ast.GtE: ">=",
        }

        op_type = type(op)
        if op_type in comparison_map:
            return comparison_map[op_type]

        raise LambdaTranslationError(f"Unsupported comparison: {op_type.__name__}")

    def _translate_bool_op(self, op: ast.boolop) -> str:
        """Translate Python boolean operator to DuckDB.

        Args:
            op: AST boolean operator node.

        Returns:
            DuckDB boolean operator string.
        """
        if isinstance(op, ast.And):
            return "AND"
        elif isinstance(op, ast.Or):
            return "OR"

        raise LambdaTranslationError(
            f"Unsupported boolean operator: {type(op).__name__}"
        )

    def _translate_unary_op(self, op: ast.unaryop) -> str:
        """Translate Python unary operator to DuckDB.

        Args:
            op: AST unary operator node.

        Returns:
            DuckDB unary operator string.
        """
        if isinstance(op, ast.USub):
            return "-"
        elif isinstance(op, ast.Not):
            return "NOT "

        raise LambdaTranslationError(f"Unsupported unary operator: {type(op).__name__}")


class MockLambdaExpression:
    """Wrapper for Python lambda expressions used in Spark functions.

    This class wraps a Python lambda function and provides methods to
    translate it to DuckDB lambda syntax for use in higher-order functions
    like transform, filter, exists, etc.

    Attributes:
        lambda_func: The Python lambda function.
        parser: LambdaParser instance for this lambda.
        param_count: Number of parameters in the lambda.

    Example:
        >>> expr = MockLambdaExpression(lambda x: x * 2)
        >>> expr.to_duckdb_lambda()
        'x -> (x * 2)'
    """

    def __init__(self, lambda_func: Callable[..., Any]):
        """Initialize MockLambdaExpression.

        Args:
            lambda_func: A Python lambda function.
        """
        self.lambda_func = lambda_func
        self.parser = LambdaParser(lambda_func)
        self.param_count = len(self.parser.param_names)

    def to_duckdb_lambda(self) -> str:
        """Convert to DuckDB lambda syntax.

        Returns:
            DuckDB lambda expression string.
        """
        return self.parser.to_duckdb_lambda()

    def get_param_names(self) -> List[str]:
        """Get parameter names.

        Returns:
            List of parameter names.
        """
        return self.parser.param_names

    def __repr__(self) -> str:
        """String representation."""
        try:
            lambda_str = self.to_duckdb_lambda()
            return f"MockLambdaExpression({lambda_str})"
        except Exception:
            return f"MockLambdaExpression(params={self.param_count})"
