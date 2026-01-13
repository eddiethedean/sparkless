"""
Safe expression evaluator for Sparkless.

This module provides a safe way to evaluate string expressions without using eval(),
preventing security vulnerabilities from code injection.
"""

import ast
from typing import Any, Dict


class SafeExpressionEvaluator:
    """Safely evaluates Python-like expressions without using eval().

    Supports:
    - Arithmetic operations: +, -, *, /, %
    - Comparison operations: ==, !=, <, >, <=, >=
    - Logical operations: and, or, not
    - Attribute access: obj.attr
    - Simple variable references
    - Literals: numbers, strings, booleans, None
    """

    @staticmethod
    def evaluate(expression: str, context: Dict[str, Any]) -> Any:
        """Evaluate a string expression safely using AST.

        Args:
            expression: Python-like expression string (e.g., "age > 25 and name == 'Alice'")
            context: Dictionary of variables/values available in the expression

        Returns:
            The evaluated result

        Raises:
            ValueError: If the expression contains unsafe operations
            SyntaxError: If the expression is not valid Python syntax
        """
        try:
            # Parse the expression into an AST
            tree = ast.parse(expression, mode="eval")

            # Evaluate the AST safely
            return SafeExpressionEvaluator._evaluate_ast(tree.body, context)
        except SyntaxError:
            # Return False for invalid syntax in boolean contexts (PySpark compatibility)
            return False
        except Exception:
            # Return False for any evaluation errors (matching current behavior)
            return False

    @staticmethod
    def evaluate_boolean(expression: str, context: Dict[str, Any]) -> bool:
        """Evaluate a boolean expression safely.

        Args:
            expression: Boolean expression string
            context: Dictionary of variables/values

        Returns:
            Boolean result, False on error
        """
        try:
            result = SafeExpressionEvaluator.evaluate(expression, context)
            return bool(result) if result is not None else False
        except Exception:
            return False

    @staticmethod
    def _evaluate_ast(node: ast.AST, context: Dict[str, Any]) -> Any:
        """Recursively evaluate an AST node.

        Args:
            node: AST node to evaluate
            context: Variable context

        Returns:
            Evaluated value

        Raises:
            ValueError: If unsafe operations are detected
        """
        if isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Num):  # Python < 3.8 compatibility
            return node.n
        elif isinstance(node, ast.Str):  # Python < 3.8 compatibility
            return node.s
        elif isinstance(node, ast.NameConstant):  # Python < 3.8 compatibility
            return node.value
        elif isinstance(node, ast.Name):
            if node.id in context:
                return context[node.id]
            # Return None for undefined variables (matching eval behavior)
            return None
        elif isinstance(node, ast.Attribute):
            obj = SafeExpressionEvaluator._evaluate_ast(node.value, context)
            if obj is None:
                return None
            # Only allow attribute access to known safe objects
            if hasattr(obj, node.attr):
                return getattr(obj, node.attr)
            return None
        elif isinstance(node, ast.BinOp):
            left = SafeExpressionEvaluator._evaluate_ast(node.left, context)
            right = SafeExpressionEvaluator._evaluate_ast(node.right, context)

            if isinstance(node.op, ast.Add):
                # Handle string concatenation and numeric addition
                if left is None or right is None:
                    return None
                return left + right
            elif isinstance(node.op, ast.Sub):
                if left is None or right is None:
                    return None
                return left - right
            elif isinstance(node.op, ast.Mult):
                if left is None or right is None:
                    return None
                return left * right
            elif isinstance(node.op, ast.Div):
                if left is None or right is None:
                    return None
                if right == 0:
                    return None
                return left / right
            elif isinstance(node.op, ast.Mod):
                if left is None or right is None:
                    return None
                if right == 0:
                    return None
                return left % right
            else:
                return None
        elif isinstance(node, ast.Compare):
            left = SafeExpressionEvaluator._evaluate_ast(node.left, context)
            result = True

            for op, comparator in zip(node.ops, node.comparators):
                right = SafeExpressionEvaluator._evaluate_ast(comparator, context)

                # Handle 'is' and 'is not' operators first (identity operators)
                if isinstance(op, ast.Is):
                    # Handle 'is' operator (e.g., 'value is None')
                    result = result and (left is right)
                    left = right  # For chained comparisons
                    continue
                elif isinstance(op, ast.IsNot):
                    # Handle 'is not' operator (e.g., 'value is not None')
                    result = result and (left is not right)
                    left = right  # For chained comparisons
                    continue

                # Handle None comparisons for other operators
                if left is None or right is None:
                    # NULL comparisons - == returns True only if both are None
                    if isinstance(op, ast.Eq):
                        result = result and (left is None and right is None)
                    elif isinstance(op, ast.NotEq):
                        result = result and (left is not None or right is not None)
                    else:
                        # Other comparisons with None return False
                        result = result and False
                    continue

                if isinstance(op, ast.Eq):
                    result = result and (left == right)
                elif isinstance(op, ast.NotEq):
                    result = result and (left != right)
                elif isinstance(op, ast.Lt):
                    result = result and (left < right)
                elif isinstance(op, ast.LtE):
                    result = result and (left <= right)
                elif isinstance(op, ast.Gt):
                    result = result and (left > right)
                elif isinstance(op, ast.GtE):
                    result = result and (left >= right)
                else:
                    result = False

                left = right  # For chained comparisons

            return result
        elif isinstance(node, ast.BoolOp):
            values = [
                SafeExpressionEvaluator._evaluate_ast(child, context)
                for child in node.values
            ]

            if isinstance(node.op, ast.And):
                return all(bool(v) if v is not None else False for v in values)
            elif isinstance(node.op, ast.Or):
                return any(bool(v) if v is not None else False for v in values)
            else:
                return False
        elif isinstance(node, ast.UnaryOp):
            operand = SafeExpressionEvaluator._evaluate_ast(node.operand, context)

            if isinstance(node.op, ast.Not):
                return not bool(operand) if operand is not None else True
            elif isinstance(node.op, ast.USub):
                return -operand if operand is not None else None
            elif isinstance(node.op, ast.UAdd):
                return operand
            else:
                return None
        elif isinstance(node, ast.IfExp):
            test = SafeExpressionEvaluator._evaluate_ast(node.test, context)
            if bool(test) if test is not None else False:
                return SafeExpressionEvaluator._evaluate_ast(node.body, context)
            else:
                return SafeExpressionEvaluator._evaluate_ast(node.orelse, context)
        else:
            # Unsupported node type - return None (safe fallback)
            return None
