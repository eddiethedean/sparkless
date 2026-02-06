"""
SQL Expression Parser for F.expr() compatibility.

This module provides SQL expression parsing to match PySpark's F.expr() behavior,
which accepts SQL syntax (e.g., "id IS NOT NULL") rather than Python expressions
(e.g., "col('id').isNotNull()").
"""

import re
from typing import Any, List, TYPE_CHECKING, Union

from .column import Column, ColumnOperation
from ...core.exceptions.analysis import ParseException

if TYPE_CHECKING:
    from ...functions.conditional import CaseWhen
    from .literals import Literal
else:
    CaseWhen = Any
    Literal = Any


class SQLExprParser:
    """Parser for SQL expressions in F.expr().

    Parses SQL expressions like PySpark:
    - Column references: "id", "user_name"
    - Literals: "123", "'string'", "true", "false", "NULL"
    - Operators: "IS NULL", "IS NOT NULL", "=", ">", "<", ">=", "<=", "!=", "<>"
    - Logical operators: "AND", "OR", "NOT"
    - Functions: "LENGTH(name)", "TRIM(name)", "UPPER(status)"
    - Parentheses for grouping
    """

    # SQL keywords (case-insensitive)
    KEYWORDS = {
        "IS",
        "NULL",
        "NOT",
        "AND",
        "OR",
        "TRUE",
        "FALSE",
        "LENGTH",
        "TRIM",
        "UPPER",
        "LOWER",
        "SUBSTRING",
        "REGEXP",
        "RLIKE",
    }

    @staticmethod
    def parse(expr: str) -> Union[Column, ColumnOperation, CaseWhen, Literal]:
        """Parse SQL expression string into Column/ColumnOperation.

        Args:
            expr: SQL expression string (e.g., "id IS NOT NULL")

        Returns:
            Column or ColumnOperation representing the parsed expression

        Raises:
            ParseException: If SQL syntax is invalid
        """
        expr = expr.strip()
        if not expr:
            raise ParseException("Empty SQL expression")

        # Try to parse as SQL expression
        try:
            return SQLExprParser._parse_expression(expr)
        except Exception as e:
            if isinstance(e, ParseException):
                raise
            raise ParseException(f"Invalid SQL expression: {expr}. Error: {str(e)}")

    @staticmethod
    def _parse_expression(
        expr: str,
    ) -> Union[Column, ColumnOperation, CaseWhen, Literal]:
        """Parse a SQL expression recursively."""
        expr = expr.strip()

        # Handle parentheses - find matching pairs
        if expr.startswith("(") and expr.endswith(")"):
            # Check if entire expression is wrapped in parentheses
            depth = 0
            for i, char in enumerate(expr):
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                    if depth == 0 and i < len(expr) - 1:
                        # Not fully wrapped, parse normally
                        break
            else:
                # Fully wrapped, unwrap and parse
                return SQLExprParser._parse_expression(expr[1:-1])

        # Parse CASE WHEN expressions FIRST (before other operators)
        # CASE WHEN expressions: CASE WHEN condition THEN value [WHEN ...] [ELSE value] END
        case_match = re.match(r"^CASE\s+(.+?)\s+END$", expr, re.IGNORECASE | re.DOTALL)
        if case_match:
            case_body = case_match.group(1).strip()
            # Parse CASE WHEN ... THEN ... [ELSE ...] structure
            # Use ConditionalFunctions.when() chain for CASE WHEN expressions

            # Split by WHEN (but be careful with nested CASE)
            # Simple approach: find WHEN ... THEN ... pairs and optional ELSE
            when_then_parts = []
            else_part = None

            # Find all WHEN ... THEN ... pairs
            when_pattern = r"WHEN\s+(.+?)\s+THEN\s+(.+?)(?=\s+WHEN|\s+ELSE|\s+END)"
            when_matches = re.finditer(
                when_pattern, case_body, re.IGNORECASE | re.DOTALL
            )
            for match in when_matches:
                condition = match.group(1).strip()
                value = match.group(2).strip()
                when_then_parts.append((condition, value))

            # Find ELSE part (match everything after ELSE until END or end of string)
            else_match = re.search(
                r"ELSE\s+(.+?)(?=\s+END|$)", case_body, re.IGNORECASE | re.DOTALL
            )
            if else_match:
                else_part = else_match.group(1).strip()

            # Build F.when() chain using ConditionalFunctions
            from ...functions.conditional import ConditionalFunctions

            case_result: Union[CaseWhen, None] = None
            for condition, value in when_then_parts:
                cond_expr = SQLExprParser._parse_expression(condition)
                val_expr = SQLExprParser._parse_expression(value)
                if case_result is None:
                    case_result = ConditionalFunctions.when(cond_expr, val_expr)
                else:
                    case_result = case_result.when(cond_expr, val_expr)

            if else_part:
                else_expr = SQLExprParser._parse_expression(else_part)
                if case_result is None:
                    # CASE with only ELSE (no WHEN) - not standard but handle it
                    return else_expr
                return case_result.otherwise(else_expr)
            elif case_result is None:
                # CASE with no WHEN or ELSE - return None
                from ...functions.core.literals import Literal

                return Literal(None)

            return case_result

        # Parse logical operators: AND, OR (before IS NULL so "a and b is not null"
        # parses as (a) and (b is not null), not (a and b) is not null - Issue #395)
        parts_or = SQLExprParser._split_logical_operator(expr, "OR")
        if len(parts_or) > 1:
            parsed_parts = [
                SQLExprParser._parse_expression(p.strip()) for p in parts_or
            ]
            or_result: Union[Column, ColumnOperation, CaseWhen, Literal] = parsed_parts[
                0
            ]
            for part in parsed_parts[1:]:
                or_result = ColumnOperation(or_result, "|", part)
            return or_result

        parts_and = SQLExprParser._split_logical_operator(expr, "AND")
        if len(parts_and) > 1:
            parsed_parts = [
                SQLExprParser._parse_expression(p.strip()) for p in parts_and
            ]
            and_result: Union[Column, ColumnOperation, CaseWhen, Literal] = (
                parsed_parts[0]
            )
            for part in parsed_parts[1:]:
                and_result = ColumnOperation(and_result, "&", part)
            return and_result

        # Parse IS NULL / IS NOT NULL
        is_null_match = re.match(r"^(.+?)\s+IS\s+(?:NOT\s+)?NULL$", expr, re.IGNORECASE)
        if is_null_match:
            col_expr = SQLExprParser._parse_expression(is_null_match.group(1).strip())
            is_not = "NOT" in expr.upper()
            # col_expr should be Column or ColumnOperation which have isnull/isnotnull methods
            # For CaseWhen or Literal, we need to handle differently
            if isinstance(col_expr, (Column, ColumnOperation)):
                if is_not:
                    return col_expr.isnotnull()
                else:
                    return col_expr.isnull()
            # If it's something else (CaseWhen, Literal), create a column operation
            # This shouldn't normally happen, but handle it gracefully
            # Create a dummy column and apply isnull/isnotnull
            dummy_col = Column("__expr__")
            if is_not:
                return dummy_col.isnotnull()
            else:
                return dummy_col.isnull()

        # Parse NOT first (before comparisons, as it wraps expressions)
        not_match = re.match(r"^NOT\s+(.+)$", expr, re.IGNORECASE)
        if not_match:
            inner_expr = not_match.group(1).strip()
            # Remove outer parentheses if present
            if inner_expr.startswith("(") and inner_expr.endswith(")"):
                # Check if fully wrapped
                depth = 0
                fully_wrapped = True
                for i, char in enumerate(inner_expr):
                    if char == "(":
                        depth += 1
                    elif char == ")":
                        depth -= 1
                        if depth == 0 and i < len(inner_expr) - 1:
                            fully_wrapped = False
                            break
                if fully_wrapped:
                    inner_expr = inner_expr[1:-1]
            inner = SQLExprParser._parse_expression(inner_expr)
            return ColumnOperation(inner, "!", None)

        # Parse arithmetic operators: *, /, %, +, - (before comparison operators)
        # These have higher precedence than comparison operators
        # Handle + and - carefully to avoid matching in numeric literals
        arithmetic_ops = [
            ("*", "*"),
            ("/", "/"),
            ("%", "%"),
            ("+", "+"),
            (
                "-",
                "-",
            ),  # Minus needs special handling to avoid matching in negative numbers
        ]

        for op_symbol, op in arithmetic_ops:
            # Split by operator, but handle string literals and negative numbers
            parts = SQLExprParser._split_by_operator(expr, op_symbol)
            if len(parts) == 2:
                # Check if this is a unary minus (e.g., "-5") rather than subtraction
                # This is a simple check - if the first part is empty, it's unary minus
                if op_symbol == "-" and not parts[0].strip():
                    # This is a unary minus, not subtraction - skip and let it be parsed as negative number
                    continue
                left = SQLExprParser._parse_expression(parts[0].strip())
                right = SQLExprParser._parse_expression(parts[1].strip())
                return ColumnOperation(left, op, right)

        # Parse comparison operators: =, >, <, >=, <=, !=, <> (after arithmetic operators)
        # Use "==" not "=" for equality so "a == 'Y'" splits once (Issue #395)
        comparison_ops = [
            (r">=", ">="),
            (r"<=", "<="),
            (r"!=", "!="),
            (r"<>", "!="),
            (r"==", "=="),
            (r"=", "=="),
            (r">", ">"),
            (r"<", "<"),
        ]

        for pattern, op in comparison_ops:
            # Split by operator, but handle string literals
            parts = SQLExprParser._split_by_operator(expr, pattern)
            if len(parts) == 2:
                left = SQLExprParser._parse_expression(parts[0].strip())
                right = SQLExprParser._parse_expression(parts[1].strip())
                return ColumnOperation(left, op, right)

        # Parse NOT LIKE (before LIKE): "col NOT LIKE 'pattern'" (Issue #394)
        not_like_match = re.search(r"\s+NOT\s+LIKE\s+", expr, re.IGNORECASE)
        if not_like_match:
            left_str = expr[: not_like_match.start()].strip()
            right_str = expr[not_like_match.end() :].strip()
            left_expr = SQLExprParser._parse_expression(left_str)
            pattern_val = SQLExprParser._parse_simple_value(right_str)
            from ...functions.string import StringFunctions

            like_result = StringFunctions.like(
                left_expr,  # type: ignore[arg-type]
                str(pattern_val),
            )
            return ColumnOperation(like_result, "!", None)

        # Parse LIKE: "col LIKE 'pattern'" (Issue #394)
        like_match = re.search(r"\s+LIKE\s+", expr, re.IGNORECASE)
        if like_match:
            left_str = expr[: like_match.start()].strip()
            right_str = expr[like_match.end() :].strip()
            left_expr = SQLExprParser._parse_expression(left_str)
            pattern_val = SQLExprParser._parse_simple_value(right_str)
            from ...functions.string import StringFunctions

            return StringFunctions.like(
                left_expr,  # type: ignore[arg-type]
                str(pattern_val),
            )

        # Parse REGEXP / RLIKE: "col REGEXP 'pattern'", "col RLIKE 'pattern'" (Issue #433)
        regexp_match = re.search(r"\s+(?:REGEXP|RLIKE)\s+", expr, re.IGNORECASE)
        if regexp_match:
            left_str = expr[: regexp_match.start()].strip()
            right_str = expr[regexp_match.end() :].strip()
            left_expr = SQLExprParser._parse_expression(left_str)
            pattern_val = SQLExprParser._parse_simple_value(right_str)
            from ...functions.string import StringFunctions

            return StringFunctions.regexp(
                left_expr,  # type: ignore[arg-type]
                str(pattern_val),
            )

        # Parse IN (literal list): "col IN ('a', 'b')", "col IN (1, 2)" (Issue #370)
        in_match = re.search(r"\s+IN\s*\(", expr, re.IGNORECASE)
        if in_match:
            left_str = expr[: in_match.start()].strip()
            open_paren = in_match.end() - 1  # position of '('
            depth = 1
            i = in_match.end()
            while i < len(expr) and depth > 0:
                if expr[i] == "(":
                    depth += 1
                elif expr[i] == ")":
                    depth -= 1
                i += 1
            if depth == 0:
                in_list_str = expr[open_paren + 1 : i - 1].strip()
                left_expr = SQLExprParser._parse_expression(left_str)
                values = SQLExprParser._parse_function_args(in_list_str)
                if hasattr(left_expr, "isin"):
                    return left_expr.isin(values)
                # If left is not a Column (e.g. literal), wrap as ColumnOperation isin
                return ColumnOperation(left_expr, "isin", values)

        # Parse function calls: LENGTH(name), TRIM(name), pi(), e(), etc.
        # Allow empty arguments for functions like pi() and e()
        func_match = re.match(r"^(\w+)\s*\((.*)\)$", expr, re.IGNORECASE)
        if func_match:
            func_name = func_match.group(1).upper()
            args_str = func_match.group(2)

            # Parse arguments (simple - just split by comma, handle nested parentheses)
            # Handle empty arguments (e.g., pi(), e())
            if args_str.strip():
                args = SQLExprParser._parse_function_args(args_str)
            else:
                args = []

            if func_name == "LENGTH" and len(args) == 1:
                from ...functions.string import StringFunctions

                return StringFunctions.length(args[0])
            elif func_name == "TRIM" and len(args) == 1:
                from ...functions.string import StringFunctions

                return StringFunctions.trim(args[0])
            elif func_name == "UPPER" and len(args) == 1:
                from ...functions.string import StringFunctions

                return StringFunctions.upper(args[0])
            elif func_name == "LOWER" and len(args) == 1:
                from ...functions.string import StringFunctions

                return StringFunctions.lower(args[0])
            else:
                # Generic function - create ColumnOperation
                return ColumnOperation(None, func_name.lower(), args)

        # Parse column reference or literal
        return SQLExprParser._parse_simple_value(expr)

    @staticmethod
    def _parse_simple_value(expr: str) -> Union[Column, Any]:
        """Parse a simple value: column reference or literal."""
        expr = expr.strip()

        # String literal
        if (expr.startswith("'") and expr.endswith("'")) or (
            expr.startswith('"') and expr.endswith('"')
        ):
            return expr[1:-1]  # Return string without quotes

        # Boolean literals
        if expr.upper() == "TRUE":
            return True
        if expr.upper() == "FALSE":
            return False

        # NULL literal
        if expr.upper() == "NULL":
            return None

        # Numeric literal
        try:
            if "." in expr:
                return float(expr)
            else:
                return int(expr)
        except ValueError:
            pass

        # Column reference (identifier)
        # Remove backticks if present
        if expr.startswith("`") and expr.endswith("`"):
            expr = expr[1:-1]

        # Validate identifier (alphanumeric, underscore, and spaces for quoted identifiers)
        # Allow spaces for identifiers like "column name" (though PySpark typically uses backticks)
        if re.match(r"^[a-zA-Z_][a-zA-Z0-9_\s]*$", expr):
            # Remove spaces for column name (normalize)
            normalized = expr.replace(" ", "_")
            return Column(normalized)

        raise ParseException(f"Invalid identifier or literal: {expr}")

    @staticmethod
    def _split_by_operator(expr: str, operator: str) -> List[str]:
        """Split expression by operator, handling string literals."""
        parts = []
        current = ""
        in_string = False
        string_char = None
        i = 0

        while i < len(expr):
            char = expr[i]

            if char in ("'", '"') and (i == 0 or expr[i - 1] != "\\"):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            if not in_string and expr[i : i + len(operator)] == operator:
                if current.strip():
                    parts.append(current.strip())
                current = ""
                i += len(operator)
                continue

            current += char
            i += 1

        if current.strip():
            parts.append(current.strip())

        return parts if len(parts) > 1 else [expr]

    @staticmethod
    def _split_logical_operator(expr: str, op_keyword: str) -> List[str]:
        """Split expression by logical operator (AND/OR), handling parentheses and strings."""
        parts = []
        current = ""
        depth = 0
        in_string = False
        string_char = None
        i = 0

        while i < len(expr):
            char = expr[i]

            # Track string literals so we don't split AND/OR inside '...' or "..."
            if char in ("'", '"') and (i == 0 or expr[i - 1] != "\\"):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            if not in_string:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                elif (
                    depth == 0
                    and expr[i : i + len(op_keyword)].upper() == op_keyword.upper()
                    and (i == 0 or not expr[i - 1].isalnum())
                    and (
                        i + len(op_keyword) >= len(expr)
                        or not expr[i + len(op_keyword)].isalnum()
                    )
                ):
                    if current.strip():
                        parts.append(current.strip())
                    current = ""
                    i += len(op_keyword)
                    while i < len(expr) and expr[i].isspace():
                        i += 1
                    continue

            current += char if i < len(expr) else ""
            i += 1

        if current.strip():
            parts.append(current.strip())

        return parts if len(parts) > 1 else [expr]

    @staticmethod
    def _parse_function_args(args_str: str) -> List[Any]:
        """Parse function arguments, handling nested parentheses."""
        args = []
        current = ""
        depth = 0
        in_string = False
        string_char = None

        for char in args_str:
            if char in ("'", '"') and (len(current) == 0 or current[-1] != "\\"):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            if not in_string:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                elif char == "," and depth == 0:
                    if current.strip():
                        args.append(SQLExprParser._parse_simple_value(current.strip()))
                    current = ""
                    continue

            current += char

        if current.strip():
            args.append(SQLExprParser._parse_simple_value(current.strip()))

        return args
