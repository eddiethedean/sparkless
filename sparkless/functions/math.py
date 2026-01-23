"""
Mathematical functions for Sparkless.

This module provides comprehensive mathematical functions that match PySpark's
math function API. Includes arithmetic operations, rounding functions, trigonometric
functions, and mathematical transformations for numerical processing in DataFrames.

Key Features:
    - Complete PySpark math function API compatibility
    - Arithmetic operations (abs, round, ceil, floor)
    - Advanced math functions (sqrt, exp, log, pow)
    - Trigonometric functions (sin, cos, tan)
    - Type-safe operations with proper return types
    - Support for both column references and numeric literals
    - Proper handling of edge cases and null values

Example:
    >>> from sparkless.sql import SparkSession, functions as F
    >>> spark = SparkSession("test")
    >>> data = [{"value": 3.7, "angle": 1.57}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(
    ...     F.round(F.col("value"), 1),
    ...     F.ceil(F.col("value")),
    ...     F.sin(F.col("angle"))
    ... ).show()
    DataFrame[1 rows, 4 columns]
    round(value, 1) CEIL(value) FLOOR(value) SIN(angle)
    3.7 4.0 3.0 0.9999996829318346
"""

from typing import Optional, Union
from sparkless.functions.base import Column, ColumnOperation


class MathFunctions:
    """Collection of mathematical functions."""

    @staticmethod
    def abs(column: Union[Column, str]) -> ColumnOperation:
        """Get absolute value.

        Args:
            column: The column to get absolute value of.

        Returns:
            ColumnOperation representing the abs function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "abs", name=f"abs({column.name})")
        return operation

    @staticmethod
    def positive(column: Union[Column, str]) -> ColumnOperation:
        """Return positive value (identity function).

        Args:
            column: The column to return as positive.

        Returns:
            ColumnOperation representing the positive function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "positive", name=f"positive({column.name})")
        return operation

    @staticmethod
    def negative(column: Union[Column, str]) -> ColumnOperation:
        """Return negative value.

        Args:
            column: The column to negate.

        Returns:
            ColumnOperation representing the negative function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "negative", name=f"negative({column.name})")
        return operation

    @staticmethod
    def round(column: Union[Column, str], scale: int = 0) -> ColumnOperation:
        """Round to specified number of decimal places.

        Args:
            column: The column to round.
            scale: Number of decimal places (default: 0).

        Returns:
            ColumnOperation representing the round function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "round", scale, name=f"round({column.name}, {scale})"
        )
        return operation

    @staticmethod
    def ceil(column: Union[Column, str]) -> ColumnOperation:
        """Round up to nearest integer.

        Args:
            column: The column to round up.

        Returns:
            ColumnOperation representing the ceil function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "ceil", name=f"CEIL({column.name})")
        return operation

    @staticmethod
    def ceiling(column: Union[Column, str]) -> ColumnOperation:
        """Alias for ceil - Round up to nearest integer.

        Args:
            column: The column to round up.

        Returns:
            ColumnOperation representing the ceiling function.
        """
        return MathFunctions.ceil(column)

    @staticmethod
    def floor(column: Union[Column, str]) -> ColumnOperation:
        """Round down to nearest integer.

        Args:
            column: The column to round down.

        Returns:
            ColumnOperation representing the floor function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "floor", name=f"FLOOR({column.name})")
        return operation

    @staticmethod
    def sqrt(column: Union[Column, str]) -> ColumnOperation:
        """Get square root.

        Args:
            column: The column to get square root of.

        Returns:
            ColumnOperation representing the sqrt function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "sqrt", name=f"SQRT({column.name})")
        return operation

    @staticmethod
    def exp(column: Union[Column, str]) -> ColumnOperation:
        """Get exponential (e^x).

        Args:
            column: The column to get exponential of.

        Returns:
            ColumnOperation representing the exp function.
        """
        if isinstance(column, str):
            column = Column(column)

        # PySpark uses uppercase EXP in column names
        operation = ColumnOperation(column, "exp", name=f"EXP({column.name})")
        return operation

    @staticmethod
    def log(
        base: Union[Column, str, float, int, None],
        column: Optional[Union[Column, str]] = None,
    ) -> ColumnOperation:
        """Get logarithm.

        PySpark signature: log(base, column) or log(column) for natural log.

        Args:
            base: Base for logarithm. Can be a float/int constant or Column.
                  If column is None, base is treated as the column (natural log).
            column: The column to get logarithm of. If None, base is the column (natural log).

        Returns:
            ColumnOperation representing the log function.
        """
        # Handle PySpark's two signatures:
        # 1. log(column) - natural log
        # 2. log(base, column) - log with base
        if column is None:
            # log(column) - natural log, base is actually the column
            if isinstance(base, str):
                column = Column(base)
            elif isinstance(base, Column):
                column = base
            else:
                raise TypeError("log() requires a column when called with one argument")
            base = None
        else:
            # log(base, column) - log with base
            if isinstance(column, str):
                column = Column(column)
            # base can be float, int, or Column
            if isinstance(base, str):
                base = Column(base)

        # PySpark's log() uses natural logarithm and names the column with 'ln'
        if base is None:
            name = f"ln({column.name})"
        elif isinstance(base, (int, float)):
            name = f"log({base}, {column.name})"
        else:
            # base is a Column
            name = f"log({base.name}, {column.name})"

        operation = ColumnOperation(column, "log", base, name=name)
        return operation

    @staticmethod
    def log10(column: Union[Column, str]) -> ColumnOperation:
        """Get base-10 logarithm (PySpark 3.0+).

        Args:
            column: The column to get log10 of.

        Returns:
            ColumnOperation representing the log10 function.

        Example:
            >>> df.select(F.log10(F.col("value")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "log10", name=f"log10({column.name})")

    @staticmethod
    def log2(column: Union[Column, str]) -> ColumnOperation:
        """Get base-2 logarithm (PySpark 3.0+).

        Args:
            column: The column to get log2 of.

        Returns:
            ColumnOperation representing the log2 function.

        Example:
            >>> df.select(F.log2(F.col("value")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "log2", name=f"log2({column.name})")

    @staticmethod
    def log1p(column: Union[Column, str]) -> ColumnOperation:
        """Get natural logarithm of (1 + x) (PySpark 3.0+).

        Computes ln(1 + x) accurately for small values of x.

        Args:
            column: The column to compute log1p of.

        Returns:
            ColumnOperation representing the log1p function.

        Example:
            >>> df.select(F.log1p(F.col("value")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "log1p", name=f"log1p({column.name})")

    @staticmethod
    def expm1(column: Union[Column, str]) -> ColumnOperation:
        """Get exp(x) - 1 (PySpark 3.0+).

        Computes e^x - 1 accurately for small values of x.

        Args:
            column: The column to compute expm1 of.

        Returns:
            ColumnOperation representing the expm1 function.

        Example:
            >>> df.select(F.expm1(F.col("value")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "expm1", name=f"expm1({column.name})")

    @staticmethod
    def pow(
        column: Union[Column, str], exponent: Union[Column, float, int]
    ) -> ColumnOperation:
        """Raise to power.

        Args:
            column: The column to raise to power.
            exponent: The exponent.

        Returns:
            ColumnOperation representing the pow function.
        """
        if isinstance(column, str):
            column = Column(column)

        # PySpark uses uppercase POWER in column names with decimal exponent
        exponent_str = (
            f"{float(exponent)}"
            if isinstance(exponent, (int, float))
            else str(exponent)
        )
        operation = ColumnOperation(
            column, "pow", exponent, name=f"POWER({column.name}, {exponent_str})"
        )
        return operation

    @staticmethod
    def power(
        column: Union[Column, str], exponent: Union[Column, float, int]
    ) -> ColumnOperation:
        """Alias for pow - Raise to power.

        Args:
            column: The column to raise to power.
            exponent: The exponent.

        Returns:
            ColumnOperation representing the power function.
        """
        return MathFunctions.pow(column, exponent)

    @staticmethod
    def sin(column: Union[Column, str]) -> ColumnOperation:
        """Get sine.

        Args:
            column: The column to get sine of.

        Returns:
            ColumnOperation representing the sin function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "sin", name=f"SIN({column.name})")
        return operation

    @staticmethod
    def cos(column: Union[Column, str]) -> ColumnOperation:
        """Get cosine.

        Args:
            column: The column to get cosine of.

        Returns:
            ColumnOperation representing the cos function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "cos", name=f"COS({column.name})")
        return operation

    @staticmethod
    def tan(column: Union[Column, str]) -> ColumnOperation:
        """Get tangent.

        Args:
            column: The column to get tangent of.

        Returns:
            ColumnOperation representing the tan function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "tan", name=f"TAN({column.name})")
        return operation

    @staticmethod
    def sign(column: Union[Column, str]) -> ColumnOperation:
        """Get sign of number (-1, 0, or 1).

        Args:
            column: The column to get sign of.

        Returns:
            ColumnOperation representing the sign function.
        """
        if isinstance(column, str):
            column = Column(column)

        # PySpark 3.2 uses signum, not sign, as the function name
        operation = ColumnOperation(column, "signum", name=f"signum({column.name})")
        return operation

    @staticmethod
    def greatest(*columns: Union[Column, str]) -> ColumnOperation:
        """Get the greatest value among columns.

        Args:
            *columns: Columns to compare.

        Returns:
            ColumnOperation representing the greatest function.
        """
        if not columns:
            raise ValueError("At least one column must be provided")

        base_column = Column(columns[0]) if isinstance(columns[0], str) else columns[0]
        column_names = [
            col.name if hasattr(col, "name") else str(col) for col in columns
        ]
        operation = ColumnOperation(
            base_column,
            "greatest",
            columns[1:],
            name=f"greatest({', '.join(column_names)})",
        )
        return operation

    @staticmethod
    def least(*columns: Union[Column, str]) -> ColumnOperation:
        """Get the least value among columns.

        Args:
            *columns: Columns to compare.

        Returns:
            ColumnOperation representing the least function.
        """
        if not columns:
            raise ValueError("At least one column must be provided")

        base_column = Column(columns[0]) if isinstance(columns[0], str) else columns[0]
        column_names = [
            col.name if hasattr(col, "name") else str(col) for col in columns
        ]
        operation = ColumnOperation(
            base_column, "least", columns[1:], name=f"least({', '.join(column_names)})"
        )
        return operation

    @staticmethod
    def acosh(col: Union[Column, str]) -> ColumnOperation:
        """Compute inverse hyperbolic cosine (arc hyperbolic cosine).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the acosh function.

        Note:
            Input must be >= 1. Returns NaN for invalid inputs.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "acosh", name=f"acosh({column.name})")

    @staticmethod
    def asinh(col: Union[Column, str]) -> ColumnOperation:
        """Compute inverse hyperbolic sine (arc hyperbolic sine).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the asinh function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "asinh", name=f"asinh({column.name})")

    @staticmethod
    def atanh(col: Union[Column, str]) -> ColumnOperation:
        """Compute inverse hyperbolic tangent (arc hyperbolic tangent).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the atanh function.

        Note:
            Input must be in range (-1, 1). Returns NaN for invalid inputs.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "atanh", name=f"atanh({column.name})")

    @staticmethod
    def acos(col: Union[Column, str]) -> ColumnOperation:
        """Compute inverse cosine (arc cosine).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the acos function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "acos", name=f"acos({column.name})")

    @staticmethod
    def asin(col: Union[Column, str]) -> ColumnOperation:
        """Compute inverse sine (arc sine).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the asin function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "asin", name=f"asin({column.name})")

    @staticmethod
    def atan(col: Union[Column, str]) -> ColumnOperation:
        """Compute inverse tangent (arc tangent).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the atan function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "atan", name=f"atan({column.name})")

    @staticmethod
    def atan2(
        y: Union[Column, str, float, int], x: Union[Column, str, float, int]
    ) -> ColumnOperation:
        """Compute 2-argument arctangent (PySpark 3.0+).

        Returns the angle theta from the conversion of rectangular coordinates (x, y)
        to polar coordinates (r, theta).

        Args:
            y: Y coordinate (column or numeric value).
            x: X coordinate (column or numeric value).

        Returns:
            ColumnOperation representing the atan2 function.

        Example:
            >>> df.select(F.atan2(F.col("y"), F.col("x")))
            >>> df.select(F.atan2(F.lit(1.0), F.lit(1.0)))  # Returns π/4
        """
        if isinstance(y, str):
            y = Column(y)
        elif isinstance(y, (int, float)):
            from sparkless.functions.core.literals import Literal

            y = Literal(y)  # type: ignore[assignment]

        return ColumnOperation(y, "atan2", x, name=f"atan2({y}, {x})")

    @staticmethod
    def cosh(col: Union[Column, str]) -> ColumnOperation:
        """Compute hyperbolic cosine.

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the cosh function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "cosh", name=f"cosh({column.name})")

    @staticmethod
    def sinh(col: Union[Column, str]) -> ColumnOperation:
        """Compute hyperbolic sine.

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the sinh function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "sinh", name=f"sinh({column.name})")

    @staticmethod
    def tanh(col: Union[Column, str]) -> ColumnOperation:
        """Compute hyperbolic tangent.

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the tanh function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "tanh", name=f"tanh({column.name})")

    @staticmethod
    def degrees(col: Union[Column, str]) -> ColumnOperation:
        """Convert radians to degrees.

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the degrees function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "degrees", name=f"degrees({column.name})")

    @staticmethod
    def radians(col: Union[Column, str]) -> ColumnOperation:
        """Convert degrees to radians.

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the radians function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "radians", name=f"radians({column.name})")

    @staticmethod
    def cbrt(col: Union[Column, str]) -> ColumnOperation:
        """Compute cube root.

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the cbrt function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "cbrt", name=f"cbrt({column.name})")

    @staticmethod
    def factorial(col: Union[Column, str]) -> ColumnOperation:
        """Compute factorial.

        Args:
            col: Column or column name (non-negative integers).

        Returns:
            ColumnOperation representing the factorial function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "factorial", name=f"factorial({column.name})")

    @staticmethod
    def rand(seed: Optional[int] = None) -> ColumnOperation:
        """Generate a random column with i.i.d. samples from U[0.0, 1.0].

        Args:
            seed: Random seed (optional).

        Returns:
            ColumnOperation representing the rand function.
        """
        from sparkless.functions.core.literals import Literal

        return ColumnOperation(
            Literal(0),
            "rand",
            value=seed,
            name=f"rand({seed})" if seed is not None else "rand()",
        )

    @staticmethod
    def randn(seed: Optional[int] = None) -> ColumnOperation:
        """Generate a random column with i.i.d. samples from standard normal distribution.

        Args:
            seed: Random seed (optional).

        Returns:
            ColumnOperation representing the randn function.
        """
        from sparkless.functions.core.literals import Literal

        return ColumnOperation(
            Literal(0),
            "randn",
            value=seed,
            name=f"randn({seed})" if seed is not None else "randn()",
        )

    @staticmethod
    def rint(col: Union[Column, str]) -> ColumnOperation:
        """Round to nearest integer using banker's rounding (half to even).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the rint function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "rint", name=f"rint({column.name})")

    @staticmethod
    def bround(col: Union[Column, str], scale: int = 0) -> ColumnOperation:
        """Round using HALF_EVEN rounding mode (banker's rounding).

        Args:
            col: Column or column name.
            scale: Number of decimal places (default 0).

        Returns:
            ColumnOperation representing the bround function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(
            column, "bround", value=scale, name=f"bround({column.name}, {scale})"
        )

    @staticmethod
    def hypot(col1: Union[Column, str], col2: Union[Column, str]) -> ColumnOperation:
        """Compute sqrt(col1^2 + col2^2) (hypotenuse).

        Args:
            col1: First column
            col2: Second column

        Returns:
            ColumnOperation representing the hypot function.
        """
        column1 = Column(col1) if isinstance(col1, str) else col1
        column2 = Column(col2) if isinstance(col2, str) else col2

        return ColumnOperation(
            column1,
            "hypot",
            value=column2,
            name=f"hypot({column1.name}, {column2.name})",
        )

    @staticmethod
    def nanvl(
        col1: Union[Column, str], col2: Union[Column, str, int, float]
    ) -> ColumnOperation:
        """Returns col1 if not NaN, or col2 if col1 is NaN.

        Args:
            col1: First column
            col2: Second column or literal value (replacement for NaN)

        Returns:
            ColumnOperation representing the nanvl function.
        """
        from .core.literals import Literal
        from typing import Union, Any

        column1 = Column(col1) if isinstance(col1, str) else col1
        column2: Union[Column, Literal, Any]
        if isinstance(col2, (int, float)):
            column2 = Literal(col2)
        elif isinstance(col2, str):
            column2 = Column(col2)
        else:
            column2 = col2

        col2_name = (
            column2.name
            if hasattr(column2, "name")
            else str(column2.value)
            if hasattr(column2, "value")
            else str(col2)
        )
        # PySpark generates: CASE WHEN (NOT (col1 = col1)) THEN col2 ELSE col1 END
        # For now, use the simpler nanvl name, but we could generate CASE WHEN if needed
        # The test expects: CASE WHEN (NOT (salary = salary)) THEN 0 ELSE salary END
        col1_name = column1.name if hasattr(column1, "name") else str(column1)
        name = f"CASE WHEN (NOT ({col1_name} = {col1_name})) THEN {col2_name} ELSE {col1_name} END"
        return ColumnOperation(
            column1,
            "nanvl",
            value=column2,
            name=name,
        )

    @staticmethod
    def signum(col: Union[Column, str]) -> ColumnOperation:
        """Compute the signum function (sign: -1, 0, or 1).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the signum function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "signum", name=f"signum({column.name})")

    # Priority 2: New Math Functions (PySpark 3.3+/3.5+)
    @staticmethod
    def cot(col: Union[Column, str]) -> ColumnOperation:
        """Compute cotangent (PySpark 3.3+).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the cot function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "cot", name=f"cot({column.name})")

    @staticmethod
    def csc(col: Union[Column, str]) -> ColumnOperation:
        """Compute cosecant (PySpark 3.3+).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the csc function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "csc", name=f"csc({column.name})")

    @staticmethod
    def sec(col: Union[Column, str]) -> ColumnOperation:
        """Compute secant (PySpark 3.3+).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the sec function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "sec", name=f"sec({column.name})")

    @staticmethod
    def e() -> ColumnOperation:
        """Return Euler's number e (PySpark 3.5+).

        Returns:
            ColumnOperation representing Euler's number constant.
        """
        from sparkless.functions.core.literals import Literal
        import math

        return ColumnOperation(Literal(math.e), "lit", name="E()")

    @staticmethod
    def pi() -> ColumnOperation:
        """Return the value of pi (PySpark 3.5+).

        Returns:
            ColumnOperation representing pi constant.
        """
        from sparkless.functions.core.literals import Literal
        import math

        return ColumnOperation(Literal(math.pi), "lit", name="PI()")

    @staticmethod
    def ln(col: Union[Column, str]) -> ColumnOperation:
        """Compute natural logarithm (alias for log) (PySpark 3.5+).

        Args:
            col: Column or column name.

        Returns:
            ColumnOperation representing the ln function.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "log", name=f"ln({column.name})")

    # Deprecated Aliases
    @staticmethod
    def toDegrees(column: Union[Column, str]) -> ColumnOperation:
        """Deprecated alias for degrees (all PySpark versions).

        Use degrees instead.

        Args:
            column: Angle in radians.

        Returns:
            ColumnOperation representing the degrees conversion.
        """
        import warnings

        warnings.warn(
            "toDegrees is deprecated. Use degrees instead.", FutureWarning, stacklevel=2
        )
        return MathFunctions.degrees(column)

    @staticmethod
    def toRadians(column: Union[Column, str]) -> ColumnOperation:
        """Deprecated alias for radians (all PySpark versions).

        Use radians instead.

        Args:
            column: Angle in degrees.

        Returns:
            ColumnOperation representing the radians conversion.
        """
        import warnings

        warnings.warn(
            "toRadians is deprecated. Use radians instead.", FutureWarning, stacklevel=2
        )
        return MathFunctions.radians(column)

    @staticmethod
    def pmod(
        dividend: Union[Column, str, int], divisor: Union[Column, str, int]
    ) -> ColumnOperation:
        """Positive modulo - always returns positive remainder.

        Args:
            dividend: The dividend.
            divisor: The divisor.

        Returns:
            ColumnOperation representing the pmod function.
        """
        if isinstance(dividend, (str, int)):
            from sparkless.functions.base import Column

            dividend = (
                Column(str(dividend)) if isinstance(dividend, int) else Column(dividend)
            )
        if isinstance(divisor, (str, int)):
            from sparkless.functions.base import Column

            divisor = (
                Column(str(divisor)) if isinstance(divisor, int) else Column(divisor)
            )

        operation = ColumnOperation(
            dividend,
            "pmod",
            divisor,
            name=f"pmod({dividend.name}, {divisor.name if hasattr(divisor, 'name') else divisor})",
        )
        return operation

    @staticmethod
    def negate(column: Union[Column, str]) -> ColumnOperation:
        """Negate value (alias for negative).

        Args:
            column: The column to negate.

        Returns:
            ColumnOperation representing the negate function.
        """
        return MathFunctions.negative(column)

    @staticmethod
    def getbit(
        column: Union[Column, str], bit: Union[Column, str, int]
    ) -> ColumnOperation:
        """Get bit at specified position (PySpark 3.5+).

        Args:
            column: The column containing the integer.
            bit: The bit position (0-indexed from right).

        Returns:
            ColumnOperation representing the getbit function.

        Example:
            >>> df.select(F.getbit(F.col("value"), 3))
        """
        from sparkless.functions.base import Column

        if isinstance(column, str):
            column = Column(column)
        if isinstance(bit, (str, int)):
            bit = Column(str(bit)) if isinstance(bit, int) else Column(bit)

        operation = ColumnOperation(
            column,
            "getbit",
            value=bit,
            name=f"getbit({column.name}, {bit.name if hasattr(bit, 'name') else bit})",
        )
        return operation

    @staticmethod
    def width_bucket(
        value: Union[Column, str],
        min_value: Union[Column, str, float],
        max_value: Union[Column, str, float],
        num_buckets: Union[Column, str, int],
    ) -> ColumnOperation:
        """Compute histogram bucket number for value (PySpark 3.5+).

        Args:
            value: The value to compute bucket for.
            min_value: Minimum value of the range.
            max_value: Maximum value of the range.
            num_buckets: Number of buckets.

        Returns:
            ColumnOperation representing the width_bucket function.

        Example:
            >>> df.select(F.width_bucket(F.col("value"), 0.0, 100.0, 10))
        """
        from sparkless.functions.base import Column

        if isinstance(value, str):
            value = Column(value)
        if isinstance(min_value, (str, float, int)):
            min_value = (
                Column(str(min_value))
                if isinstance(min_value, (int, float))
                else Column(min_value)
            )
        if isinstance(max_value, (str, float, int)):
            from sparkless.functions.base import Column

            max_value = (
                Column(str(max_value))
                if isinstance(max_value, (int, float))
                else Column(max_value)
            )
        if isinstance(num_buckets, (str, int)):
            from sparkless.functions.base import Column

            num_buckets = (
                Column(str(num_buckets))
                if isinstance(num_buckets, int)
                else Column(num_buckets)
            )

        # Store all parameters as a tuple in value
        params = (min_value, max_value, num_buckets)
        min_repr = min_value.name if hasattr(min_value, "name") else str(min_value)
        max_repr = max_value.name if hasattr(max_value, "name") else str(max_value)
        num_repr = (
            num_buckets.name if hasattr(num_buckets, "name") else str(num_buckets)
        )

        operation = ColumnOperation(
            value,
            "width_bucket",
            value=params,
            name=f"width_bucket({value.name}, {min_repr}, {max_repr}, {num_repr})",
        )
        return operation
