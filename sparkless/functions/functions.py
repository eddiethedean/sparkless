"""
Core functions module for Sparkless.

This module provides the main F namespace and re-exports all function classes
for backward compatibility with the original functions.py structure. The Functions
class serves as the primary interface for all PySpark-compatible functions.

Key Features:
    - Complete PySpark F namespace compatibility
    - Column functions (col, lit, when, coalesce, isnull)
    - String functions (upper, lower, length, trim, regexp_replace, split)
    - Math functions (abs, round, ceil, floor, sqrt, exp, log, pow, sin, cos, tan)
    - Aggregate functions (count, sum, avg, max, min, stddev, variance)
    - DateTime functions (current_timestamp, current_date, to_date, to_timestamp)
    - Window functions (row_number, rank, dense_rank, lag, lead)

Example:
    >>> from sparkless.sql import SparkSession, functions as F
    >>> spark = SparkSession("test")
    >>> data = [{"name": "Alice", "age": 25}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.upper(F.col("name")), F.col("age") * 2).show()
    DataFrame[1 rows, 2 columns]

    upper(name) (age * 2)
    ALICE        50
"""

from typing import Any, Callable, Dict, Optional, TYPE_CHECKING, Tuple, Union
from .core.column import Column, ColumnOperation

if TYPE_CHECKING:
    from .conditional import CaseWhen
    from .core.literals import Literal
else:
    CaseWhen = Any
    Literal = Any
from .core.literals import Literal
from .base import AggregateFunction
from .conditional import CaseWhen, ConditionalFunctions
from .window_execution import WindowFunction
from .string import StringFunctions
from .math import MathFunctions
from .aggregate import AggregateFunctions
from .datetime import DateTimeFunctions
from .array import ArrayFunctions
from .map import MapFunctions
from .bitwise import BitwiseFunctions
from .xml import XMLFunctions
from .crypto import CryptoFunctions
from ..errors import PySparkTypeError, PySparkValueError

if TYPE_CHECKING:
    from ..session import SparkSession


class Functions:
    """Main functions namespace (F) for Sparkless.

    This class provides access to all functions in a PySpark-compatible way.
    """

    # Column functions
    @staticmethod
    def _resolve_session(session: Optional["SparkSession"]) -> "SparkSession":
        """Resolve an active SparkSession for session-aware functions."""
        from ..session import SparkSession

        if session is not None:
            return session

        # Use getActiveSession() for PySpark compatibility
        active = SparkSession.getActiveSession()
        if active is not None:
            return active

        raise PySparkValueError(
            "No active SparkSession found. Call SparkSession.builder.getOrCreate() "
            "or pass a session explicitly."
        )

    @staticmethod
    def _require_active_session(operation_name: str) -> None:
        """Require an active SparkSession for the operation.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        from ..session import SparkSession

        if not SparkSession._has_active_session():
            raise RuntimeError(
                f"Cannot perform {operation_name}: "
                "No active SparkSession found. "
                "This operation requires an active SparkSession, similar to PySpark. "
                "Create a SparkSession first: spark = SparkSession('app_name')"
            )

    @staticmethod
    def col(name: str) -> Column:
        """Create a column reference.

        Note:
            In PySpark, col() can be called without an active SparkSession.
            The column expression is evaluated later when used with a DataFrame.
        """
        return Column(name)

    @staticmethod
    def lit(value: Any) -> Literal:
        """Create a literal value.

        Note:
            In PySpark, lit() can be called without an active SparkSession.
            The literal expression is evaluated later when used with a DataFrame.
        """
        return Literal(value)

    @staticmethod
    def cast(column: Union[Column, str], data_type: Any) -> ColumnOperation:
        """Cast column to different data type.

        Args:
            column: The column to cast.
            data_type: The target data type.

        Returns:
            ColumnOperation representing the cast function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("cast operation")
        if isinstance(column, str):
            column = Column(column)
        return column.cast(data_type)

    @staticmethod
    def current_catalog(session: Optional["SparkSession"] = None) -> Literal:
        """Return the current catalog name as a literal."""
        # Validate session at creation time (matches PySpark behavior)
        Functions._resolve_session(session)

        # Store the explicit session if provided, otherwise will resolve at evaluation
        explicit_session = session

        def resolver() -> str:
            """Resolve catalog name from session at evaluation time."""

            # Use explicit session if provided, otherwise resolve from singleton
            if explicit_session is not None:
                eval_spark = explicit_session
            else:
                eval_spark = Functions._resolve_session(None)
            catalog = getattr(eval_spark.catalog, "currentCatalog", None)
            return catalog() if callable(catalog) else "spark_catalog"

        # Create lazy literal that resolves at evaluation time
        # Don't call resolver() here - it will be called during evaluation
        literal = Literal("", resolver=resolver)
        literal.name = "current_catalog()"
        return literal

    @staticmethod
    def current_database(session: Optional["SparkSession"] = None) -> Literal:
        """Return the current database/schema as a literal."""
        # Validate session at creation time (matches PySpark behavior)
        Functions._resolve_session(session)

        # Store the explicit session if provided, otherwise will resolve at evaluation
        explicit_session = session

        def resolver() -> str:
            """Resolve database name from session at evaluation time."""

            # Use explicit session if provided, otherwise resolve from singleton
            if explicit_session is not None:
                return explicit_session.catalog.currentDatabase()
            else:
                eval_spark = Functions._resolve_session(None)
                return eval_spark.catalog.currentDatabase()

        # Create lazy literal that resolves at evaluation time
        # Don't call resolver() here - it will be called during evaluation
        literal = Literal("", resolver=resolver)
        literal.name = "current_database()"
        return literal

    @staticmethod
    def current_schema(session: Optional["SparkSession"] = None) -> Literal:
        """Alias for current_database (Spark SQL compatibility)."""
        # Validate session at creation time (matches PySpark behavior)
        Functions._resolve_session(session)

        # Store the explicit session if provided, otherwise will resolve at evaluation
        explicit_session = session

        def resolver() -> str:
            """Resolve schema name from session at evaluation time."""

            # Use explicit session if provided, otherwise resolve from singleton
            if explicit_session is not None:
                return explicit_session.catalog.currentDatabase()
            else:
                eval_spark = Functions._resolve_session(None)
                return eval_spark.catalog.currentDatabase()

        # Create lazy literal that resolves at evaluation time
        # Don't call resolver() here - it will be called during evaluation
        literal = Literal("", resolver=resolver)
        literal.name = "current_schema()"
        return literal

    @staticmethod
    def current_user(session: Optional["SparkSession"] = None) -> Literal:
        """Return the current Spark user as a literal."""
        # Validate session at creation time (matches PySpark behavior)
        Functions._resolve_session(session)

        # Store the explicit session if provided, otherwise will resolve at evaluation
        explicit_session = session

        def resolver() -> str:
            """Resolve user name from session at evaluation time."""

            # Use explicit session if provided, otherwise resolve from singleton
            if explicit_session is not None:
                eval_spark = explicit_session
            else:
                eval_spark = Functions._resolve_session(None)
            spark_user = getattr(eval_spark.sparkContext, "sparkUser", None)
            return spark_user() if callable(spark_user) else "mock_user"

        # Create lazy literal that resolves at evaluation time
        # Don't call resolver() here - it will be called during evaluation
        literal = Literal("", resolver=resolver)
        literal.name = "current_user()"
        return literal

    # String functions
    @staticmethod
    def upper(column: Union[Column, str]) -> ColumnOperation:
        """Convert string to uppercase."""
        return StringFunctions.upper(column)

    @staticmethod
    def lower(column: Union[Column, str]) -> ColumnOperation:
        """Convert string to lowercase."""
        return StringFunctions.lower(column)

    @staticmethod
    def length(column: Union[Column, str]) -> ColumnOperation:
        """Get string length."""
        return StringFunctions.length(column)

    @staticmethod
    def char_length(column: Union[Column, str]) -> ColumnOperation:
        """Get character length (alias for length) (PySpark 3.5+)."""
        return StringFunctions.char_length(column)

    @staticmethod
    def character_length(column: Union[Column, str]) -> ColumnOperation:
        """Get character length (alias for length) (PySpark 3.5+)."""
        return StringFunctions.character_length(column)

    @staticmethod
    def trim(column: Union[Column, str]) -> ColumnOperation:
        """Trim whitespace."""
        return StringFunctions.trim(column)

    @staticmethod
    def ltrim(column: Union[Column, str]) -> ColumnOperation:
        """Trim left whitespace."""
        return StringFunctions.ltrim(column)

    @staticmethod
    def rtrim(column: Union[Column, str]) -> ColumnOperation:
        """Trim right whitespace."""
        return StringFunctions.rtrim(column)

    @staticmethod
    def regexp_replace(
        column: Union[Column, str], pattern: str, replacement: str
    ) -> ColumnOperation:
        """Replace regex pattern."""
        return StringFunctions.regexp_replace(column, pattern, replacement)

    @staticmethod
    def split(
        column: Union[Column, str], delimiter: str, limit: Optional[int] = None
    ) -> ColumnOperation:
        """Split string by delimiter.

        Args:
            column: The column to split.
            delimiter: The delimiter to split on.
            limit: Optional limit on the number of times the pattern is applied.
                   If None or -1, no limit (default PySpark behavior).
        """
        return StringFunctions.split(column, delimiter, limit)

    @staticmethod
    def substring(
        column: Union[Column, str], start: int, length: Optional[int] = None
    ) -> ColumnOperation:
        """Extract substring."""
        return StringFunctions.substring(column, start, length)

    @staticmethod
    def concat(*columns: Union[Column, str]) -> ColumnOperation:
        """Concatenate strings."""
        return StringFunctions.concat(*columns)

    @staticmethod
    def format_string(format_str: str, *columns: Union[Column, str]) -> ColumnOperation:
        """Format string using printf-style placeholders."""
        return StringFunctions.format_string(format_str, *columns)

    @staticmethod
    def translate(
        column: Union[Column, str], matching_string: str, replace_string: str
    ) -> ColumnOperation:
        """Translate characters in a string using a character mapping."""
        return StringFunctions.translate(column, matching_string, replace_string)

    @staticmethod
    def ascii(column: Union[Column, str]) -> ColumnOperation:
        """Return ASCII value of the first character."""
        return StringFunctions.ascii(column)

    @staticmethod
    def base64(column: Union[Column, str]) -> ColumnOperation:
        """Encode the string to base64."""
        return StringFunctions.base64(column)

    @staticmethod
    def btrim(
        column: Union[Column, str], trim_string: Optional[str] = None
    ) -> ColumnOperation:
        """Trim characters from both ends of string."""
        return StringFunctions.btrim(column, trim_string)

    @staticmethod
    def contains(column: Union[Column, str], substring: str) -> ColumnOperation:
        """Check if string contains substring."""
        return StringFunctions.contains(column, substring)

    @staticmethod
    def left(column: Union[Column, str], length: int) -> ColumnOperation:
        """Extract left N characters from string."""
        return StringFunctions.left(column, length)

    @staticmethod
    def right(column: Union[Column, str], length: int) -> ColumnOperation:
        """Extract right N characters from string."""
        return StringFunctions.right(column, length)

    @staticmethod
    def bit_length(column: Union[Column, str]) -> ColumnOperation:
        """Get bit length of string."""
        return StringFunctions.bit_length(column)

    @staticmethod
    def startswith(column: Union[Column, str], substring: str) -> ColumnOperation:
        """Check if string starts with substring."""
        return StringFunctions.startswith(column, substring)

    @staticmethod
    def endswith(column: Union[Column, str], substring: str) -> ColumnOperation:
        """Check if string ends with substring."""
        return StringFunctions.endswith(column, substring)

    @staticmethod
    def like(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """SQL LIKE pattern matching."""
        return StringFunctions.like(column, pattern)

    @staticmethod
    def rlike(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """Regular expression pattern matching."""
        return StringFunctions.rlike(column, pattern)

    @staticmethod
    def isin(column: Union[Column, str], *values: Any) -> ColumnOperation:
        """Check if column value is in list of values.

        Args:
            column: The column to check.
            *values: Variable number of values to check against.

        Returns:
            ColumnOperation representing the isin function.
        """
        if isinstance(column, str):
            column = Column(column)
        return column.isin(*values)

    @staticmethod
    def replace(column: Union[Column, str], old: str, new: str) -> ColumnOperation:
        """Replace occurrences of substring in string."""
        return StringFunctions.replace(column, old, new)

    @staticmethod
    def substr(
        column: Union[Column, str], start: int, length: Optional[int] = None
    ) -> ColumnOperation:
        """Alias for substring - Extract substring from string."""
        return StringFunctions.substr(column, start, length)

    @staticmethod
    def split_part(
        column: Union[Column, str], delimiter: str, part: int
    ) -> ColumnOperation:
        """Extract part of string split by delimiter."""
        return StringFunctions.split_part(column, delimiter, part)

    @staticmethod
    def position(
        substring: Union[Column, str], column: Union[Column, str]
    ) -> ColumnOperation:
        """Find position of substring in string (1-indexed)."""
        return StringFunctions.position(substring, column)

    @staticmethod
    def octet_length(column: Union[Column, str]) -> ColumnOperation:
        """Get byte length (octet length) of string."""
        return StringFunctions.octet_length(column)

    @staticmethod
    def char(column: Union[Column, str]) -> ColumnOperation:
        """Convert integer to character."""
        return StringFunctions.char(column)

    @staticmethod
    def ucase(column: Union[Column, str]) -> ColumnOperation:
        """Alias for upper - Convert string to uppercase."""
        return StringFunctions.ucase(column)

    @staticmethod
    def lcase(column: Union[Column, str]) -> ColumnOperation:
        """Alias for lower - Convert string to lowercase."""
        return StringFunctions.lcase(column)

    @staticmethod
    def elt(n: Union[Column, int], *columns: Union[Column, str]) -> ColumnOperation:
        """Return element at index from list of columns."""
        return StringFunctions.elt(n, *columns)

    @staticmethod
    def unbase64(column: Union[Column, str]) -> ColumnOperation:
        """Decode a base64-encoded string."""
        return StringFunctions.unbase64(column)

    @staticmethod
    def md5(column: Union[Column, str]) -> ColumnOperation:
        """MD5 hash (PySpark 3.0+)."""
        return StringFunctions.md5(column)

    @staticmethod
    def sha1(column: Union[Column, str]) -> ColumnOperation:
        """SHA-1 hash (PySpark 3.0+)."""
        return StringFunctions.sha1(column)

    @staticmethod
    def sha2(column: Union[Column, str], numBits: int) -> ColumnOperation:
        """SHA-2 hash family (PySpark 3.0+)."""
        return StringFunctions.sha2(column, numBits)

    @staticmethod
    def sha(column: Union[Column, str]) -> ColumnOperation:
        """SHA-1 hash alias (PySpark 3.5+)."""
        return StringFunctions.sha(column)

    @staticmethod
    def mask(
        column: Union[Column, str],
        upperChar: Optional[str] = None,
        lowerChar: Optional[str] = None,
        digitChar: Optional[str] = None,
        otherChar: Optional[str] = None,
    ) -> ColumnOperation:
        """Mask sensitive data in a string (PySpark 3.5+)."""
        return StringFunctions.mask(column, upperChar, lowerChar, digitChar, otherChar)

    @staticmethod
    def json_array_length(
        column: Union[Column, str], path: Optional[str] = None
    ) -> ColumnOperation:
        """Get the length of a JSON array (PySpark 3.5+)."""
        return StringFunctions.json_array_length(column, path)

    @staticmethod
    def json_object_keys(
        column: Union[Column, str], path: Optional[str] = None
    ) -> ColumnOperation:
        """Get the keys of a JSON object (PySpark 3.5+)."""
        return StringFunctions.json_object_keys(column, path)

    @staticmethod
    def xpath_number(column: Union[Column, str], path: str) -> ColumnOperation:
        """Extract number from XML using XPath (PySpark 3.5+)."""
        return StringFunctions.xpath_number(column, path)

    @staticmethod
    def user() -> ColumnOperation:
        """Get current user name (PySpark 3.5+)."""
        return StringFunctions.user()

    @staticmethod
    def crc32(column: Union[Column, str]) -> ColumnOperation:
        """CRC32 checksum (PySpark 3.0+)."""
        return StringFunctions.crc32(column)

    # Cryptographic functions
    @staticmethod
    def aes_encrypt(
        data: Union[Column, str],
        key: Union[Column, str],
        mode: Optional[str] = None,
        padding: Optional[str] = None,
    ) -> ColumnOperation:
        """Encrypt data using AES encryption (PySpark 3.5+)."""
        return CryptoFunctions.aes_encrypt(data, key, mode, padding)

    @staticmethod
    def aes_decrypt(
        data: Union[Column, str],
        key: Union[Column, str],
        mode: Optional[str] = None,
        padding: Optional[str] = None,
    ) -> ColumnOperation:
        """Decrypt data using AES decryption (PySpark 3.5+)."""
        return CryptoFunctions.aes_decrypt(data, key, mode, padding)

    @staticmethod
    def try_aes_decrypt(
        data: Union[Column, str],
        key: Union[Column, str],
        mode: Optional[str] = None,
        padding: Optional[str] = None,
    ) -> ColumnOperation:
        """Null-safe AES decryption - returns NULL on error (PySpark 3.5+)."""
        return CryptoFunctions.try_aes_decrypt(data, key, mode, padding)

    @staticmethod
    def to_str(column: Union[Column, str]) -> ColumnOperation:
        """Convert column to string (all PySpark versions)."""
        return StringFunctions.to_str(column)

    @staticmethod
    def regexp_extract_all(
        column: Union[Column, str], pattern: str, idx: int = 0
    ) -> ColumnOperation:
        """Extract all matches of a regex pattern."""
        return StringFunctions.regexp_extract_all(column, pattern, idx)

    @staticmethod
    def array_join(
        column: Union[Column, str],
        delimiter: str,
        null_replacement: Optional[str] = None,
    ) -> ColumnOperation:
        """Join array elements with a delimiter."""
        return StringFunctions.array_join(column, delimiter, null_replacement)

    @staticmethod
    def repeat(column: Union[Column, str], n: int) -> ColumnOperation:
        """Repeat a string N times."""
        return StringFunctions.repeat(column, n)

    @staticmethod
    def concat_ws(sep: str, *cols: Union[Column, str]) -> ColumnOperation:
        """Concatenate multiple columns with separator."""
        return StringFunctions.concat_ws(sep, *cols)

    @staticmethod
    def regexp_extract(
        column: Union[Column, str], pattern: str, idx: int = 0
    ) -> ColumnOperation:
        """Extract specific group matched by regex."""
        return StringFunctions.regexp_extract(column, pattern, idx)

    @staticmethod
    def substring_index(
        column: Union[Column, str], delim: str, count: int
    ) -> ColumnOperation:
        """Returns substring before/after count occurrences of delimiter."""
        return StringFunctions.substring_index(column, delim, count)

    @staticmethod
    def format_number(column: Union[Column, str], d: int) -> ColumnOperation:
        """Format number with d decimal places and thousands separator."""
        return StringFunctions.format_number(column, d)

    @staticmethod
    def instr(column: Union[Column, str], substr: str) -> ColumnOperation:
        """Locate position of first occurrence of substr."""
        return StringFunctions.instr(column, substr)

    @staticmethod
    def locate(
        substr: str, column: Union[Column, str], pos: int = 1
    ) -> ColumnOperation:
        """Locate position of substr starting from pos."""
        return StringFunctions.locate(substr, column, pos)

    @staticmethod
    def lpad(column: Union[Column, str], len: int, pad: str) -> ColumnOperation:
        """Left-pad string to length len with pad string."""
        return StringFunctions.lpad(column, len, pad)

    @staticmethod
    def rpad(column: Union[Column, str], len: int, pad: str) -> ColumnOperation:
        """Right-pad string to length len with pad string."""
        return StringFunctions.rpad(column, len, pad)

    @staticmethod
    def levenshtein(
        left: Union[Column, str], right: Union[Column, str]
    ) -> ColumnOperation:
        """Compute Levenshtein distance between two strings."""
        return StringFunctions.levenshtein(left, right)

    @staticmethod
    def bin(column: Union[Column, str]) -> ColumnOperation:
        """Convert to binary string."""
        return StringFunctions.bin(column)

    @staticmethod
    def hex(column: Union[Column, str]) -> ColumnOperation:
        """Convert to hexadecimal string."""
        return StringFunctions.hex(column)

    @staticmethod
    def unhex(column: Union[Column, str]) -> ColumnOperation:
        """Convert hex string to binary."""
        return StringFunctions.unhex(column)

    @staticmethod
    def hash(*cols: Union[Column, str]) -> ColumnOperation:
        """Compute hash value."""
        return StringFunctions.hash(*cols)

    @staticmethod
    def xxhash64(*cols: Union[Column, str]) -> ColumnOperation:
        """Compute xxHash64 value (all PySpark versions)."""
        return StringFunctions.xxhash64(*cols)

    @staticmethod
    def encode(column: Union[Column, str], charset: str) -> ColumnOperation:
        """Encode string to binary."""
        return StringFunctions.encode(column, charset)

    @staticmethod
    def decode(column: Union[Column, str], charset: str) -> ColumnOperation:
        """Decode binary to string."""
        return StringFunctions.decode(column, charset)

    @staticmethod
    def conv(
        column: Union[Column, str], from_base: int, to_base: int
    ) -> ColumnOperation:
        """Convert number between bases."""
        return StringFunctions.conv(column, from_base, to_base)

    @staticmethod
    def initcap(column: Union[Column, str]) -> ColumnOperation:
        """Capitalize first letter of each word."""
        return StringFunctions.initcap(column)

    @staticmethod
    def soundex(column: Union[Column, str]) -> ColumnOperation:
        """Soundex encoding for phonetic matching."""
        return StringFunctions.soundex(column)

    # Math functions
    @staticmethod
    def abs(column: Union[Column, str]) -> ColumnOperation:
        """Get absolute value."""
        return MathFunctions.abs(column)

    @staticmethod
    def round(column: Union[Column, str], scale: int = 0) -> ColumnOperation:
        """Round to decimal places."""
        return MathFunctions.round(column, scale)

    @staticmethod
    def ceil(column: Union[Column, str]) -> ColumnOperation:
        """Round up."""
        return MathFunctions.ceil(column)

    @staticmethod
    def ceiling(column: Union[Column, str]) -> ColumnOperation:
        """Alias for ceil - Round up to nearest integer."""
        return MathFunctions.ceiling(column)

    @staticmethod
    def floor(column: Union[Column, str]) -> ColumnOperation:
        """Round down."""
        return MathFunctions.floor(column)

    @staticmethod
    def sqrt(column: Union[Column, str]) -> ColumnOperation:
        """Square root."""
        return MathFunctions.sqrt(column)

    @staticmethod
    def exp(column: Union[Column, str]) -> ColumnOperation:
        """Exponential."""
        return MathFunctions.exp(column)

    @staticmethod
    def log(
        base: Union[Column, str, float, int, None],
        column: Optional[Union[Column, str]] = None,
    ) -> ColumnOperation:
        """Logarithm.

        PySpark signature: log(base, column) or log(column) for natural log.
        """
        return MathFunctions.log(base, column)

    @staticmethod
    def log10(column: Union[Column, str]) -> ColumnOperation:
        """Base-10 logarithm (PySpark 3.0+)."""
        return MathFunctions.log10(column)

    @staticmethod
    def log2(column: Union[Column, str]) -> ColumnOperation:
        """Base-2 logarithm (PySpark 3.0+)."""
        return MathFunctions.log2(column)

    @staticmethod
    def log1p(column: Union[Column, str]) -> ColumnOperation:
        """Natural log of (1 + x) (PySpark 3.0+)."""
        return MathFunctions.log1p(column)

    @staticmethod
    def expm1(column: Union[Column, str]) -> ColumnOperation:
        """exp(x) - 1 (PySpark 3.0+)."""
        return MathFunctions.expm1(column)

    @staticmethod
    def pow(
        column: Union[Column, str], exponent: Union[Column, float, int]
    ) -> ColumnOperation:
        """Power."""
        return MathFunctions.pow(column, exponent)

    @staticmethod
    def power(
        column: Union[Column, str], exponent: Union[Column, float, int]
    ) -> ColumnOperation:
        """Alias for pow - Raise to power."""
        return MathFunctions.power(column, exponent)

    @staticmethod
    def positive(column: Union[Column, str]) -> ColumnOperation:
        """Return positive value (identity function)."""
        return MathFunctions.positive(column)

    @staticmethod
    def negative(column: Union[Column, str]) -> ColumnOperation:
        """Return negative value."""
        return MathFunctions.negative(column)

    @staticmethod
    def sin(column: Union[Column, str]) -> ColumnOperation:
        """Sine."""
        return MathFunctions.sin(column)

    @staticmethod
    def cos(column: Union[Column, str]) -> ColumnOperation:
        """Cosine."""
        return MathFunctions.cos(column)

    @staticmethod
    def tan(column: Union[Column, str]) -> ColumnOperation:
        """Tangent."""
        return MathFunctions.tan(column)

    @staticmethod
    def acosh(column: Union[Column, str]) -> ColumnOperation:
        """Inverse hyperbolic cosine (PySpark 3.0+)."""
        return MathFunctions.acosh(column)

    @staticmethod
    def asinh(column: Union[Column, str]) -> ColumnOperation:
        """Inverse hyperbolic sine (PySpark 3.0+)."""
        return MathFunctions.asinh(column)

    @staticmethod
    def atanh(column: Union[Column, str]) -> ColumnOperation:
        """Inverse hyperbolic tangent (PySpark 3.0+)."""
        return MathFunctions.atanh(column)

    @staticmethod
    def acos(column: Union[Column, str]) -> ColumnOperation:
        """Inverse cosine (arc cosine)."""
        return MathFunctions.acos(column)

    @staticmethod
    def asin(column: Union[Column, str]) -> ColumnOperation:
        """Inverse sine (arc sine)."""
        return MathFunctions.asin(column)

    @staticmethod
    def atan(column: Union[Column, str]) -> ColumnOperation:
        """Inverse tangent (arc tangent)."""
        return MathFunctions.atan(column)

    @staticmethod
    def atan2(
        y: Union[Column, str, float, int], x: Union[Column, str, float, int]
    ) -> ColumnOperation:
        """2-argument arctangent (PySpark 3.0+)."""
        return MathFunctions.atan2(y, x)

    @staticmethod
    def cosh(column: Union[Column, str]) -> ColumnOperation:
        """Hyperbolic cosine."""
        return MathFunctions.cosh(column)

    @staticmethod
    def sinh(column: Union[Column, str]) -> ColumnOperation:
        """Hyperbolic sine."""
        return MathFunctions.sinh(column)

    @staticmethod
    def tanh(column: Union[Column, str]) -> ColumnOperation:
        """Hyperbolic tangent."""
        return MathFunctions.tanh(column)

    @staticmethod
    def degrees(column: Union[Column, str]) -> ColumnOperation:
        """Convert radians to degrees."""
        return MathFunctions.degrees(column)

    @staticmethod
    def radians(column: Union[Column, str]) -> ColumnOperation:
        """Convert degrees to radians."""
        return MathFunctions.radians(column)

    @staticmethod
    def cbrt(column: Union[Column, str]) -> ColumnOperation:
        """Cube root."""
        return MathFunctions.cbrt(column)

    @staticmethod
    def factorial(column: Union[Column, str]) -> ColumnOperation:
        """Factorial of non-negative integer."""
        return MathFunctions.factorial(column)

    @staticmethod
    def rand(seed: Optional[int] = None) -> ColumnOperation:
        """Generate random column with uniform distribution [0.0, 1.0]."""
        return MathFunctions.rand(seed)

    @staticmethod
    def randn(seed: Optional[int] = None) -> ColumnOperation:
        """Generate random column with standard normal distribution."""
        return MathFunctions.randn(seed)

    @staticmethod
    def rint(column: Union[Column, str]) -> ColumnOperation:
        """Round to nearest integer using banker's rounding."""
        return MathFunctions.rint(column)

    @staticmethod
    def bround(column: Union[Column, str], scale: int = 0) -> ColumnOperation:
        """Round using HALF_EVEN rounding mode."""
        return MathFunctions.bround(column, scale)

    @staticmethod
    def sign(column: Union[Column, str]) -> ColumnOperation:
        """Sign of number (matches PySpark signum)."""
        return MathFunctions.sign(column)

    @staticmethod
    def hypot(col1: Union[Column, str], col2: Union[Column, str]) -> ColumnOperation:
        """Compute hypotenuse."""
        return MathFunctions.hypot(col1, col2)

    @staticmethod
    def nanvl(col1: Union[Column, str], col2: Union[Column, str]) -> ColumnOperation:
        """Return col1 if not NaN, else col2."""
        return MathFunctions.nanvl(col1, col2)

    @staticmethod
    def signum(column: Union[Column, str]) -> ColumnOperation:
        """Compute signum (sign)."""
        return MathFunctions.signum(column)

    @staticmethod
    def width_bucket(
        value: Union[Column, str],
        min_value: Union[Column, str, float],
        max_value: Union[Column, str, float],
        num_buckets: Union[Column, str, int],
    ) -> ColumnOperation:
        """Compute histogram bucket number for value (PySpark 3.5+)."""
        return MathFunctions.width_bucket(value, min_value, max_value, num_buckets)

    @staticmethod
    def cot(column: Union[Column, str]) -> ColumnOperation:
        """Compute cotangent (PySpark 3.3+)."""
        return MathFunctions.cot(column)

    @staticmethod
    def csc(column: Union[Column, str]) -> ColumnOperation:
        """Compute cosecant (PySpark 3.3+)."""
        return MathFunctions.csc(column)

    @staticmethod
    def sec(column: Union[Column, str]) -> ColumnOperation:
        """Compute secant (PySpark 3.3+)."""
        return MathFunctions.sec(column)

    @staticmethod
    def e() -> ColumnOperation:
        """Euler's number e (PySpark 3.5+)."""
        return MathFunctions.e()

    @staticmethod
    def pi() -> ColumnOperation:
        """Pi constant (PySpark 3.5+)."""
        return MathFunctions.pi()

    @staticmethod
    def ln(column: Union[Column, str]) -> ColumnOperation:
        """Natural logarithm (PySpark 3.5+)."""
        return MathFunctions.ln(column)

    @staticmethod
    def greatest(*columns: Union[Column, str]) -> ColumnOperation:
        """Greatest value among columns."""
        return MathFunctions.greatest(*columns)

    @staticmethod
    def least(*columns: Union[Column, str]) -> ColumnOperation:
        """Least value among columns."""
        return MathFunctions.least(*columns)

    # Aggregate functions
    @staticmethod
    def count(column: Union[Column, str, None] = None) -> ColumnOperation:
        """Count values."""
        return AggregateFunctions.count(column)

    @staticmethod
    def sum(column: Union[Column, str]) -> ColumnOperation:
        """Sum values."""
        return AggregateFunctions.sum(column)

    @staticmethod
    def avg(column: Union[Column, str]) -> ColumnOperation:
        """Average values."""
        return AggregateFunctions.avg(column)

    @staticmethod
    def max(column: Union[Column, str]) -> ColumnOperation:
        """Maximum value."""
        return AggregateFunctions.max(column)

    @staticmethod
    def min(column: Union[Column, str]) -> ColumnOperation:
        """Minimum value."""
        return AggregateFunctions.min(column)

    @staticmethod
    def first(
        column: Union[Column, str], ignorenulls: bool = False
    ) -> AggregateFunction:
        """First value."""
        return AggregateFunctions.first(column, ignorenulls=ignorenulls)

    @staticmethod
    def last(column: Union[Column, str]) -> AggregateFunction:
        """Last value."""
        return AggregateFunctions.last(column)

    @staticmethod
    def collect_list(column: Union[Column, str]) -> AggregateFunction:
        """Collect values into list."""
        return AggregateFunctions.collect_list(column)

    @staticmethod
    def collect_set(column: Union[Column, str]) -> AggregateFunction:
        """Collect unique values into set."""
        return AggregateFunctions.collect_set(column)

    @staticmethod
    def stddev(column: Union[Column, str]) -> "ColumnOperation":  # noqa: F821
        """Standard deviation."""

        return AggregateFunctions.stddev(column)

    @staticmethod
    def std(column: Union[Column, str]) -> "ColumnOperation":  # noqa: F821
        """Alias for stddev - Standard deviation."""

        return AggregateFunctions.std(column)

    @staticmethod
    def product(column: Union[Column, str]) -> AggregateFunction:
        """Multiply all values in column."""
        return AggregateFunctions.product(column)

    @staticmethod
    def sum_distinct(column: Union[Column, str]) -> AggregateFunction:
        """Sum of distinct values."""
        return AggregateFunctions.sum_distinct(column)

    @staticmethod
    def variance(column: Union[Column, str]) -> "ColumnOperation":  # noqa: F821
        """Variance."""

        return AggregateFunctions.variance(column)

    @staticmethod
    def skewness(column: Union[Column, str]) -> AggregateFunction:
        """Skewness."""
        return AggregateFunctions.skewness(column)

    @staticmethod
    def kurtosis(column: Union[Column, str]) -> AggregateFunction:
        """Kurtosis."""
        return AggregateFunctions.kurtosis(column)

    @staticmethod
    def countDistinct(column: Union[Column, str]) -> AggregateFunction:
        """Count distinct values."""
        return AggregateFunctions.countDistinct(column)

    @staticmethod
    def count_distinct(column: Union[Column, str]) -> AggregateFunction:
        """Alias for countDistinct - Count distinct values."""
        return AggregateFunctions.count_distinct(column)

    @staticmethod
    def percentile_approx(
        column: Union[Column, str], percentage: float, accuracy: int = 10000
    ) -> AggregateFunction:
        """Approximate percentile."""
        return AggregateFunctions.percentile_approx(column, percentage, accuracy)

    @staticmethod
    def corr(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Correlation between two columns."""
        return AggregateFunctions.corr(column1, column2)

    @staticmethod
    def covar_samp(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Sample covariance between two columns."""
        return AggregateFunctions.covar_samp(column1, column2)

    @staticmethod
    def mean(column: Union[Column, str]) -> AggregateFunction:
        """Mean of values (alias for avg)."""
        return AggregateFunctions.mean(column)

    @staticmethod
    def approx_count_distinct(
        column: Union[Column, str], rsd: Optional[float] = None
    ) -> ColumnOperation:
        """Approximate count of distinct elements.

        Args:
            column: Column to count distinct values.
            rsd: Optional relative standard deviation (default: None, which uses PySpark's default of 0.05).
                 Controls the approximation accuracy. Lower values provide better accuracy but use more memory.
        """
        return AggregateFunctions.approx_count_distinct(column, rsd=rsd)

    @staticmethod
    def stddev_pop(column: Union[Column, str]) -> AggregateFunction:
        """Population standard deviation."""
        return AggregateFunctions.stddev_pop(column)

    @staticmethod
    def stddev_samp(column: Union[Column, str]) -> AggregateFunction:
        """Sample standard deviation."""
        return AggregateFunctions.stddev_samp(column)

    @staticmethod
    def var_pop(column: Union[Column, str]) -> AggregateFunction:
        """Population variance."""
        return AggregateFunctions.var_pop(column)

    @staticmethod
    def var_samp(column: Union[Column, str]) -> AggregateFunction:
        """Sample variance."""
        return AggregateFunctions.var_samp(column)

    @staticmethod
    def covar_pop(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> AggregateFunction:
        """Population covariance."""
        return AggregateFunctions.covar_pop(column1, column2)

    @staticmethod
    def median(column: Union[Column, str]) -> AggregateFunction:
        """Median value (PySpark 3.4+)."""
        return AggregateFunctions.median(column)

    @staticmethod
    def mode(column: Union[Column, str]) -> AggregateFunction:
        """Most frequent value (PySpark 3.4+)."""
        return AggregateFunctions.mode(column)

    @staticmethod
    def percentile(column: Union[Column, str], percentage: float) -> AggregateFunction:
        """Exact percentile (PySpark 3.5+)."""
        return AggregateFunctions.percentile(column, percentage)

    @staticmethod
    def approx_percentile(
        column: Union[Column, str],
        percentage: Union[float, Column, str],
        accuracy: Union[int, Column, str] = 10000,
    ) -> AggregateFunction:
        """Approximate percentile (PySpark 3.5+)."""
        return AggregateFunctions.approx_percentile(column, percentage, accuracy)

    @staticmethod
    def bool_and(column: Union[Column, str]) -> AggregateFunction:
        """Aggregate AND (PySpark 3.1+)."""
        return AggregateFunctions.bool_and(column)

    @staticmethod
    def bool_or(column: Union[Column, str]) -> AggregateFunction:
        """Aggregate OR (PySpark 3.1+)."""
        return AggregateFunctions.bool_or(column)

    @staticmethod
    def every(column: Union[Column, str]) -> AggregateFunction:
        """Alias for bool_and (PySpark 3.1+)."""
        return AggregateFunctions.every(column)

    @staticmethod
    def some(column: Union[Column, str]) -> AggregateFunction:
        """Alias for bool_or (PySpark 3.1+)."""
        return AggregateFunctions.some(column)

    @staticmethod
    def max_by(
        column: Union[Column, str], ord: Union[Column, str]
    ) -> AggregateFunction:
        """Value with max of ord column (PySpark 3.1+)."""
        return AggregateFunctions.max_by(column, ord)

    @staticmethod
    def min_by(
        column: Union[Column, str], ord: Union[Column, str]
    ) -> AggregateFunction:
        """Value with min of ord column (PySpark 3.1+)."""
        return AggregateFunctions.min_by(column, ord)

    @staticmethod
    def count_if(column: Union[Column, str]) -> AggregateFunction:
        """Count where condition is true (PySpark 3.1+)."""
        return AggregateFunctions.count_if(column)

    @staticmethod
    def any_value(column: Union[Column, str]) -> AggregateFunction:
        """Return any non-null value (PySpark 3.1+)."""
        return AggregateFunctions.any_value(column)

    # Datetime functions
    @staticmethod
    def current_timestamp() -> ColumnOperation:
        """Current timestamp.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("current_timestamp function")
        return DateTimeFunctions.current_timestamp()

    @staticmethod
    def current_date() -> ColumnOperation:
        """Current date.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("current_date function")
        return DateTimeFunctions.current_date()

    @staticmethod
    def version() -> Literal:
        """Return Spark version string (PySpark 3.0+).

        Returns:
            Literal with sparkless version
        """
        from sparkless import __version__

        # Return sparkless version as a constant expression
        return Literal(f"sparkless-{__version__}")

    @staticmethod
    def to_date(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert to date."""
        return DateTimeFunctions.to_date(column, format)

    @staticmethod
    def to_timestamp(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert to timestamp."""
        return DateTimeFunctions.to_timestamp(column, format)

    @staticmethod
    def date_from_unix_date(days: Union[Column, str, int]) -> ColumnOperation:
        """Convert unix date (days since epoch) to date (PySpark 3.5+)."""
        return DateTimeFunctions.date_from_unix_date(days)

    @staticmethod
    def to_timestamp_ltz(
        timestamp_str: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert string to timestamp with local timezone (PySpark 3.5+)."""
        return DateTimeFunctions.to_timestamp_ltz(timestamp_str, format)

    @staticmethod
    def to_timestamp_ntz(
        timestamp_str: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert string to timestamp with no timezone (PySpark 3.5+)."""
        return DateTimeFunctions.to_timestamp_ntz(timestamp_str, format)

    @staticmethod
    def hour(column: Union[Column, str]) -> ColumnOperation:
        """Extract hour."""
        return DateTimeFunctions.hour(column)

    @staticmethod
    def day(column: Union[Column, str]) -> ColumnOperation:
        """Extract day."""
        return DateTimeFunctions.day(column)

    @staticmethod
    def dayofmonth(column: Union[Column, str]) -> ColumnOperation:
        """Extract day of month (alias for day)."""
        return DateTimeFunctions.dayofmonth(column)

    @staticmethod
    def month(column: Union[Column, str]) -> ColumnOperation:
        """Extract month."""
        return DateTimeFunctions.month(column)

    @staticmethod
    def year(column: Union[Column, str]) -> ColumnOperation:
        """Extract year."""
        return DateTimeFunctions.year(column)

    # Conditional functions
    @staticmethod
    def coalesce(*columns: Union[Column, str, Any]) -> ColumnOperation:
        """Return first non-null value."""
        return ConditionalFunctions.coalesce(*columns)

    @staticmethod
    def isnull(column: Union[Column, str]) -> ColumnOperation:
        """Check if column is null."""
        return ConditionalFunctions.isnull(column)

    @staticmethod
    def isnotnull(column: Union[Column, str]) -> ColumnOperation:
        """Check if column is not null."""
        return ConditionalFunctions.isnotnull(column)

    @staticmethod
    def isnan(column: Union[Column, str]) -> ColumnOperation:
        """Check if column is NaN."""
        return ConditionalFunctions.isnan(column)

    @staticmethod
    def when(condition: Any, value: Any = None) -> CaseWhen:
        """Start CASE WHEN expression.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("CASE WHEN expression")
        if value is not None:
            return ConditionalFunctions.when(condition, value)
        return ConditionalFunctions.when(condition)

    @staticmethod
    def case_when(*conditions: Tuple[Any, Any], else_value: Any = None) -> CaseWhen:
        """Create CASE WHEN expression with multiple conditions."""
        return ConditionalFunctions.case_when(*conditions, else_value=else_value)

    @staticmethod
    def dayofweek(column: Union[Column, str]) -> ColumnOperation:
        """Extract day of week."""
        return DateTimeFunctions.dayofweek(column)

    @staticmethod
    def dayofyear(column: Union[Column, str]) -> ColumnOperation:
        """Extract day of year."""
        return DateTimeFunctions.dayofyear(column)

    @staticmethod
    def weekofyear(column: Union[Column, str]) -> ColumnOperation:
        """Extract week of year."""
        return DateTimeFunctions.weekofyear(column)

    @staticmethod
    def quarter(column: Union[Column, str]) -> ColumnOperation:
        """Extract quarter."""
        return DateTimeFunctions.quarter(column)

    @staticmethod
    def now() -> ColumnOperation:
        """Alias for current_timestamp - Get current timestamp."""
        return DateTimeFunctions.now()

    @staticmethod
    def curdate() -> ColumnOperation:
        """Alias for current_date - Get current date."""
        return DateTimeFunctions.curdate()

    @staticmethod
    def days(column: Union[Column, str, int]) -> ColumnOperation:
        """Convert number to days interval."""
        return DateTimeFunctions.days(column)

    @staticmethod
    def hours(column: Union[Column, str, int]) -> ColumnOperation:
        """Convert number to hours interval."""
        return DateTimeFunctions.hours(column)

    @staticmethod
    def months(column: Union[Column, str, int]) -> ColumnOperation:
        """Convert number to months interval."""
        return DateTimeFunctions.months(column)

    # SQL expression function
    @staticmethod
    def expr(expression: str) -> Union[ColumnOperation, Column, "CaseWhen", "Literal"]:
        """Parse SQL expression into a column.

        Args:
            expression: SQL expression string (e.g., "id IS NOT NULL", "age > 18").
                  Must use SQL syntax, not Python expressions.

        Returns:
            ColumnOperation for the expression.

        Raises:
            RuntimeError: If no active SparkSession is available
            ParseException: If SQL syntax is invalid
        """
        Functions._require_active_session(f"expression '{expression}'")

        # Parse SQL expression instead of storing as raw string
        from ..functions.core.sql_expr_parser import SQLExprParser
        from ..functions.core.column import ColumnOperation, Column
        from ..core.exceptions.analysis import ParseException

        try:
            parsed = SQLExprParser.parse(expression)
            # If parsed result is a Column or ColumnOperation, return it
            if isinstance(parsed, ColumnOperation):
                # Mark as coming from F.expr() for detection in materialization
                setattr(parsed, "_from_expr", True)
                return parsed
            elif isinstance(parsed, Column):
                # Simple column reference - return as ColumnOperation for consistency
                # Use a dummy operation to wrap the column
                result = ColumnOperation(parsed, "expr", expression)
                setattr(result, "_from_expr", True)
                return result
            # Check for CaseWhen or Literal
            from ..functions.core.literals import Literal

            if isinstance(parsed, Literal):
                # Literal value - wrap in ColumnOperation
                result = ColumnOperation(None, "lit", parsed)
                setattr(result, "_from_expr", True)
                return result
            # For CaseWhen or other types, check if it has evaluate method
            if hasattr(parsed, "evaluate") and hasattr(parsed, "conditions"):
                # CaseWhen object - return directly (it has evaluate() method)
                return parsed
            # Fallback: wrap unknown types in ColumnOperation
            result = ColumnOperation(None, "lit", Literal(parsed))
            setattr(result, "_from_expr", True)
            return result
        except Exception as e:
            if isinstance(e, ParseException):
                raise
            # Fallback to old behavior if parsing fails (for backward compatibility)
            # But warn that this might not work correctly
            import warnings

            warnings.warn(
                f"Failed to parse SQL expression '{expression}'. "
                f"F.expr() should use SQL syntax (e.g., 'id IS NOT NULL'), "
                f"not Python expressions (e.g., \"col('id').isNotNull()\"). "
                f"Error: {str(e)}",
                UserWarning,
                stacklevel=2,
            )
            from sparkless.functions.base import Column

            dummy = Column("__expr__")
            operation = ColumnOperation(dummy, "expr", expression, name=expression)
            operation.function_name = "expr"
            return operation

    @staticmethod
    def minute(column: Union[Column, str]) -> ColumnOperation:
        """Extract minute."""
        return DateTimeFunctions.minute(column)

    @staticmethod
    def second(column: Union[Column, str]) -> ColumnOperation:
        """Extract second."""
        return DateTimeFunctions.second(column)

    @staticmethod
    def add_months(column: Union[Column, str], num_months: int) -> ColumnOperation:
        """Add months to date."""
        return DateTimeFunctions.add_months(column, num_months)

    @staticmethod
    def months_between(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Calculate months between two dates."""
        return DateTimeFunctions.months_between(column1, column2)

    @staticmethod
    def date_add(column: Union[Column, str], days: int) -> ColumnOperation:
        """Add days to date."""
        return DateTimeFunctions.date_add(column, days)

    @staticmethod
    def date_sub(column: Union[Column, str], days: int) -> ColumnOperation:
        """Subtract days from date."""
        return DateTimeFunctions.date_sub(column, days)

    @staticmethod
    def date_format(column: Union[Column, str], format: str) -> ColumnOperation:
        """Format date/timestamp as string."""
        return DateTimeFunctions.date_format(column, format)

    @staticmethod
    def make_date(
        year: Union[Column, int],
        month: Union[Column, int],
        day: Union[Column, int],
    ) -> ColumnOperation:
        """Construct date from year, month, day (PySpark 3.0+)."""
        return DateTimeFunctions.make_date(year, month, day)

    @staticmethod
    def date_trunc(format: str, timestamp: Union[Column, str]) -> ColumnOperation:
        """Truncate timestamp to specified unit."""
        return DateTimeFunctions.date_trunc(format, timestamp)

    @staticmethod
    def datediff(end: Union[Column, str], start: Union[Column, str]) -> ColumnOperation:
        """Number of days between two dates."""
        return DateTimeFunctions.datediff(end, start)

    @staticmethod
    def date_diff(
        end: Union[Column, str], start: Union[Column, str]
    ) -> ColumnOperation:
        """Alias for datediff - Returns number of days between two dates."""
        return DateTimeFunctions.date_diff(end, start)

    @staticmethod
    def unix_timestamp(
        timestamp: Optional[Union[Column, str]] = None,
        format: str = "yyyy-MM-dd HH:mm:ss",
    ) -> ColumnOperation:
        """Convert timestamp to Unix timestamp."""
        return DateTimeFunctions.unix_timestamp(timestamp, format)

    @staticmethod
    def last_day(date: Union[Column, str]) -> ColumnOperation:
        """Last day of the month for given date."""
        return DateTimeFunctions.last_day(date)

    @staticmethod
    def next_day(date: Union[Column, str], dayOfWeek: str) -> ColumnOperation:
        """First date later than date on specified day of week."""
        return DateTimeFunctions.next_day(date, dayOfWeek)

    @staticmethod
    def trunc(date: Union[Column, str], format: str) -> ColumnOperation:
        """Truncate date to specified unit."""
        return DateTimeFunctions.trunc(date, format)

    @staticmethod
    def timestamp_seconds(col: Union[Column, str, int]) -> ColumnOperation:
        """Convert seconds since epoch to timestamp (PySpark 3.1+)."""
        return DateTimeFunctions.timestamp_seconds(col)

    @staticmethod
    def weekday(col: Union[Column, str]) -> ColumnOperation:
        """Day of week as integer (0=Monday, 6=Sunday) (PySpark 3.5+)."""
        return DateTimeFunctions.weekday(col)

    @staticmethod
    def extract(field: str, source: Union[Column, str]) -> ColumnOperation:
        """Extract field from date/timestamp (PySpark 3.5+)."""
        return DateTimeFunctions.extract(field, source)

    @staticmethod
    def raise_error(msg: Union[Column, str]) -> ColumnOperation:
        """Raise an error with the specified message (PySpark 3.1+).

        Args:
            msg: Error message

        Returns:
            ColumnOperation representing the raise_error function
        """
        if isinstance(msg, str):
            from sparkless.functions.core.literals import Literal

            msg = Literal(msg)  # type: ignore[assignment]

        return ColumnOperation(
            msg,
            "raise_error",
            name=f"raise_error({msg})",
        )

    @staticmethod
    def from_unixtime(
        column: Union[Column, str], format: str = "yyyy-MM-dd HH:mm:ss"
    ) -> ColumnOperation:
        """Convert unix timestamp to string."""
        return DateTimeFunctions.from_unixtime(column, format)

    @staticmethod
    def timestampadd(
        unit: str, quantity: Union[int, Column], timestamp: Union[str, Column]
    ) -> ColumnOperation:
        """Add time units to a timestamp."""
        return DateTimeFunctions.timestampadd(unit, quantity, timestamp)

    @staticmethod
    def timestampdiff(
        unit: str, start: Union[str, Column], end: Union[str, Column]
    ) -> ColumnOperation:
        """Calculate difference between two timestamps."""
        return DateTimeFunctions.timestampdiff(unit, start, end)

    @staticmethod
    def nvl(column: Union[Column, str], default_value: Any) -> ColumnOperation:
        """Return default if null. PySpark uses coalesce internally."""
        # Use coalesce for SQL generation compatibility
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.coalesce(column, default_value)

    @staticmethod
    def nvl2(
        column: Union[Column, str], value_if_not_null: Any, value_if_null: Any
    ) -> Any:
        """Return value based on null check. PySpark uses when/otherwise internally."""
        # Use when/otherwise for SQL generation compatibility
        from .conditional import ConditionalFunctions
        from sparkless.functions.base import Column

        # Convert string to Column if needed
        col = Column(column) if isinstance(column, str) else column

        # nvl2 should check if column IS NULL, not if column is truthy
        return ConditionalFunctions.when(col.isNull(), value_if_null).otherwise(
            value_if_not_null
        )

    @staticmethod
    def equal_null(
        col1: Union[Column, str], col2: Union[Column, str, Any]
    ) -> ColumnOperation:
        """Equality check that treats NULL as equal."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.equal_null(col1, col2)

    # Window functions
    @staticmethod
    def row_number() -> ColumnOperation:
        """Row number window function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("row_number window function")
        # Create a special column for functions without input
        from sparkless.functions.base import Column
        from sparkless.spark_types import IntegerType

        dummy_column = Column("__row_number__")
        operation = ColumnOperation(dummy_column, "row_number")
        operation.name = "row_number()"
        operation.function_name = "row_number"
        operation.return_type = IntegerType(nullable=False)
        return operation

    @staticmethod
    def rank() -> ColumnOperation:
        """Rank window function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("rank window function")
        # Create a special column for functions without input
        from sparkless.functions.base import Column

        dummy_column = Column("__rank__")
        operation = ColumnOperation(dummy_column, "rank")
        operation.name = "rank()"
        operation.function_name = "rank"
        return operation

    @staticmethod
    def dense_rank() -> ColumnOperation:
        """Dense rank window function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("dense_rank window function")
        # Create a special column for functions without input
        from sparkless.functions.base import Column

        dummy_column = Column("__dense_rank__")
        operation = ColumnOperation(dummy_column, "dense_rank")
        operation.name = "dense_rank()"
        operation.function_name = "dense_rank"
        return operation

    @staticmethod
    def lag(
        column: Union[Column, str], offset: int = 1, default: Any = None
    ) -> ColumnOperation:
        """Lag window function.

        Args:
            column: The column to lag.
            offset: Number of rows to look back. Default is 1.
            default: Default value if offset goes beyond partition. Default is None.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("lag window function")
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "lag", (offset, default))
        operation.name = f"lag({column.name}, {offset})"
        operation.function_name = "lag"
        return operation

    @staticmethod
    def lead(
        column: Union[Column, str], offset: int = 1, default: Any = None
    ) -> ColumnOperation:
        """Lead window function.

        Args:
            column: The column to lead.
            offset: Number of rows to look forward. Default is 1.
            default: Default value if offset goes beyond partition. Default is None.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("lead window function")
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "lead", (offset, default))
        operation.name = f"lead({column.name}, {offset})"
        operation.function_name = "lead"
        return operation

    @staticmethod
    def nth_value(column: Union[Column, str], n: int) -> ColumnOperation:
        """Nth value window function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("nth_value window function")
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "nth_value", n)
        operation.name = f"nth_value({column.name}, {n})"
        operation.function_name = "nth_value"
        return operation

    @staticmethod
    def ntile(n: int) -> ColumnOperation:
        """NTILE window function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("ntile window function")
        from sparkless.functions.base import Column

        dummy_column = Column("__ntile__")
        operation = ColumnOperation(dummy_column, "ntile", n)
        operation.name = f"ntile({n})"
        operation.function_name = "ntile"
        return operation

    @staticmethod
    def cume_dist() -> ColumnOperation:
        """Cumulative distribution window function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("cume_dist window function")
        from sparkless.functions.base import Column

        dummy_column = Column("__cume_dist__")
        operation = ColumnOperation(dummy_column, "cume_dist")
        operation.name = "cume_dist()"
        operation.function_name = "cume_dist"
        return operation

    @staticmethod
    def percent_rank() -> ColumnOperation:
        """Percent rank window function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("percent_rank window function")
        from sparkless.functions.base import Column

        dummy_column = Column("__percent_rank__")
        operation = ColumnOperation(dummy_column, "percent_rank")
        operation.name = "percent_rank()"
        operation.function_name = "percent_rank"
        return operation

    @staticmethod
    def first_value(column: Union[Column, str]) -> ColumnOperation:
        """First value window function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("first_value window function")
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "first_value")
        operation.name = f"first_value({column.name})"
        operation.function_name = "first_value"
        return operation

    @staticmethod
    def last_value(column: Union[Column, str]) -> ColumnOperation:
        """Last value window function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("last_value window function")
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "last_value")
        operation.name = f"last_value({column.name})"
        operation.function_name = "last_value"
        return operation

    @staticmethod
    def desc(column: Union[Column, str]) -> ColumnOperation:
        """Create descending order column."""
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "desc", None, name=f"{column.name} DESC")
        operation.function_name = "desc"
        return operation

    # Array functions
    @staticmethod
    def array(*cols: Union[Column, str]) -> ColumnOperation:
        """Create array from columns (PySpark 3.0+)."""
        return ArrayFunctions.array(*cols)

    @staticmethod
    def array_repeat(col: Union[Column, str], count: int) -> ColumnOperation:
        """Repeat value to create array (PySpark 3.0+)."""
        return ArrayFunctions.array_repeat(col, count)

    @staticmethod
    def sort_array(col: Union[Column, str], asc: bool = True) -> ColumnOperation:
        """Sort array elements (PySpark 3.0+)."""
        return ArrayFunctions.sort_array(col, asc)

    @staticmethod
    def array_agg(col: Union[Column, str]) -> AggregateFunction:
        """Aggregate values into array (PySpark 3.5+)."""
        return ArrayFunctions.array_agg(col)

    @staticmethod
    def cardinality(col: Union[Column, str]) -> ColumnOperation:
        """Return size of array or map (PySpark 3.5+)."""
        return ArrayFunctions.cardinality(col)

    @staticmethod
    def array_distinct(column: Union[Column, str]) -> ColumnOperation:
        """Remove duplicate elements from array."""
        return ArrayFunctions.array_distinct(column)

    @staticmethod
    def array_intersect(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Intersection of two arrays."""
        return ArrayFunctions.array_intersect(column1, column2)

    @staticmethod
    def array_union(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Union of two arrays."""
        return ArrayFunctions.array_union(column1, column2)

    @staticmethod
    def array_except(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Elements in first array but not second."""
        return ArrayFunctions.array_except(column1, column2)

    @staticmethod
    def array_position(column: Union[Column, str], value: Any) -> ColumnOperation:
        """Position of element in array."""
        return ArrayFunctions.array_position(column, value)

    @staticmethod
    def array_remove(column: Union[Column, str], value: Any) -> ColumnOperation:
        """Remove all occurrences of element from array."""
        return ArrayFunctions.array_remove(column, value)

    # Higher-order array functions (PySpark 3.2+)
    @staticmethod
    def transform(
        column: Union[Column, str], function: Callable[[Any], Any]
    ) -> ColumnOperation:
        """Apply function to each array element."""
        return ArrayFunctions.transform(column, function)

    @staticmethod
    def filter(
        column: Union[Column, str], function: Callable[[Any], bool]
    ) -> ColumnOperation:
        """Filter array elements with predicate."""
        return ArrayFunctions.filter(column, function)

    @staticmethod
    def exists(
        column: Union[Column, str], function: Callable[[Any], bool]
    ) -> ColumnOperation:
        """Check if any element satisfies predicate."""
        return ArrayFunctions.exists(column, function)

    @staticmethod
    def forall(
        column: Union[Column, str], function: Callable[[Any], bool]
    ) -> ColumnOperation:
        """Check if all elements satisfy predicate."""
        return ArrayFunctions.forall(column, function)

    @staticmethod
    def aggregate(
        column: Union[Column, str],
        initial_value: Any,
        merge: Callable[[Any, Any], Any],
        finish: Optional[Callable[[Any], Any]] = None,
    ) -> ColumnOperation:
        """Aggregate array elements to single value."""
        return ArrayFunctions.aggregate(column, initial_value, merge, finish)

    @staticmethod
    def zip_with(
        left: Union[Column, str],
        right: Union[Column, str],
        function: Callable[[Any, Any], Any],
    ) -> ColumnOperation:
        """Merge two arrays element-wise."""
        return ArrayFunctions.zip_with(left, right, function)

    # Basic array functions (PySpark 3.2+)
    @staticmethod
    def array_compact(column: Union[Column, str]) -> ColumnOperation:
        """Remove null values from array."""
        return ArrayFunctions.array_compact(column)

    @staticmethod
    def slice(column: Union[Column, str], start: int, length: int) -> ColumnOperation:
        """Extract array slice."""
        return ArrayFunctions.slice(column, start, length)

    @staticmethod
    def element_at(column: Union[Column, str], index: int) -> ColumnOperation:
        """Get element at index."""
        return ArrayFunctions.element_at(column, index)

    @staticmethod
    def array_append(column: Union[Column, str], element: Any) -> ColumnOperation:
        """Append element to array."""
        return ArrayFunctions.array_append(column, element)

    @staticmethod
    def array_prepend(column: Union[Column, str], element: Any) -> ColumnOperation:
        """Prepend element to array."""
        return ArrayFunctions.array_prepend(column, element)

    @staticmethod
    def array_insert(
        column: Union[Column, str], pos: int, value: Any
    ) -> ColumnOperation:
        """Insert element at position."""
        return ArrayFunctions.array_insert(column, pos, value)

    @staticmethod
    def array_size(column: Union[Column, str]) -> ColumnOperation:
        """Get array length."""
        return ArrayFunctions.array_size(column)

    @staticmethod
    def array_sort(column: Union[Column, str]) -> ColumnOperation:
        """Sort array elements."""
        return ArrayFunctions.array_sort(column)

    @staticmethod
    def arrays_overlap(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Check if arrays have common elements."""
        return ArrayFunctions.arrays_overlap(column1, column2)

    @staticmethod
    def array_contains(column: Union[Column, str], value: Any) -> ColumnOperation:
        """Check if array contains value."""
        return ArrayFunctions.array_contains(column, value)

    @staticmethod
    def array_max(column: Union[Column, str]) -> ColumnOperation:
        """Return maximum value from array."""
        return ArrayFunctions.array_max(column)

    @staticmethod
    def array_min(column: Union[Column, str]) -> ColumnOperation:
        """Return minimum value from array."""
        return ArrayFunctions.array_min(column)

    @staticmethod
    def explode(column: Union[Column, str]) -> ColumnOperation:
        """Returns a new row for each element in array or map."""
        return ArrayFunctions.explode(column)

    @staticmethod
    def size(column: Union[Column, str]) -> ColumnOperation:
        """Return size of array or map."""
        return ArrayFunctions.size(column)

    @staticmethod
    def flatten(column: Union[Column, str]) -> ColumnOperation:
        """Flatten array of arrays into single array."""
        return ArrayFunctions.flatten(column)

    @staticmethod
    def reverse(column: Union[Column, str]) -> ColumnOperation:
        """Reverse string or array elements. Defaults to string reverse."""
        # Default to string reverse (more common use case)
        # If array reverse is needed, use ArrayFunctions.reverse() directly
        return StringFunctions.reverse(column)

    @staticmethod
    def explode_outer(column: Union[Column, str]) -> ColumnOperation:
        """Explode array including null/empty arrays."""
        return ArrayFunctions.explode_outer(column)

    @staticmethod
    def posexplode(column: Union[Column, str]) -> ColumnOperation:
        """Explode array with position."""
        return ArrayFunctions.posexplode(column)

    @staticmethod
    def posexplode_outer(column: Union[Column, str]) -> ColumnOperation:
        """Explode array with position including null/empty."""
        return ArrayFunctions.posexplode_outer(column)

    @staticmethod
    def arrays_zip(*columns: Union[Column, str]) -> ColumnOperation:
        """Merge arrays into array of structs."""
        return ArrayFunctions.arrays_zip(*columns)

    @staticmethod
    def sequence(
        start: Union[Column, str, int],
        stop: Union[Column, str, int],
        step: Union[Column, str, int] = 1,
    ) -> ColumnOperation:
        """Generate array sequence from start to stop."""
        return ArrayFunctions.sequence(start, stop, step)

    @staticmethod
    def shuffle(column: Union[Column, str]) -> ColumnOperation:
        """Randomly shuffle array elements."""
        return ArrayFunctions.shuffle(column)

    # Map functions
    @staticmethod
    def map_keys(column: Union[Column, str]) -> ColumnOperation:
        """Get all keys from map."""
        return MapFunctions.map_keys(column)

    @staticmethod
    def map_values(column: Union[Column, str]) -> ColumnOperation:
        """Get all values from map."""
        return MapFunctions.map_values(column)

    @staticmethod
    def map_entries(column: Union[Column, str]) -> ColumnOperation:
        """Get key-value pairs as array of structs."""
        return MapFunctions.map_entries(column)

    @staticmethod
    def map_concat(*columns: Union[Column, str]) -> ColumnOperation:
        """Concatenate multiple maps."""
        return MapFunctions.map_concat(*columns)

    @staticmethod
    def map_from_arrays(
        keys: Union[Column, str], values: Union[Column, str]
    ) -> ColumnOperation:
        """Create map from key and value arrays."""
        return MapFunctions.map_from_arrays(keys, values)

    # Advanced map functions (PySpark 3.2+)
    @staticmethod
    def create_map(*cols: Union[Column, str, Any]) -> ColumnOperation:
        """Create map from key-value pairs."""
        return MapFunctions.create_map(*cols)

    @staticmethod
    def map_contains_key(column: Union[Column, str], key: Any) -> ColumnOperation:
        """Check if map contains key."""
        return MapFunctions.map_contains_key(column, key)

    @staticmethod
    def map_from_entries(column: Union[Column, str]) -> ColumnOperation:
        """Convert array of structs to map."""
        return MapFunctions.map_from_entries(column)

    @staticmethod
    def map_filter(
        column: Union[Column, str], function: Callable[[Any, Any], bool]
    ) -> ColumnOperation:
        """Filter map entries with predicate."""
        return MapFunctions.map_filter(column, function)

    @staticmethod
    def transform_keys(
        column: Union[Column, str], function: Callable[[Any, Any], Any]
    ) -> ColumnOperation:
        """Transform map keys with function."""
        return MapFunctions.transform_keys(column, function)

    @staticmethod
    def transform_values(
        column: Union[Column, str], function: Callable[[Any, Any], Any]
    ) -> ColumnOperation:
        """Transform map values with function."""
        return MapFunctions.transform_values(column, function)

    @staticmethod
    def map_zip_with(
        col1: Union[Column, str],
        col2: Union[Column, str],
        function: Callable[[Any, Any, Any], Any],
    ) -> ColumnOperation:
        """Merge two maps using function (PySpark 3.1+)."""
        return MapFunctions.map_zip_with(col1, col2, function)

    # Struct functions (PySpark 3.2+)
    @staticmethod
    def struct(*cols: Union[Column, str]) -> ColumnOperation:
        """Create a struct column from given columns."""
        if not cols:
            raise ValueError("struct requires at least one column")

        # Check if first column is a Literal - if so, store all columns in value

        # Generate name with actual column/literal values
        from sparkless.core.type_utils import get_expression_name, is_literal

        col_names = [get_expression_name(col) for col in cols]
        struct_name = f"struct({', '.join(col_names)})"

        # Check if first column is a Literal (runtime check, not in type annotation)
        if is_literal(cols[0]):
            # Use a dummy column and store all columns in value
            base_col = Column("__struct_dummy__")
            return ColumnOperation(
                base_col,
                "struct",
                value=cols,  # Store all columns including the first one
                name=struct_name,
            )
        else:
            # Use first column as base
            base_col = cols[0] if isinstance(cols[0], Column) else Column(str(cols[0]))
            return ColumnOperation(
                base_col,
                "struct",
                value=cols[1:] if len(cols) > 1 else None,
                name=struct_name,
            )

    @staticmethod
    def named_struct(*cols: Any) -> ColumnOperation:
        """Create a struct column with named fields.

        Args:
            *cols: Alternating field names (strings) and column values.
        """
        if len(cols) < 2 or len(cols) % 2 != 0:
            raise ValueError("named_struct requires alternating field names and values")

        # Use first value column as base (skip first name)
        base_col = cols[1] if isinstance(cols[1], Column) else Column(str(cols[1]))

        return ColumnOperation(
            base_col,
            "named_struct",
            value=cols,
            name="named_struct(...)",
        )

    # Bitwise functions (PySpark 3.2+)
    @staticmethod
    def bit_count(column: Union[Column, str]) -> ColumnOperation:
        """Count set bits."""
        return BitwiseFunctions.bit_count(column)

    @staticmethod
    def bit_get(column: Union[Column, str], pos: int) -> ColumnOperation:
        """Get bit at position."""
        return BitwiseFunctions.bit_get(column, pos)

    @staticmethod
    def getbit(column: Union[Column, str], pos: int) -> ColumnOperation:
        """Get bit at position (alias for bit_get) (PySpark 3.5+)."""
        return BitwiseFunctions.getbit(column, pos)

    # Bitmap Functions (PySpark 3.5+)
    @staticmethod
    def bitmap_bit_position(column: Union[Column, str]) -> ColumnOperation:
        """Get the bit position in a bitmap (PySpark 3.5+)."""
        return BitwiseFunctions.bitmap_bit_position(column)

    @staticmethod
    def bitmap_bucket_number(column: Union[Column, str]) -> ColumnOperation:
        """Get the bucket number in a bitmap (PySpark 3.5+)."""
        return BitwiseFunctions.bitmap_bucket_number(column)

    @staticmethod
    def bitmap_construct_agg(column: Union[Column, str]) -> AggregateFunction:
        """Aggregate function - construct bitmap from values (PySpark 3.5+)."""
        return BitwiseFunctions.bitmap_construct_agg(column)

    @staticmethod
    def bitmap_count(column: Union[Column, str]) -> ColumnOperation:
        """Count the number of set bits in a bitmap (PySpark 3.5+)."""
        return BitwiseFunctions.bitmap_count(column)

    @staticmethod
    def bitmap_or_agg(column: Union[Column, str]) -> AggregateFunction:
        """Aggregate function - bitwise OR of bitmaps (PySpark 3.5+)."""
        return BitwiseFunctions.bitmap_or_agg(column)

    @staticmethod
    def bitwise_not(column: Union[Column, str]) -> ColumnOperation:
        """Bitwise NOT."""
        return BitwiseFunctions.bitwise_not(column)

    @staticmethod
    def bit_and(column: Union[Column, str]) -> AggregateFunction:
        """Bitwise AND aggregate (PySpark 3.5+)."""
        return BitwiseFunctions.bit_and(column)

    @staticmethod
    def bit_or(column: Union[Column, str]) -> AggregateFunction:
        """Bitwise OR aggregate (PySpark 3.5+)."""
        return BitwiseFunctions.bit_or(column)

    @staticmethod
    def bit_xor(column: Union[Column, str]) -> AggregateFunction:
        """Bitwise XOR aggregate (PySpark 3.5+)."""
        return BitwiseFunctions.bit_xor(column)

    # Timezone functions (PySpark 3.2+)
    @staticmethod
    def convert_timezone(
        sourceTz: str, targetTz: str, sourceTs: Union[Column, str]
    ) -> ColumnOperation:
        """Convert timestamp between timezones."""
        return DateTimeFunctions.convert_timezone(sourceTz, targetTz, sourceTs)

    @staticmethod
    def current_timezone() -> ColumnOperation:
        """Get current timezone.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        Functions._require_active_session("current_timezone function")
        return DateTimeFunctions.current_timezone()

    @staticmethod
    def from_utc_timestamp(ts: Union[Column, str], tz: str) -> ColumnOperation:
        """Convert UTC timestamp to timezone."""
        return DateTimeFunctions.from_utc_timestamp(ts, tz)

    @staticmethod
    def to_utc_timestamp(ts: Union[Column, str], tz: str) -> ColumnOperation:
        """Convert timestamp to UTC."""
        return DateTimeFunctions.to_utc_timestamp(ts, tz)

    # URL functions (PySpark 3.2+)
    @staticmethod
    def parse_url(url: Union[Column, str], part: str) -> ColumnOperation:
        """Extract part from URL."""
        return StringFunctions.parse_url(url, part)

    @staticmethod
    def url_encode(url: Union[Column, str]) -> ColumnOperation:
        """URL-encode string."""
        return StringFunctions.url_encode(url)

    @staticmethod
    def url_decode(url: Union[Column, str]) -> ColumnOperation:
        """URL-decode string."""
        return StringFunctions.url_decode(url)

    @staticmethod
    def overlay(
        src: Union[Column, str],
        replace: Union[Column, str],
        pos: Union[Column, int],
        len: Union[Column, int] = -1,
    ) -> ColumnOperation:
        """Replace part of string (PySpark 3.0+)."""
        return StringFunctions.overlay(src, replace, pos, len)

    # Miscellaneous functions (PySpark 3.2+)
    @staticmethod
    def date_part(field: str, source: Union[Column, str]) -> ColumnOperation:
        """Extract date/time part."""
        return DateTimeFunctions.date_part(field, source)

    @staticmethod
    def dayname(date: Union[Column, str]) -> ColumnOperation:
        """Get day of week name."""
        return DateTimeFunctions.dayname(date)

    @staticmethod
    def assert_true(
        condition: Union[Column, ColumnOperation],
    ) -> ColumnOperation:
        """Assert condition is true."""
        return ConditionalFunctions.assert_true(condition)

    @staticmethod
    def ifnull(col1: Union[Column, str], col2: Union[Column, str]) -> ColumnOperation:
        """Return col2 if col1 is null (PySpark 3.5+)."""
        return ConditionalFunctions.ifnull(col1, col2)

    @staticmethod
    def nullif(col1: Union[Column, str], col2: Union[Column, str]) -> ColumnOperation:
        """Return null if col1 equals col2 (PySpark 3.5+)."""
        return ConditionalFunctions.nullif(col1, col2)

    # Null-safe try_* functions (PySpark 3.5+)
    @staticmethod
    def try_add(
        left: Union[Column, str, int, float],
        right: Union[Column, str, int, float],
    ) -> ColumnOperation:
        """Null-safe addition - returns NULL on error (PySpark 3.5+)."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.try_add(left, right)

    @staticmethod
    def try_subtract(
        left: Union[Column, str, int, float],
        right: Union[Column, str, int, float],
    ) -> ColumnOperation:
        """Null-safe subtraction - returns NULL on error (PySpark 3.5+)."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.try_subtract(left, right)

    @staticmethod
    def try_multiply(
        left: Union[Column, str, int, float],
        right: Union[Column, str, int, float],
    ) -> ColumnOperation:
        """Null-safe multiplication - returns NULL on error (PySpark 3.5+)."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.try_multiply(left, right)

    @staticmethod
    def try_divide(
        left: Union[Column, str, int, float],
        right: Union[Column, str, int, float],
    ) -> ColumnOperation:
        """Null-safe division - returns NULL on error (PySpark 3.5+)."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.try_divide(left, right)

    @staticmethod
    def try_sum(column: Union[Column, str]) -> AggregateFunction:
        """Null-safe sum aggregate - returns NULL on error (PySpark 3.5+)."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.try_sum(column)

    @staticmethod
    def try_avg(column: Union[Column, str]) -> AggregateFunction:
        """Null-safe average aggregate - returns NULL on error (PySpark 3.5+)."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.try_avg(column)

    @staticmethod
    def try_element_at(
        column: Union[Column, str], index: Union[Column, str, int]
    ) -> ColumnOperation:
        """Null-safe element_at - returns NULL on error (PySpark 3.5+)."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.try_element_at(column, index)

    @staticmethod
    def try_to_binary(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Null-safe to_binary - returns NULL on error (PySpark 3.5+)."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.try_to_binary(column, format)

    @staticmethod
    def try_to_number(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Null-safe to_number - returns NULL on error (PySpark 3.5+)."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.try_to_number(column, format)

    @staticmethod
    def try_to_timestamp(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Null-safe to_timestamp - returns NULL on error (PySpark 3.5+)."""
        from .conditional import ConditionalFunctions

        return ConditionalFunctions.try_to_timestamp(column, format)

    # XML functions (PySpark 3.2+)
    @staticmethod
    def from_xml(col: Union[Column, str], schema: str) -> ColumnOperation:
        """Parse XML string to struct."""
        return XMLFunctions.from_xml(col, schema)

    @staticmethod
    def to_xml(col: Union[Column, ColumnOperation]) -> ColumnOperation:
        """Convert struct to XML string."""
        return XMLFunctions.to_xml(col)

    @staticmethod
    def schema_of_xml(col: Union[Column, str]) -> ColumnOperation:
        """Infer schema from XML."""
        return XMLFunctions.schema_of_xml(col)

    @staticmethod
    def xpath(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract array from XML using XPath."""
        return XMLFunctions.xpath(xml, path)

    @staticmethod
    def xpath_boolean(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract boolean from XML using XPath."""
        return XMLFunctions.xpath_boolean(xml, path)

    @staticmethod
    def xpath_double(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract double from XML using XPath."""
        return XMLFunctions.xpath_double(xml, path)

    @staticmethod
    def xpath_float(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract float from XML using XPath."""
        return XMLFunctions.xpath_float(xml, path)

    @staticmethod
    def xpath_int(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract integer from XML using XPath."""
        return XMLFunctions.xpath_int(xml, path)

    @staticmethod
    def xpath_long(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract long from XML using XPath."""
        return XMLFunctions.xpath_long(xml, path)

    @staticmethod
    def xpath_short(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract short from XML using XPath."""
        return XMLFunctions.xpath_short(xml, path)

    @staticmethod
    def xpath_string(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract string from XML using XPath."""
        return XMLFunctions.xpath_string(xml, path)

    # JSON/CSV functions
    @staticmethod
    def from_json(
        column: Union[Column, str],
        schema: Any,
        options: Optional[Dict[str, Any]] = None,
    ) -> ColumnOperation:
        """Parse JSON string into struct/array."""
        from sparkless.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.from_json(column, schema, options)

    @staticmethod
    def to_json(column: Union[Column, str]) -> ColumnOperation:
        """Convert struct/array to JSON string."""
        from sparkless.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.to_json(column)

    @staticmethod
    def get_json_object(column: Union[Column, str], path: str) -> ColumnOperation:
        """Extract JSON object at path."""
        from sparkless.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.get_json_object(column, path)

    @staticmethod
    def json_tuple(column: Union[Column, str], *fields: str) -> ColumnOperation:
        """Extract multiple fields from JSON."""
        from sparkless.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.json_tuple(column, *fields)

    @staticmethod
    def schema_of_json(json_string: str) -> ColumnOperation:
        """Infer schema from JSON string."""
        from sparkless.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.schema_of_json(json_string)

    @staticmethod
    def from_csv(
        column: Union[Column, str],
        schema: Any,
        options: Optional[Dict[str, Any]] = None,
    ) -> ColumnOperation:
        """Parse CSV string into struct."""
        from sparkless.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.from_csv(column, schema, options)

    @staticmethod
    def to_csv(column: Union[Column, str]) -> ColumnOperation:
        """Convert struct to CSV string."""
        from sparkless.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.to_csv(column)

    @staticmethod
    def schema_of_csv(csv_string: str) -> ColumnOperation:
        """Infer schema from CSV string."""
        from sparkless.functions.json_csv import JSONCSVFunctions

        return JSONCSVFunctions.schema_of_csv(csv_string)

    # Column ordering functions
    @staticmethod
    def asc(column: Union[Column, str]) -> ColumnOperation:
        """Sort ascending."""
        from sparkless.functions.ordering import OrderingFunctions

        return OrderingFunctions.asc(column)

    @staticmethod
    def asc_nulls_first(column: Union[Column, str]) -> ColumnOperation:
        """Sort ascending, nulls first."""
        from sparkless.functions.ordering import OrderingFunctions

        return OrderingFunctions.asc_nulls_first(column)

    @staticmethod
    def asc_nulls_last(column: Union[Column, str]) -> ColumnOperation:
        """Sort ascending, nulls last."""
        from sparkless.functions.ordering import OrderingFunctions

        return OrderingFunctions.asc_nulls_last(column)

    @staticmethod
    def desc_nulls_first(column: Union[Column, str]) -> ColumnOperation:
        """Sort descending, nulls first."""
        from sparkless.functions.ordering import OrderingFunctions

        return OrderingFunctions.desc_nulls_first(column)

    @staticmethod
    def desc_nulls_last(column: Union[Column, str]) -> ColumnOperation:
        """Sort descending, nulls last."""
        from sparkless.functions.ordering import OrderingFunctions

        return OrderingFunctions.desc_nulls_last(column)

    # Metadata/utility functions
    @staticmethod
    def input_file_name() -> ColumnOperation:
        """Return input file name."""
        from sparkless.functions.metadata import MetadataFunctions

        return MetadataFunctions.input_file_name()

    @staticmethod
    def monotonically_increasing_id() -> ColumnOperation:
        """Generate monotonically increasing ID."""
        from sparkless.functions.metadata import MetadataFunctions

        return MetadataFunctions.monotonically_increasing_id()

    @staticmethod
    def spark_partition_id() -> ColumnOperation:
        """Return partition ID."""
        from sparkless.functions.metadata import MetadataFunctions

        return MetadataFunctions.spark_partition_id()

    @staticmethod
    def broadcast(df: Any) -> Any:
        """Mark DataFrame for broadcast (hint)."""
        from sparkless.functions.metadata import MetadataFunctions

        return MetadataFunctions.broadcast(df)

    @staticmethod
    def column(col_name: str) -> Column:
        """Create column reference (alias for col)."""
        from sparkless.functions.metadata import MetadataFunctions

        return MetadataFunctions.column(col_name)

    @staticmethod
    def grouping(column: Union[Column, str]) -> ColumnOperation:
        """Grouping indicator for CUBE/ROLLUP."""
        from sparkless.functions.metadata import GroupingFunctions

        return GroupingFunctions.grouping(column)

    @staticmethod
    def grouping_id(*cols: Union[Column, str]) -> ColumnOperation:
        """Grouping ID for CUBE/ROLLUP."""
        from sparkless.functions.metadata import GroupingFunctions

        return GroupingFunctions.grouping_id(*cols)

    @staticmethod
    def udf(
        f: Optional[Callable[..., Any]] = None, returnType: Any = None
    ) -> Callable[..., Any]:
        """Create a user-defined function (all PySpark versions).

        Args:
            f: Python function to wrap, or DataType if used as decorator with returnType
            returnType: Return type of the function (defaults to StringType)

        Returns:
            Wrapped function that can be used in DataFrame operations

        Example:
            >>> from sparkless.sql import SparkSession, functions as F
            >>> from sparkless.spark_types import IntegerType
            >>> spark = SparkSession("test")
            >>> square = F.udf(lambda x: x * x, IntegerType())
            >>> df = spark.createDataFrame([{"value": 5}])
            >>> df.select(square("value").alias("squared")).show()

            # Decorator pattern:
            >>> @F.udf(IntegerType())
            >>> def square(x):
            ...     return x * x
            >>> df.select(square("value")).show()
        """
        from sparkless.spark_types import DataType, StringType

        # Handle decorator pattern: @udf(DataType()) where DataType is passed as first arg
        # When used as @udf(T.StringType()), the DataType instance is passed as f
        # Check if f is a DataType instance (not a callable function)
        if f is not None and isinstance(f, DataType):
            # f is actually the returnType, not a function
            returnType = f
            f = None

        if returnType is None:
            returnType = StringType()

        def udf_wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
            """Wrap function to create ColumnOperation."""

            def apply_udf(*cols: Union[Column, str]) -> ColumnOperation:
                # Convert string column names to Column objects
                column_objs = []
                for col in cols:
                    if isinstance(col, str):
                        column_objs.append(Column(col))
                    else:
                        column_objs.append(col)

                # Create the first column operation
                if not column_objs:
                    raise ValueError("UDF requires at least one column argument")

                first_col = column_objs[0]
                # Get column name safely
                col_name = getattr(first_col, "name", str(first_col))

                # Generate name with all column names
                if len(column_objs) == 1:
                    udf_name = f"udf({col_name})"
                else:
                    all_names = [getattr(c, "name", str(c)) for c in column_objs]
                    udf_name = f"udf({', '.join(all_names)})"

                # Create a UDF operation that stores the function
                op = ColumnOperation(first_col, "udf", name=udf_name)
                op._udf_func = func
                op._udf_return_type = returnType
                op._udf_cols = column_objs
                return op

            return apply_udf

        # Support decorator pattern: @udf or udf(lambda x: x)
        if f is None:
            return udf_wrapper
        else:
            return udf_wrapper(f)

    @staticmethod
    def pandas_udf(
        f: Optional[Any] = None, returnType: Any = None, functionType: Any = None
    ) -> Any:
        """Create a Pandas UDF (vectorized UDF) (all PySpark versions).

        Pandas UDFs are user-defined functions that execute vectorized operations
        using Pandas Series/DataFrame, providing better performance than row-at-a-time UDFs.

        Args:
            f: Python function to wrap OR return type (if used as decorator)
            returnType: Return type of the function (defaults to StringType)
            functionType: Type of Pandas UDF (optional, for compatibility)

        Returns:
            Wrapped function that can be used in DataFrame operations

        Example:
            >>> from sparkless.sql import SparkSession, functions as F
            >>> from sparkless.spark_types import IntegerType
            >>> spark = SparkSession("test")
            >>> @F.pandas_udf(IntegerType())
            >>> def multiply_by_two(s):
            ...     return s * 2
            >>> df = spark.createDataFrame([{"value": 5}])
            >>> df.select(multiply_by_two("value").alias("doubled")).show()
        """
        from sparkless.spark_types import StringType
        from sparkless.functions.udf import UserDefinedFunction

        # Handle different call patterns:
        # 1. @pandas_udf(IntegerType()) - f is the type, returnType is None
        # 2. @pandas_udf(returnType=IntegerType()) - f is None, returnType is the type
        # 3. pandas_udf(lambda x: x, IntegerType()) - f is function, returnType is the type

        # Check if first argument is a data type (not a function)
        if f is not None and not callable(f):
            # f is actually the return type
            actual_returnType = f
            f = None
        else:
            actual_returnType = returnType if returnType is not None else StringType()

        def pandas_udf_wrapper(func: Callable[..., Any]) -> UserDefinedFunction:
            """Wrap function to create UserDefinedFunction with Pandas eval type."""
            udf_obj = UserDefinedFunction(func, actual_returnType, evalType="PANDAS")
            return udf_obj

        # Support decorator pattern: @pandas_udf or pandas_udf(lambda x: x)
        if f is None:
            return pandas_udf_wrapper
        else:
            return pandas_udf_wrapper(f)

    @staticmethod
    def window(
        timeColumn: Union[Column, str],
        windowDuration: str,
        slideDuration: Optional[str] = None,
        startTime: Optional[str] = None,
    ) -> ColumnOperation:
        """Create time-based window for grouping operations (all PySpark versions).

        Args:
            timeColumn: Timestamp column to window
            windowDuration: Duration string (e.g., "10 seconds", "1 minute", "2 hours")
            slideDuration: Slide duration for sliding windows (defaults to windowDuration)
            startTime: Offset for window alignment (e.g., "0 seconds")

        Returns:
            Column representing window struct with start and end times

        Example:
            >>> df.groupBy(F.window("timestamp", "10 minutes")).count()
            >>> df.groupBy(F.window("timestamp", "10 minutes", "5 minutes")).agg(F.sum("value"))
        """
        column = Column(timeColumn) if isinstance(timeColumn, str) else timeColumn

        # Create a window operation
        op = ColumnOperation(column, "window", name=f"window({column.name})")
        op._window_duration = windowDuration
        op._window_slide = slideDuration or windowDuration
        op._window_start = startTime or "0 seconds"
        return op

    @staticmethod
    def window_time(windowColumn: Union[Column, str]) -> ColumnOperation:
        """Extract window start time from window column (PySpark 3.4+).

        Args:
            windowColumn: Window column to extract time from

        Returns:
            Column operation representing window start timestamp

        Example:
            >>> df.groupBy(F.window("timestamp", "1 hour")).agg(
            ...     F.window_time(F.col("window")).alias("window_start")
            ... )
        """
        column = Column(windowColumn) if isinstance(windowColumn, str) else windowColumn
        op = ColumnOperation(column, "window_time", name=f"window_time({column.name})")
        return op

    # New string functions (Phase 1)
    @staticmethod
    def ilike(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """Case-insensitive LIKE pattern matching."""
        return StringFunctions.ilike(column, pattern)

    @staticmethod
    def find_in_set(
        column: Union[Column, str], str_list: Union[Column, str]
    ) -> ColumnOperation:
        """Find position of value in comma-separated string list."""
        return StringFunctions.find_in_set(column, str_list)

    @staticmethod
    def regexp_count(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """Count occurrences of regex pattern in string."""
        return StringFunctions.regexp_count(column, pattern)

    @staticmethod
    def regexp_like(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """Regex pattern matching (similar to rlike)."""
        return StringFunctions.regexp_like(column, pattern)

    @staticmethod
    def regexp_substr(
        column: Union[Column, str], pattern: str, pos: int = 1, occurrence: int = 1
    ) -> ColumnOperation:
        """Extract substring matching regex pattern."""
        return StringFunctions.regexp_substr(column, pattern, pos, occurrence)

    @staticmethod
    def regexp_instr(
        column: Union[Column, str], pattern: str, pos: int = 1, occurrence: int = 1
    ) -> ColumnOperation:
        """Find position of regex pattern match."""
        return StringFunctions.regexp_instr(column, pattern, pos, occurrence)

    @staticmethod
    def regexp(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """Alias for rlike - regex pattern matching."""
        return StringFunctions.regexp(column, pattern)

    @staticmethod
    def sentences(
        column: Union[Column, str],
        language: Optional[str] = None,
        country: Optional[str] = None,
    ) -> ColumnOperation:
        """Split text into sentences."""
        return StringFunctions.sentences(column, language, country)

    @staticmethod
    def printf(format_str: str, *columns: Union[Column, str]) -> ColumnOperation:
        """Formatted string (like sprintf)."""
        return StringFunctions.printf(format_str, *columns)

    @staticmethod
    def to_char(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert number/date to character string."""
        return StringFunctions.to_char(column, format)

    @staticmethod
    def to_varchar(
        column: Union[Column, str], length: Optional[int] = None
    ) -> ColumnOperation:
        """Convert to varchar type."""
        return StringFunctions.to_varchar(column, length)

    @staticmethod
    def typeof(column: Union[Column, str]) -> ColumnOperation:
        """Get type of value as string."""
        return StringFunctions.typeof(column)

    @staticmethod
    def stack(n: int, *cols: Union[Column, str, Any]) -> ColumnOperation:
        """Stack multiple columns into rows."""
        return StringFunctions.stack(n, *cols)

    # New math/bitwise functions (Phase 2)
    @staticmethod
    def pmod(
        dividend: Union[Column, str, int], divisor: Union[Column, str, int]
    ) -> ColumnOperation:
        """Positive modulo - always returns positive remainder."""
        return MathFunctions.pmod(dividend, divisor)

    @staticmethod
    def negate(column: Union[Column, str]) -> ColumnOperation:
        """Negate value (alias for negative)."""
        return MathFunctions.negate(column)

    @staticmethod
    def shiftleft(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Bitwise left shift."""
        return BitwiseFunctions.shiftleft(column, num_bits)

    @staticmethod
    def shiftright(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Bitwise right shift (signed)."""
        return BitwiseFunctions.shiftright(column, num_bits)

    @staticmethod
    def shiftrightunsigned(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Bitwise unsigned right shift."""
        return BitwiseFunctions.shiftrightunsigned(column, num_bits)

    # Deprecated camelCase aliases (PySpark 3.0-3.1)
    @staticmethod
    def shiftLeft(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Deprecated alias for shiftleft (PySpark 3.0-3.1)."""
        return BitwiseFunctions.shiftLeft(column, num_bits)

    @staticmethod
    def shiftRight(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Deprecated alias for shiftright (PySpark 3.0-3.1)."""
        return BitwiseFunctions.shiftRight(column, num_bits)

    @staticmethod
    def shiftRightUnsigned(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Deprecated alias for shiftrightunsigned (PySpark 3.0-3.1)."""
        return BitwiseFunctions.shiftRightUnsigned(column, num_bits)

    # New datetime functions (Phase 3)
    @staticmethod
    def years(column: Union[Column, str, int]) -> ColumnOperation:
        """Convert number to years interval."""
        return DateTimeFunctions.years(column)

    @staticmethod
    def localtimestamp() -> ColumnOperation:
        """Get local timestamp (without timezone)."""
        return DateTimeFunctions.localtimestamp()

    @staticmethod
    def dateadd(
        date_part: str, value: Union[Column, str, int], date: Union[Column, str]
    ) -> ColumnOperation:
        """SQL Server style date addition."""
        return DateTimeFunctions.dateadd(date_part, value, date)

    @staticmethod
    def datepart(date_part: str, date: Union[Column, str]) -> ColumnOperation:
        """SQL Server style date part extraction."""
        return DateTimeFunctions.datepart(date_part, date)

    @staticmethod
    def make_timestamp(
        year: Union[Column, str, int],
        month: Union[Column, str, int],
        day: Union[Column, str, int],
        hour: Union[Column, str, int] = 0,
        minute: Union[Column, str, int] = 0,
        second: Union[Column, str, int] = 0,
    ) -> ColumnOperation:
        """Create timestamp from components."""
        return DateTimeFunctions.make_timestamp(year, month, day, hour, minute, second)

    @staticmethod
    def make_timestamp_ltz(
        year: Union[Column, str, int],
        month: Union[Column, str, int],
        day: Union[Column, str, int],
        hour: Union[Column, str, int] = 0,
        minute: Union[Column, str, int] = 0,
        second: Union[Column, str, int] = 0,
        timezone: Optional[str] = None,
    ) -> ColumnOperation:
        """Create timestamp with local timezone."""
        return DateTimeFunctions.make_timestamp_ltz(
            year, month, day, hour, minute, second, timezone
        )

    @staticmethod
    def make_timestamp_ntz(
        year: Union[Column, str, int],
        month: Union[Column, str, int],
        day: Union[Column, str, int],
        hour: Union[Column, str, int] = 0,
        minute: Union[Column, str, int] = 0,
        second: Union[Column, str, int] = 0,
    ) -> ColumnOperation:
        """Create timestamp with no timezone."""
        return DateTimeFunctions.make_timestamp_ntz(
            year, month, day, hour, minute, second
        )

    @staticmethod
    def make_interval(
        years: Union[Column, str, int] = 0,
        months: Union[Column, str, int] = 0,
        weeks: Union[Column, str, int] = 0,
        days: Union[Column, str, int] = 0,
        hours: Union[Column, str, int] = 0,
        mins: Union[Column, str, int] = 0,
        secs: Union[Column, str, int] = 0,
    ) -> ColumnOperation:
        """Create interval from components."""
        return DateTimeFunctions.make_interval(
            years, months, weeks, days, hours, mins, secs
        )

    @staticmethod
    def make_dt_interval(
        days: Union[Column, str, int] = 0,
        hours: Union[Column, str, int] = 0,
        mins: Union[Column, str, int] = 0,
        secs: Union[Column, str, int] = 0,
    ) -> ColumnOperation:
        """Create day-time interval."""
        return DateTimeFunctions.make_dt_interval(days, hours, mins, secs)

    @staticmethod
    def make_ym_interval(
        years: Union[Column, str, int] = 0, months: Union[Column, str, int] = 0
    ) -> ColumnOperation:
        """Create year-month interval."""
        return DateTimeFunctions.make_ym_interval(years, months)

    @staticmethod
    def to_number(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert string to number."""
        return DateTimeFunctions.to_number(column, format)

    @staticmethod
    def to_binary(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert to binary format."""
        return DateTimeFunctions.to_binary(column, format)

    @staticmethod
    def to_unix_timestamp(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert to unix timestamp."""
        return DateTimeFunctions.to_unix_timestamp(column, format)

    @staticmethod
    def unix_date(column: Union[Column, str]) -> ColumnOperation:
        """Convert unix timestamp to date."""
        return DateTimeFunctions.unix_date(column)

    @staticmethod
    def unix_seconds(column: Union[Column, str]) -> ColumnOperation:
        """Convert timestamp to unix seconds."""
        return DateTimeFunctions.unix_seconds(column)

    @staticmethod
    def unix_millis(column: Union[Column, str]) -> ColumnOperation:
        """Convert timestamp to unix milliseconds."""
        return DateTimeFunctions.unix_millis(column)

    @staticmethod
    def unix_micros(column: Union[Column, str]) -> ColumnOperation:
        """Convert timestamp to unix microseconds."""
        return DateTimeFunctions.unix_micros(column)

    @staticmethod
    def timestamp_millis(column: Union[Column, str]) -> ColumnOperation:
        """Create timestamp from unix milliseconds."""
        return DateTimeFunctions.timestamp_millis(column)

    @staticmethod
    def timestamp_micros(column: Union[Column, str]) -> ColumnOperation:
        """Create timestamp from unix microseconds."""
        return DateTimeFunctions.timestamp_micros(column)

    # New aggregate functions (Phase 4)
    @staticmethod
    def regr_avgx(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression average of x."""
        return AggregateFunctions.regr_avgx(y, x)

    @staticmethod
    def regr_avgy(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression average of y."""
        return AggregateFunctions.regr_avgy(y, x)

    @staticmethod
    def regr_count(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression count."""
        return AggregateFunctions.regr_count(y, x)

    @staticmethod
    def regr_intercept(
        y: Union[Column, str], x: Union[Column, str]
    ) -> AggregateFunction:
        """Linear regression intercept."""
        return AggregateFunctions.regr_intercept(y, x)

    @staticmethod
    def regr_r2(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression R-squared."""
        return AggregateFunctions.regr_r2(y, x)

    @staticmethod
    def regr_slope(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression slope."""
        return AggregateFunctions.regr_slope(y, x)

    @staticmethod
    def regr_sxx(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression sum of squares of x."""
        return AggregateFunctions.regr_sxx(y, x)

    @staticmethod
    def regr_sxy(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression sum of products."""
        return AggregateFunctions.regr_sxy(y, x)

    @staticmethod
    def regr_syy(y: Union[Column, str], x: Union[Column, str]) -> AggregateFunction:
        """Linear regression sum of squares of y."""
        return AggregateFunctions.regr_syy(y, x)

    # New utility functions (Phase 5)
    @staticmethod
    def get(
        col: Union[Column, str], key: Union[Column, str, int, Any]
    ) -> ColumnOperation:
        """Get element from array by index or map by key."""
        return ArrayFunctions.get(col, key)

    @staticmethod
    def inline(col: Union[Column, str]) -> ColumnOperation:
        """Explode array of structs into rows."""
        return ArrayFunctions.inline(col)

    @staticmethod
    def inline_outer(col: Union[Column, str]) -> ColumnOperation:
        """Explode array of structs into rows (outer join style)."""
        return ArrayFunctions.inline_outer(col)

    @staticmethod
    def str_to_map(
        column: Union[Column, str],
        pair_delim: Optional[str] = ",",
        key_value_delim: Optional[str] = ":",
    ) -> ColumnOperation:
        """Convert string to map using delimiters."""
        return MapFunctions.str_to_map(column, pair_delim, key_value_delim)

    # Deprecated Aliases
    @staticmethod
    def approxCountDistinct(*cols: Union[Column, str]) -> AggregateFunction:
        """Deprecated alias for approx_count_distinct (all PySpark versions)."""
        return AggregateFunctions.approxCountDistinct(*cols)

    @staticmethod
    def sumDistinct(column: Union[Column, str]) -> AggregateFunction:
        """Deprecated alias for sum_distinct (all PySpark versions)."""
        return AggregateFunctions.sumDistinct(column)

    @staticmethod
    def bitwiseNOT(column: Union[Column, str]) -> ColumnOperation:
        """Deprecated alias for bitwise_not (all PySpark versions)."""
        return BitwiseFunctions.bitwiseNOT(column)

    @staticmethod
    def toDegrees(column: Union[Column, str]) -> ColumnOperation:
        """Deprecated alias for degrees (all PySpark versions)."""
        return MathFunctions.toDegrees(column)

    @staticmethod
    def toRadians(column: Union[Column, str]) -> ColumnOperation:
        """Deprecated alias for radians (all PySpark versions)."""
        return MathFunctions.toRadians(column)

    # Dynamic dispatch helpers
    @staticmethod
    def call_function(function_name: str, *columns: Any) -> Any:
        """
        Dynamically invoke a function from the sparkless functions namespace.

        Args:
            function_name: Name of the function to invoke (e.g. ``"upper"``).
            *columns: Positional arguments passed to the resolved function.

        Returns:
            Whatever the resolved function returns (typically a ColumnOperation).

        Raises:
            PySparkValueError: If the requested function is not registered.
            PySparkTypeError: If the supplied arguments are incompatible with the
                resolved function signature.
        """

        if not isinstance(function_name, str) or not function_name:
            raise PySparkTypeError(
                "call_function() expects a non-empty string as function name"
            )

        if function_name == "call_function":
            raise PySparkValueError("Function 'call_function' cannot be dispatched")

        candidate = getattr(Functions, function_name, None)
        if candidate is None or not callable(candidate):
            raise PySparkValueError(
                f"Function {function_name!r} is not registered in sparkless"
            )

        try:
            return candidate(*columns)
        except TypeError as exc:
            raise PySparkTypeError(
                f"Function {function_name!r} does not accept the provided arguments: {exc}"
            ) from exc


# Create the F namespace instance
F = Functions()

# Re-export all the main classes for backward compatibility
__all__ = [
    "Column",
    "ColumnOperation",
    "Literal",
    "AggregateFunction",
    "CaseWhen",
    "WindowFunction",
    "Functions",
    "F",
    "StringFunctions",
    "MathFunctions",
    "AggregateFunctions",
    "DateTimeFunctions",
]
