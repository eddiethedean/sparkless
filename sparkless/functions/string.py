"""
String functions for Sparkless.

This module provides comprehensive string manipulation functions that match PySpark's
string function API. Includes case conversion, trimming, pattern matching, and string
transformation operations for text processing in DataFrames.

Key Features:
    - Complete PySpark string function API compatibility
    - Case conversion (upper, lower)
    - Length and trimming operations (length, trim, ltrim, rtrim)
    - Pattern matching and replacement (regexp_replace, split)
    - String manipulation (substring, concat)
    - Type-safe operations with proper return types
    - Support for both column references and string literals

Example:
    >>> from sparkless.sql import SparkSession, functions as F
    >>> spark = SparkSession("test")
    >>> data = [{"name": "  Alice  ", "email": "alice@example.com"}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(
    ...     F.upper(F.trim(F.col("name"))),
    ...     F.regexp_replace(F.col("email"), "@.*", "@company.com")
    ... ).show()
    DataFrame[1 rows, 2 columns]

    upper(trim(name)) regexp_replace(email, '@.*', '@company.com')
    ALICE               alice@example.com
"""

from typing import Any, Optional, Union
from sparkless.functions.base import Column, ColumnOperation


class StringFunctions:
    """Collection of string manipulation functions."""

    @staticmethod
    def upper(column: Union[Column, str]) -> ColumnOperation:
        """Convert string to uppercase.

        Args:
            column: The column to convert.

        Returns:
            ColumnOperation representing the upper function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "upper", name=f"upper({column.name})")
        return operation

    @staticmethod
    def lower(column: Union[Column, str]) -> ColumnOperation:
        """Convert string to lowercase.

        Args:
            column: The column to convert.

        Returns:
            ColumnOperation representing the lower function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "lower", name=f"lower({column.name})")
        return operation

    @staticmethod
    def length(column: Union[Column, str]) -> ColumnOperation:
        """Get the length of a string.

        Args:
            column: The column to get length of.

        Returns:
            ColumnOperation representing the length function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "length", name=f"length({column.name})")
        return operation

    @staticmethod
    def char_length(column: Union[Column, str]) -> ColumnOperation:
        """Alias for length() - Get the character length of a string (PySpark 3.5+).

        Args:
            column: The column to get length of.

        Returns:
            ColumnOperation representing the char_length function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "length", name=f"char_length({column.name})"
        )
        return operation

    @staticmethod
    def character_length(column: Union[Column, str]) -> ColumnOperation:
        """Alias for length() - Get the character length of a string (PySpark 3.5+).

        Args:
            column: The column to get length of.

        Returns:
            ColumnOperation representing the character_length function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "length", name=f"character_length({column.name})"
        )
        return operation

    @staticmethod
    def trim(column: Union[Column, str]) -> ColumnOperation:
        """Trim whitespace from string.

        Args:
            column: The column to trim.

        Returns:
            ColumnOperation representing the trim function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "trim", name=f"trim({column.name})")
        return operation

    @staticmethod
    def ltrim(column: Union[Column, str]) -> ColumnOperation:
        """Trim whitespace from left side of string.

        Args:
            column: The column to trim.

        Returns:
            ColumnOperation representing the ltrim function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "ltrim", name=f"ltrim({column.name})")
        return operation

    @staticmethod
    def rtrim(column: Union[Column, str]) -> ColumnOperation:
        """Trim whitespace from right side of string.

        Args:
            column: The column to trim.

        Returns:
            ColumnOperation representing the rtrim function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "rtrim", name=f"rtrim({column.name})")
        return operation

    @staticmethod
    def btrim(
        column: Union[Column, str], trim_string: Optional[str] = None
    ) -> ColumnOperation:
        """Trim characters from both ends of string.

        Args:
            column: The column to trim.
            trim_string: Optional string of characters to trim (default: whitespace).

        Returns:
            ColumnOperation representing the btrim function.
        """
        if isinstance(column, str):
            column = Column(column)

        if trim_string is not None:
            operation = ColumnOperation(
                column,
                "btrim",
                trim_string,
                name=f"btrim({column.name}, '{trim_string}')",
            )
        else:
            operation = ColumnOperation(column, "btrim", name=f"btrim({column.name})")
        return operation

    @staticmethod
    def contains(column: Union[Column, str], substring: str) -> ColumnOperation:
        """Check if string contains substring.

        Args:
            column: The column to check.
            substring: The substring to search for.

        Returns:
            ColumnOperation representing the contains function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "contains",
            substring,
            name=f"contains({column.name}, '{substring}')",
        )
        return operation

    @staticmethod
    def left(column: Union[Column, str], length: int) -> ColumnOperation:
        """Extract left N characters from string.

        Args:
            column: The column to extract from.
            length: Number of characters to extract from the left.

        Returns:
            ColumnOperation representing the left function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "left", length, name=f"left({column.name}, {length})"
        )
        return operation

    @staticmethod
    def right(column: Union[Column, str], length: int) -> ColumnOperation:
        """Extract right N characters from string.

        Args:
            column: The column to extract from.
            length: Number of characters to extract from the right.

        Returns:
            ColumnOperation representing the right function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "right", length, name=f"right({column.name}, {length})"
        )
        return operation

    @staticmethod
    def bit_length(column: Union[Column, str]) -> ColumnOperation:
        """Get bit length of string.

        Args:
            column: The column to get bit length of.

        Returns:
            ColumnOperation representing the bit_length function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "bit_length", name=f"bit_length({column.name})"
        )
        return operation

    @staticmethod
    def startswith(column: Union[Column, str], substring: str) -> ColumnOperation:
        """Check if string starts with substring.

        Args:
            column: The column to check.
            substring: The substring to check for.

        Returns:
            ColumnOperation representing the startswith function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "startswith",
            substring,
            name=f"startswith({column.name}, '{substring}')",
        )
        return operation

    @staticmethod
    def endswith(column: Union[Column, str], substring: str) -> ColumnOperation:
        """Check if string ends with substring.

        Args:
            column: The column to check.
            substring: The substring to check for.

        Returns:
            ColumnOperation representing the endswith function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "endswith",
            substring,
            name=f"endswith({column.name}, '{substring}')",
        )
        return operation

    @staticmethod
    def like(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """SQL LIKE pattern matching.

        Args:
            column: The column to match.
            pattern: The LIKE pattern (supports % and _ wildcards).

        Returns:
            ColumnOperation representing the like function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "like", pattern, name=f"like({column.name}, '{pattern}')"
        )
        return operation

    @staticmethod
    def rlike(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """Regular expression pattern matching.

        Args:
            column: The column to match.
            pattern: The regular expression pattern.

        Returns:
            ColumnOperation representing the rlike function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "rlike", pattern, name=f"rlike({column.name}, '{pattern}')"
        )
        return operation

    @staticmethod
    def replace(column: Union[Column, str], old: str, new: str) -> ColumnOperation:
        """Replace occurrences of substring in string.

        Args:
            column: The column to replace in.
            old: The substring to replace.
            new: The replacement substring.

        Returns:
            ColumnOperation representing the replace function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "replace",
            (old, new),
            name=f"replace({column.name}, '{old}', '{new}')",
        )
        return operation

    @staticmethod
    def substr(
        column: Union[Column, str], start: int, length: Optional[int] = None
    ) -> ColumnOperation:
        """Alias for substring - Extract substring from string.

        Args:
            column: The column to extract from.
            start: Starting position (1-indexed).
            length: Optional length of substring.

        Returns:
            ColumnOperation representing the substr function.
        """
        return StringFunctions.substring(column, start, length)

    @staticmethod
    def split_part(
        column: Union[Column, str], delimiter: str, part: int
    ) -> ColumnOperation:
        """Extract part of string split by delimiter.

        Args:
            column: The column to split.
            delimiter: The delimiter to split on.
            part: The part number to extract (1-indexed).

        Returns:
            ColumnOperation representing the split_part function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "split_part",
            (delimiter, part),
            name=f"split_part({column.name}, '{delimiter}', {part})",
        )
        return operation

    @staticmethod
    def position(
        substring: Union[Column, str], column: Union[Column, str]
    ) -> ColumnOperation:
        """Find position of substring in string (1-indexed).

        Args:
            substring: The substring to search for.
            column: The column to search in.

        Returns:
            ColumnOperation representing the position function.
        """
        if isinstance(substring, str):
            substring = Column(substring)
        if isinstance(column, str):
            column = Column(column)

        # Note: PySpark position(substring, str) - substring first, then column
        # Format the name separately to avoid f-string backslash issues
        substr_repr = substring.name if hasattr(substring, "name") else str(substring)

        operation = ColumnOperation(
            column,
            "position",
            substring,
            name=f"position('{substr_repr}', {column.name})",
        )
        return operation

    @staticmethod
    def octet_length(column: Union[Column, str]) -> ColumnOperation:
        """Get byte length (octet length) of string.

        Args:
            column: The column to get byte length of.

        Returns:
            ColumnOperation representing the octet_length function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "octet_length", name=f"octet_length({column.name})"
        )
        return operation

    @staticmethod
    def char(column: Union[Column, str]) -> ColumnOperation:
        """Convert integer to character.

        Args:
            column: The column containing integer values.

        Returns:
            ColumnOperation representing the char function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "char", name=f"char({column.name})")
        return operation

    @staticmethod
    def ucase(column: Union[Column, str]) -> ColumnOperation:
        """Alias for upper - Convert string to uppercase.

        Args:
            column: The column to convert.

        Returns:
            ColumnOperation representing the ucase function.
        """
        return StringFunctions.upper(column)

    @staticmethod
    def lcase(column: Union[Column, str]) -> ColumnOperation:
        """Alias for lower - Convert string to lowercase.

        Args:
            column: The column to convert.

        Returns:
            ColumnOperation representing the lcase function.
        """
        return StringFunctions.lower(column)

    @staticmethod
    def elt(n: Union[Column, int], *columns: Union[Column, str]) -> ColumnOperation:
        """Return element at index from list of columns.

        Args:
            n: The index (1-indexed).
            *columns: The columns to choose from.

        Returns:
            ColumnOperation representing the elt function.
        """
        if not columns:
            raise ValueError("At least one column must be provided for elt")

        base_column = Column(columns[0]) if isinstance(columns[0], str) else columns[0]
        column_names = [
            col.name if hasattr(col, "name") else str(col) for col in columns
        ]
        operation = ColumnOperation(
            base_column,
            "elt",
            (n, columns),
            name=f"elt({n}, {', '.join(column_names)})",
        )
        return operation

    @staticmethod
    def regexp_replace(
        column: Union[Column, str], pattern: str, replacement: str
    ) -> ColumnOperation:
        """Replace regex pattern in string.

        Args:
            column: The column to replace in.
            pattern: The regex pattern to match.
            replacement: The replacement string.

        Returns:
            ColumnOperation representing the regexp_replace function.
        """
        if isinstance(column, str):
            column = Column(column)

        # PySpark format: regexp_replace(column, pattern, replacement, pos)
        # Default pos is 1, so we include it in the name
        operation = ColumnOperation(
            column,
            "regexp_replace",
            (pattern, replacement),
            name=f"regexp_replace({column.name}, {pattern}, {replacement}, 1)",
        )
        return operation

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

        Returns:
            ColumnOperation representing the split function.
        """
        if isinstance(column, str):
            column = Column(column)

        # PySpark default is -1 (no limit), but we use None internally for "no limit"
        # When limit is None, we'll use -1 in the name to match PySpark
        limit_value = limit if limit is not None else -1
        operation = ColumnOperation(
            column,
            "split",
            (delimiter, limit),
            name=f"split({column.name}, {delimiter}, {limit_value})",
        )
        return operation

    @staticmethod
    def substring(
        column: Union[Column, str], start: int, length: Optional[int] = None
    ) -> ColumnOperation:
        """Extract substring from string.

        Args:
            column: The column to extract from.
            start: Starting position (1-indexed).
            length: Optional length of substring.

        Returns:
            ColumnOperation representing the substring function.
        """
        if isinstance(column, str):
            column = Column(column)

        name = (
            f"substring({column.name}, {start}, {length})"
            if length is not None
            else f"substring({column.name}, {start})"
        )
        operation = ColumnOperation(column, "substring", (start, length), name=name)
        return operation

    @staticmethod
    def concat(*columns: Union[Column, str]) -> ColumnOperation:
        """Concatenate multiple strings.

        Args:
            *columns: Columns or strings to concatenate.

        Returns:
            ColumnOperation representing the concat function.
        """
        # Use the first column as the base
        if not columns:
            raise ValueError("At least one column must be provided")

        base_column = Column(columns[0]) if isinstance(columns[0], str) else columns[0]
        column_names = [
            col.name if hasattr(col, "name") else str(col) for col in columns
        ]
        operation = ColumnOperation(
            base_column,
            "concat",
            columns[1:],
            name=f"concat({', '.join(column_names)})",
        )
        return operation

    @staticmethod
    def format_string(format_str: str, *columns: Union[Column, str]) -> ColumnOperation:
        """Format string using printf-style format string.

        Args:
            format_str: The format string (e.g., "Hello %s, you are %d years old").
            *columns: Columns to use as format arguments.

        Returns:
            ColumnOperation representing the format_string function.
        """
        if not columns:
            raise ValueError("At least one column must be provided for format_string")

        base_column = Column(columns[0]) if isinstance(columns[0], str) else columns[0]
        column_names = [
            col.name if hasattr(col, "name") else str(col) for col in columns
        ]
        operation = ColumnOperation(
            base_column,
            "format_string",
            (format_str, columns[1:]),
            name=f"format_string('{format_str}', {', '.join(column_names)})",
        )
        return operation

    @staticmethod
    def translate(
        column: Union[Column, str], matching_string: str, replace_string: str
    ) -> ColumnOperation:
        """Translate characters in string using character mapping.

        Args:
            column: The column to translate.
            matching_string: Characters to match.
            replace_string: Characters to replace with (must be same length as matching_string).

        Returns:
            ColumnOperation representing the translate function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "translate",
            (matching_string, replace_string),
            # Match PySpark's column naming style (no quotes in the expression name)
            name=f"translate({column.name}, {matching_string}, {replace_string})",
        )
        return operation

    @staticmethod
    def ascii(column: Union[Column, str]) -> ColumnOperation:
        """Get ASCII value of first character in string.

        Args:
            column: The column to get ASCII value of.

        Returns:
            ColumnOperation representing the ascii function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "ascii", name=f"ascii({column.name})")
        return operation

    @staticmethod
    def base64(column: Union[Column, str]) -> ColumnOperation:
        """Encode string to base64.

        Args:
            column: The column to encode.

        Returns:
            ColumnOperation representing the base64 function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "base64", name=f"base64({column.name})")
        return operation

    @staticmethod
    def unbase64(column: Union[Column, str]) -> ColumnOperation:
        """Decode base64 string.

        Args:
            column: The column to decode.

        Returns:
            ColumnOperation representing the unbase64 function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "unbase64", name=f"unbase64({column.name})")
        return operation

    @staticmethod
    def regexp_extract_all(
        column: Union[Column, str], pattern: str, idx: int = 0
    ) -> ColumnOperation:
        r"""Extract all matches of a regex pattern.

        Args:
            column: The column to extract from.
            pattern: The regex pattern to match.
            idx: Group index to extract (default: 0 for entire match).

        Returns:
            ColumnOperation representing the regexp_extract_all function.

        Example:
            >>> df.select(F.regexp_extract_all(F.col("text"), r"\d+", 0))
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "regexp_extract_all",
            (pattern, idx),
            name=f"regexp_extract_all({column.name}, '{pattern}', {idx})",
        )
        return operation

    @staticmethod
    def array_join(
        column: Union[Column, str],
        delimiter: str,
        null_replacement: Optional[str] = None,
    ) -> ColumnOperation:
        """Join array elements with a delimiter.

        Args:
            column: The array column to join.
            delimiter: The delimiter to use for joining.
            null_replacement: Optional string to replace nulls with.

        Returns:
            ColumnOperation representing the array_join function.

        Example:
            >>> df.select(F.array_join(F.col("tags"), ", "))
            >>> df.select(F.array_join(F.col("tags"), "|", "N/A"))
        """
        if isinstance(column, str):
            column = Column(column)

        if null_replacement is not None:
            name = f"array_join({column.name}, '{null_replacement}', '{delimiter}')"
            args: Any = (delimiter, null_replacement)
        else:
            # PySpark doesn't quote the delimiter in the column name
            name = f"array_join({column.name}, {delimiter})"
            args = (delimiter, None)

        operation = ColumnOperation(column, "array_join", args, name=name)
        return operation

    @staticmethod
    def reverse(column: Union[Column, str]) -> ColumnOperation:
        """Reverse a string column.

        Args:
            column: The string column to reverse.

        Returns:
            ColumnOperation representing the reverse function.

        Example:
            >>> df.select(F.reverse(F.col("name")))
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "reverse", name=f"reverse({column.name})")
        return operation

    @staticmethod
    def repeat(column: Union[Column, str], n: int) -> ColumnOperation:
        """Repeat a string N times.

        Args:
            column: The column to repeat.
            n: Number of times to repeat.

        Returns:
            ColumnOperation representing the repeat function.

        Example:
            >>> df.select(F.repeat(F.col("text"), 3))
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "repeat", n, name=f"repeat({column.name}, {n})"
        )
        return operation

    @staticmethod
    def initcap(column: Union[Column, str]) -> ColumnOperation:
        """Capitalize first letter of each word.

        Args:
            column: The column to capitalize.

        Returns:
            ColumnOperation representing the initcap function.

        Example:
            >>> df.select(F.initcap(F.col("name")))
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "initcap", name=f"initcap({column.name})")
        return operation

    @staticmethod
    def soundex(column: Union[Column, str]) -> ColumnOperation:
        """Soundex encoding for phonetic matching.

        Args:
            column: The column to encode.

        Returns:
            ColumnOperation representing the soundex function.

        Example:
            >>> df.select(F.soundex(F.col("name")))
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "soundex", name=f"soundex({column.name})")
        return operation

    # URL Functions (PySpark 3.2+)

    @staticmethod
    def parse_url(url: Union[Column, str], part: str) -> ColumnOperation:
        """Extract a part from a URL.

        Args:
            url: URL column or string.
            part: Part to extract (HOST, PATH, QUERY, REF, PROTOCOL, FILE, AUTHORITY, USERINFO).

        Returns:
            ColumnOperation representing the parse_url function.

        Example:
            >>> df.select(F.parse_url(F.col("url"), "HOST"))
        """
        if isinstance(url, str):
            url = Column(url)

        return ColumnOperation(
            url,
            "parse_url",
            part,
            name=f"parse_url({url.name}, '{part}')",
        )

    @staticmethod
    def url_encode(url: Union[Column, str]) -> ColumnOperation:
        """URL-encode a string.

        Args:
            url: String column to encode.

        Returns:
            ColumnOperation representing the url_encode function.

        Example:
            >>> df.select(F.url_encode(F.col("text")))
        """
        if isinstance(url, str):
            url = Column(url)

        return ColumnOperation(url, "url_encode", name=f"url_encode({url.name})")

    @staticmethod
    def url_decode(url: Union[Column, str]) -> ColumnOperation:
        """URL-decode a string.

        Args:
            url: String column to decode.

        Returns:
            ColumnOperation representing the url_decode function.

        Example:
            >>> df.select(F.url_decode(F.col("encoded")))
        """
        if isinstance(url, str):
            url = Column(url)

        return ColumnOperation(url, "url_decode", name=f"url_decode({url.name})")

    @staticmethod
    def concat_ws(sep: str, *cols: Union[Column, str]) -> ColumnOperation:
        """Concatenate multiple columns with a separator.

        Args:
            sep: Separator string
            *cols: Columns to concatenate

        Returns:
            ColumnOperation representing concat_ws

        Example:
            >>> df.select(F.concat_ws("-", F.col("first"), F.col("last")))
        """
        columns = []
        for col in cols:
            if isinstance(col, str):
                columns.append(Column(col))
            else:
                columns.append(col)

        # Generate proper name with all column names
        col_names = [col.name if hasattr(col, "name") else str(col) for col in columns]
        name = f"concat_ws({sep}, {', '.join(col_names)})"

        return ColumnOperation(
            columns[0] if columns else Column(""),
            "concat_ws",
            value=(sep, columns[1:] if len(columns) > 1 else []),
            name=name,
        )

    @staticmethod
    def regexp_extract(
        column: Union[Column, str], pattern: str, idx: int = 0
    ) -> ColumnOperation:
        """Extract a specific group matched by a regex pattern.

        Args:
            column: Input column
            pattern: Regular expression pattern. Supports lookahead (?=...) and
                    lookbehind (?<=...) assertions via Python fallback when Polars
                    native support is unavailable.
            idx: Group index to extract (default 0)

        Returns:
            ColumnOperation representing regexp_extract

        Example:
            >>> df.select(F.regexp_extract(F.col("email"), r"(.+)@(.+)", 1))
            >>> df.select(F.regexp_extract(F.col("text"), r"(?<=prefix_)\\w+", 0))

        Note:
            Fixed in version 3.23.0 (Issue #228): Added fallback support for regex
            patterns with lookahead and lookbehind assertions using Python's re module
            when Polars native support is unavailable.
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "regexp_extract",
            value=(pattern, idx),
            name=f"regexp_extract({column.name}, {pattern}, {idx})",
        )

    @staticmethod
    def substring_index(
        column: Union[Column, str], delim: str, count: int
    ) -> ColumnOperation:
        """Returns substring before/after count occurrences of delimiter.

        Args:
            column: Input string column
            delim: Delimiter string
            count: Number of delimiters (positive for left, negative for right)

        Returns:
            ColumnOperation representing substring_index

        Example:
            >>> df.select(F.substring_index(F.col("path"), "/", 2))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "substring_index",
            value=(delim, count),
            name=f"substring_index({column.name}, {delim}, {count})",
        )

    @staticmethod
    def format_number(column: Union[Column, str], d: int) -> ColumnOperation:
        """Format number with d decimal places and thousands separator.

        Args:
            column: Numeric column
            d: Number of decimal places

        Returns:
            ColumnOperation representing format_number

        Example:
            >>> df.select(F.format_number(F.col("amount"), 2))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "format_number", value=d, name=f"format_number({column.name}, {d})"
        )

    @staticmethod
    def instr(column: Union[Column, str], substr: str) -> ColumnOperation:
        """Locate the position of the first occurrence of substr (1-indexed).

        Args:
            column: Input string column
            substr: Substring to locate

        Returns:
            ColumnOperation representing instr

        Example:
            >>> df.select(F.instr(F.col("text"), "spark"))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "instr", value=substr, name=f"instr({column.name}, {substr})"
        )

    @staticmethod
    def locate(
        substr: str, column: Union[Column, str], pos: int = 1
    ) -> ColumnOperation:
        """Locate the position of substr starting from pos (1-indexed).

        Args:
            substr: Substring to locate
            column: Input string column
            pos: Starting position (default 1)

        Returns:
            ColumnOperation representing locate

        Example:
            >>> df.select(F.locate("spark", F.col("text"), 1))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "locate",
            value=(substr, pos),
            name=f"locate({substr}, {column.name}, {pos})",
        )

    @staticmethod
    def lpad(column: Union[Column, str], len: int, pad: str) -> ColumnOperation:
        """Left-pad string column to length len with pad string.

        Args:
            column: Input string column
            len: Target length
            pad: Padding string

        Returns:
            ColumnOperation representing lpad

        Example:
            >>> df.select(F.lpad(F.col("id"), 5, "0"))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "lpad", value=(len, pad), name=f"lpad({column.name}, {len}, {pad})"
        )

    @staticmethod
    def rpad(column: Union[Column, str], len: int, pad: str) -> ColumnOperation:
        """Right-pad string column to length len with pad string.

        Args:
            column: Input string column
            len: Target length
            pad: Padding string

        Returns:
            ColumnOperation representing rpad

        Example:
            >>> df.select(F.rpad(F.col("id"), 5, "0"))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "rpad", value=(len, pad), name=f"rpad({column.name}, {len}, {pad})"
        )

    @staticmethod
    def levenshtein(
        left: Union[Column, str], right: Union[Column, str]
    ) -> ColumnOperation:
        """Compute Levenshtein distance between two strings.

        Args:
            left: First string column
            right: Second string column

        Returns:
            ColumnOperation representing levenshtein

        Example:
            >>> df.select(F.levenshtein(F.col("word1"), F.col("word2")))
        """
        if isinstance(left, str):
            left = Column(left)
        if isinstance(right, str):
            right = Column(right)

        return ColumnOperation(
            left,
            "levenshtein",
            value=right,
            name=f"levenshtein({left.name}, {right.name})",
        )

    @staticmethod
    def overlay(
        src: Union[Column, str],
        replace: Union[Column, str],
        pos: Union[Column, int],
        len: Union[Column, int] = -1,
    ) -> ColumnOperation:
        """Replace part of a string with another string starting at a position (PySpark 3.0+).

        Args:
            src: Source string column
            replace: Replacement string
            pos: Starting position (1-indexed)
            len: Length to replace (default -1 means to end of string)

        Returns:
            ColumnOperation for overlay operation

        Example:
            >>> df.select(F.overlay(F.col("text"), F.lit("NEW"), F.lit(5), F.lit(3)))
        """
        if isinstance(src, str):
            src = Column(src)

        # Generate proper name with all arguments
        replace_str = (
            replace.name
            if isinstance(replace, Column)
            else (replace.value if hasattr(replace, "value") else str(replace))
        )
        pos_str = (
            pos.name
            if isinstance(pos, Column)
            else (pos.value if hasattr(pos, "value") else str(pos))
        )
        len_str = (
            len.name
            if isinstance(len, Column)
            else (len.value if hasattr(len, "value") else str(len))
        )
        name = f"overlay({src.name}, {replace_str}, {pos_str}, {len_str})"

        return ColumnOperation(src, "overlay", value=(replace, pos, len), name=name)

    @staticmethod
    def bin(column: Union[Column, str]) -> ColumnOperation:
        """Convert to binary string representation.

        Args:
            column: Numeric column

        Returns:
            ColumnOperation representing bin
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "bin", name=f"bin({column.name})")

    @staticmethod
    def hex(column: Union[Column, str]) -> ColumnOperation:
        """Convert to hexadecimal string.

        Args:
            column: Column to convert

        Returns:
            ColumnOperation representing hex
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "hex", name=f"hex({column.name})")

    @staticmethod
    def unhex(column: Union[Column, str]) -> ColumnOperation:
        """Convert hex string to binary.

        Args:
            column: Hex string column

        Returns:
            ColumnOperation representing unhex
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "unhex", name=f"unhex({column.name})")

    @staticmethod
    def hash(*cols: Union[Column, str]) -> ColumnOperation:
        """Compute hash value of given columns.

        Args:
            *cols: Columns to hash

        Returns:
            ColumnOperation representing hash
        """
        columns = []
        for col in cols:
            if isinstance(col, str):
                columns.append(Column(col))
            else:
                columns.append(col)

        # Generate proper name with column names
        if len(columns) == 1:
            col_name = (
                columns[0].name if hasattr(columns[0], "name") else str(columns[0])
            )
            name = f"hash({col_name})"
        else:
            col_names = ", ".join(
                c.name if hasattr(c, "name") else str(c) for c in columns
            )
            name = f"hash({col_names})"

        return ColumnOperation(
            columns[0] if columns else Column(""),
            "hash",
            value=columns[1:] if len(columns) > 1 else [],
            name=name,
        )

    @staticmethod
    def xxhash64(*cols: Union[Column, str]) -> ColumnOperation:
        """Compute xxHash64 value of given columns (all PySpark versions).

        Args:
            *cols: Columns to hash

        Returns:
            ColumnOperation representing xxhash64
        """
        columns = []
        for col in cols:
            if isinstance(col, str):
                columns.append(Column(col))
            else:
                columns.append(col)

        if len(columns) == 1:
            col_name = (
                columns[0].name if hasattr(columns[0], "name") else str(columns[0])
            )
            name = f"xxhash64({col_name})"
        else:
            col_names = ", ".join(
                c.name if hasattr(c, "name") else str(c) for c in columns
            )
            name = f"xxhash64({col_names})"

        return ColumnOperation(
            columns[0] if columns else Column(""),
            "xxhash64",
            value=columns[1:] if len(columns) > 1 else [],
            name=name,
        )

    @staticmethod
    def encode(column: Union[Column, str], charset: str) -> ColumnOperation:
        """Encode string to binary using charset.

        Args:
            column: String column
            charset: Character set (e.g., 'UTF-8')

        Returns:
            ColumnOperation representing encode
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "encode", value=charset, name=f"encode({column.name}, {charset})"
        )

    @staticmethod
    def decode(column: Union[Column, str], charset: str) -> ColumnOperation:
        """Decode binary to string using charset.

        Args:
            column: Binary column
            charset: Character set (e.g., 'UTF-8')

        Returns:
            ColumnOperation representing decode
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "decode", value=charset, name=f"decode({column.name}, {charset})"
        )

    @staticmethod
    def conv(
        column: Union[Column, str], from_base: int, to_base: int
    ) -> ColumnOperation:
        """Convert number from one base to another.

        Args:
            column: Number column
            from_base: Source base (2-36)
            to_base: Target base (2-36)

        Returns:
            ColumnOperation representing conv
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column,
            "conv",
            value=(from_base, to_base),
            name=f"conv({column.name}, {from_base}, {to_base})",
        )

    @staticmethod
    def md5(column: Union[Column, str]) -> ColumnOperation:
        """Calculate MD5 hash of string (PySpark 3.0+).

        Args:
            column: String column to hash

        Returns:
            ColumnOperation representing md5 function (returns 32-char hex string)

        Example:
            >>> df.select(F.md5(F.col("text")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "md5", name=f"md5({column.name})")

    @staticmethod
    def sha1(column: Union[Column, str]) -> ColumnOperation:
        """Calculate SHA-1 hash of string (PySpark 3.0+).

        Args:
            column: String column to hash

        Returns:
            ColumnOperation representing sha1 function (returns 40-char hex string)

        Example:
            >>> df.select(F.sha1(F.col("text")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "sha1", name=f"sha1({column.name})")

    @staticmethod
    def sha2(column: Union[Column, str], numBits: int) -> ColumnOperation:
        """Calculate SHA-2 family hash (PySpark 3.0+).

        Args:
            column: String column to hash
            numBits: Bit length - 224, 256, 384, or 512

        Returns:
            ColumnOperation representing sha2 function (returns hex string)

        Example:
            >>> df.select(F.sha2(F.col("text"), 256))
        """
        if isinstance(column, str):
            column = Column(column)

        if numBits not in [224, 256, 384, 512]:
            raise ValueError(f"numBits must be 224, 256, 384, or 512, got {numBits}")

        return ColumnOperation(
            column, "sha2", value=numBits, name=f"sha2({column.name}, {numBits})"
        )

    @staticmethod
    def crc32(column: Union[Column, str]) -> ColumnOperation:
        """Calculate CRC32 checksum (PySpark 3.0+).

        Args:
            column: String column to checksum

        Returns:
            ColumnOperation representing crc32 function (returns signed 32-bit int)

        Example:
            >>> df.select(F.crc32(F.col("text")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "crc32", name=f"crc32({column.name})")

    @staticmethod
    def to_str(column: Union[Column, str]) -> ColumnOperation:
        """Convert column to string representation (all PySpark versions).

        Args:
            column: Column to convert to string

        Returns:
            Column operation for string conversion

        Example:
            >>> df.select(F.to_str(F.col("value")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "to_str", name=f"to_str({column.name})")

    @staticmethod
    def ilike(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """Case-insensitive LIKE pattern matching.

        Args:
            column: The column to match against.
            pattern: The pattern to match (SQL LIKE pattern).

        Returns:
            ColumnOperation representing the ilike function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "ilike", pattern, name=f"ilike({column.name}, '{pattern}')"
        )
        return operation

    @staticmethod
    def find_in_set(
        column: Union[Column, str], str_list: Union[Column, str]
    ) -> ColumnOperation:
        """Find position of value in comma-separated string list.

        Args:
            column: The value to find.
            str_list: The comma-separated string list.

        Returns:
            ColumnOperation representing the find_in_set function.
        """
        if isinstance(column, str):
            column = Column(column)
        if isinstance(str_list, str):
            str_list = Column(str_list)

        operation = ColumnOperation(
            column,
            "find_in_set",
            str_list,
            name=f"find_in_set({column.name}, {str_list.name if hasattr(str_list, 'name') else str_list})",
        )
        return operation

    @staticmethod
    def regexp_count(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """Count occurrences of regex pattern in string.

        Args:
            column: The column to search in.
            pattern: The regex pattern to count.

        Returns:
            ColumnOperation representing the regexp_count function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "regexp_count",
            pattern,
            name=f"regexp_count({column.name}, '{pattern}')",
        )
        return operation

    @staticmethod
    def regexp_like(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """Regex pattern matching (similar to rlike).

        Args:
            column: The column to match against.
            pattern: The regex pattern to match.

        Returns:
            ColumnOperation representing the regexp_like function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "regexp_like",
            pattern,
            name=f"regexp_like({column.name}, '{pattern}')",
        )
        return operation

    @staticmethod
    def regexp_substr(
        column: Union[Column, str], pattern: str, pos: int = 1, occurrence: int = 1
    ) -> ColumnOperation:
        """Extract substring matching regex pattern.

        Args:
            column: The column to extract from.
            pattern: The regex pattern to match.
            pos: Starting position (1-indexed).
            occurrence: Which occurrence to extract.

        Returns:
            ColumnOperation representing the regexp_substr function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "regexp_substr",
            (pattern, pos, occurrence),
            name=f"regexp_substr({column.name}, '{pattern}', {pos}, {occurrence})",
        )
        return operation

    @staticmethod
    def regexp_instr(
        column: Union[Column, str], pattern: str, pos: int = 1, occurrence: int = 1
    ) -> ColumnOperation:
        """Find position of regex pattern match.

        Args:
            column: The column to search in.
            pattern: The regex pattern to find.
            pos: Starting position (1-indexed).
            occurrence: Which occurrence to find.

        Returns:
            ColumnOperation representing the regexp_instr function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "regexp_instr",
            (pattern, pos, occurrence),
            name=f"regexp_instr({column.name}, '{pattern}', {pos}, {occurrence})",
        )
        return operation

    @staticmethod
    def regexp(column: Union[Column, str], pattern: str) -> ColumnOperation:
        """Alias for rlike - regex pattern matching.

        Args:
            column: The column to match against.
            pattern: The regex pattern to match.

        Returns:
            ColumnOperation representing the regexp function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "regexp", pattern, name=f"regexp({column.name}, '{pattern}')"
        )
        return operation

    @staticmethod
    def sentences(
        column: Union[Column, str],
        language: Optional[str] = None,
        country: Optional[str] = None,
    ) -> ColumnOperation:
        """Split text into sentences.

        Args:
            column: The column containing text.
            language: Language code (optional).
            country: Country code (optional).

        Returns:
            ColumnOperation representing the sentences function.
        """
        if isinstance(column, str):
            column = Column(column)

        # Store language and country as tuple if provided
        value = None
        if language is not None or country is not None:
            value = (language, country)

        operation = ColumnOperation(
            column, "sentences", value, name=f"sentences({column.name})"
        )
        return operation

    @staticmethod
    def printf(format_str: str, *columns: Union[Column, str]) -> ColumnOperation:
        """Formatted string (like sprintf).

        Args:
            format_str: Format string with placeholders.
            *columns: Columns to format.

        Returns:
            ColumnOperation representing the printf function.
        """
        # Convert string columns to Column objects
        col_list = [Column(col) if isinstance(col, str) else col for col in columns]

        # Use first column as base, store format and other columns as value
        if not col_list:
            raise ValueError("printf requires at least one column argument")

        operation = ColumnOperation(
            col_list[0],
            "printf",
            (format_str, *col_list[1:]),
            name=f"printf('{format_str}', ...)",
        )
        return operation

    @staticmethod
    def to_char(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert number/date to character string.

        Args:
            column: The column to convert.
            format: Optional format string.

        Returns:
            ColumnOperation representing the to_char function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "to_char", format, name=f"to_char({column.name})"
        )
        return operation

    @staticmethod
    def to_varchar(
        column: Union[Column, str], length: Optional[int] = None
    ) -> ColumnOperation:
        """Convert to varchar type.

        Args:
            column: The column to convert.
            length: Optional length for varchar.

        Returns:
            ColumnOperation representing the to_varchar function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "to_varchar", length, name=f"to_varchar({column.name})"
        )
        return operation

    @staticmethod
    def typeof(column: Union[Column, str]) -> ColumnOperation:
        """Get type of value as string.

        Args:
            column: The column to get type of.

        Returns:
            ColumnOperation representing the typeof function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "typeof", name=f"typeof({column.name})")
        return operation

    @staticmethod
    def stack(n: int, *cols: Union[Column, str, Any]) -> ColumnOperation:
        """Stack multiple columns into rows.

        Args:
            n: Number of rows to create per input row.
            *cols: Columns to stack.

        Returns:
            ColumnOperation representing the stack function.
        """
        from sparkless.functions.base import Column

        col_list = [Column(col) if isinstance(col, str) else col for col in cols]

        if not col_list:
            raise ValueError("stack requires at least one column argument")

        # Use first column as base, store n and other columns as value
        operation = ColumnOperation(
            col_list[0], "stack", (n, *col_list[1:]), name=f"stack({n}, ...)"
        )
        return operation

    @staticmethod
    def sha(column: Union[Column, str]) -> ColumnOperation:
        """Alias for sha1 - Calculate SHA-1 hash of string (PySpark 3.5+).

        Args:
            column: String column to hash.

        Returns:
            ColumnOperation representing sha function (returns 40-char hex string).

        Example:
            >>> df.select(F.sha(F.col("text")))
        """
        return StringFunctions.sha1(column)

    @staticmethod
    def mask(
        column: Union[Column, str],
        upperChar: Optional[str] = None,
        lowerChar: Optional[str] = None,
        digitChar: Optional[str] = None,
        otherChar: Optional[str] = None,
    ) -> ColumnOperation:
        """Mask sensitive data in a string (PySpark 3.5+).

        Args:
            column: String column to mask.
            upperChar: Character to use for uppercase letters (default: 'X').
            lowerChar: Character to use for lowercase letters (default: 'x').
            digitChar: Character to use for digits (default: 'n').
            otherChar: Character to use for other characters (default: '-').

        Returns:
            ColumnOperation representing the mask function.

        Example:
            >>> df.select(F.mask(F.col("email"), upperChar='U', lowerChar='l', digitChar='d'))
        """
        if isinstance(column, str):
            column = Column(column)

        params = {}
        if upperChar is not None:
            params["upperChar"] = upperChar
        if lowerChar is not None:
            params["lowerChar"] = lowerChar
        if digitChar is not None:
            params["digitChar"] = digitChar
        if otherChar is not None:
            params["otherChar"] = otherChar

        param_str = ", ".join([f"{k}='{v}'" for k, v in params.items()])
        name = f"mask({column.name}" + (", " + param_str if param_str else "") + ")"

        operation = ColumnOperation(
            column,
            "mask",
            value=params if params else None,
            name=name,
        )
        return operation

    @staticmethod
    def json_array_length(
        column: Union[Column, str], path: Optional[str] = None
    ) -> ColumnOperation:
        """Get the length of a JSON array (PySpark 3.5+).

        Args:
            column: JSON column to get array length from.
            path: Optional JSONPath expression to specify array location.

        Returns:
            ColumnOperation representing the json_array_length function.

        Example:
            >>> df.select(F.json_array_length(F.col("json_col"), "$.array"))
        """
        if isinstance(column, str):
            column = Column(column)

        name = (
            f"json_array_length({column.name}" + (f", '{path}'" if path else "") + ")"
        )
        operation = ColumnOperation(column, "json_array_length", value=path, name=name)
        return operation

    @staticmethod
    def json_object_keys(
        column: Union[Column, str], path: Optional[str] = None
    ) -> ColumnOperation:
        """Get the keys of a JSON object (PySpark 3.5+).

        Args:
            column: JSON column to get object keys from.
            path: Optional JSONPath expression to specify object location.

        Returns:
            ColumnOperation representing the json_object_keys function.

        Example:
            >>> df.select(F.json_object_keys(F.col("json_col"), "$.object"))
        """
        if isinstance(column, str):
            column = Column(column)

        name = f"json_object_keys({column.name}" + (f", '{path}'" if path else "") + ")"
        operation = ColumnOperation(column, "json_object_keys", value=path, name=name)
        return operation

    @staticmethod
    def xpath_number(column: Union[Column, str], path: str) -> ColumnOperation:
        """Extract number from XML using XPath (PySpark 3.5+).

        Args:
            column: XML column to extract from.
            path: XPath expression.

        Returns:
            ColumnOperation representing the xpath_number function.

        Example:
            >>> df.select(F.xpath_number(F.col("xml_col"), "/root/value"))
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "xpath_number",
            value=path,
            name=f"xpath_number({column.name}, '{path}')",
        )
        return operation

    @staticmethod
    def user() -> ColumnOperation:
        """Get current user name (PySpark 3.5+).

        Returns:
            ColumnOperation representing the user function.

        Example:
            >>> df.select(F.user())
        """
        operation = ColumnOperation(None, "user", name="user()")
        return operation
