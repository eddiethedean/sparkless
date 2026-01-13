"""
Bitwise functions for Sparkless (PySpark 3.2+).

This module provides bitwise operations on integer columns.
"""

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from sparkless.functions.base import AggregateFunction

from sparkless.functions.base import Column, ColumnOperation


class BitwiseFunctions:
    """Collection of bitwise manipulation functions."""

    @staticmethod
    def bit_count(column: Union[Column, str]) -> ColumnOperation:
        """Count the number of set bits (population count).

        Args:
            column: Integer column.

        Returns:
            ColumnOperation representing the bit_count function.

        Example:
            >>> df.select(F.bit_count(F.col("value")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(column, "bit_count", name=f"bit_count({column.name})")

    @staticmethod
    def bit_get(column: Union[Column, str], pos: int) -> ColumnOperation:
        """Get bit value at position.

        Args:
            column: Integer column.
            pos: Bit position (0-based, from right).

        Returns:
            ColumnOperation representing the bit_get function.

        Example:
            >>> df.select(F.bit_get(F.col("value"), 0))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "bit_get", pos, name=f"bit_get({column.name}, {pos})"
        )

    @staticmethod
    def getbit(column: Union[Column, str], pos: int) -> ColumnOperation:
        """Get bit value at position (alias for bit_get) (PySpark 3.5+).

        Args:
            column: Integer column.
            pos: Bit position (0-based, from right).

        Returns:
            ColumnOperation representing the getbit function.

        Example:
            >>> df.select(F.getbit(F.col("value"), 0))
        """
        return BitwiseFunctions.bit_get(column, pos)

    @staticmethod
    def bitwise_not(column: Union[Column, str]) -> ColumnOperation:
        """Perform bitwise NOT operation.

        Args:
            column: Integer column.

        Returns:
            ColumnOperation representing the bitwise_not function.

        Example:
            >>> df.select(F.bitwise_not(F.col("value")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "bitwise_not", name=f"bitwise_not({column.name})"
        )

    # Priority 2: Bitwise Aggregate Functions
    @staticmethod
    def bit_and(column: Union[Column, str]) -> "AggregateFunction":
        """Aggregate function - bitwise AND of all values (PySpark 3.5+).

        Args:
            column: Integer column.

        Returns:
            AggregateFunction representing the bit_and aggregate function.

        Example:
            >>> df.groupBy("dept").agg(F.bit_and("flags"))
        """
        from sparkless.functions.base import AggregateFunction
        from sparkless.spark_types import LongType

        return AggregateFunction(column, "bit_and", LongType())

    @staticmethod
    def bit_or(column: Union[Column, str]) -> "AggregateFunction":
        """Aggregate function - bitwise OR of all values (PySpark 3.5+).

        Args:
            column: Integer column.

        Returns:
            AggregateFunction representing the bit_or aggregate function.

        Example:
            >>> df.groupBy("dept").agg(F.bit_or("flags"))
        """
        from sparkless.functions.base import AggregateFunction
        from sparkless.spark_types import LongType

        return AggregateFunction(column, "bit_or", LongType())

    @staticmethod
    def bit_xor(column: Union[Column, str]) -> "AggregateFunction":
        """Aggregate function - bitwise XOR of all values (PySpark 3.5+).

        Args:
            column: Integer column.

        Returns:
            AggregateFunction representing the bit_xor aggregate function.

        Example:
            >>> df.groupBy("dept").agg(F.bit_xor("flags"))
        """
        from sparkless.functions.base import AggregateFunction
        from sparkless.spark_types import LongType

        return AggregateFunction(column, "bit_xor", LongType())

    # Deprecated Aliases
    @staticmethod
    def bitwiseNOT(column: Union[Column, str]) -> ColumnOperation:
        """Deprecated alias for bitwise_not (all PySpark versions).

        Use bitwise_not instead.

        Args:
            column: Integer column.

        Returns:
            ColumnOperation representing bitwise NOT.
        """
        import warnings

        warnings.warn(
            "bitwiseNOT is deprecated. Use bitwise_not instead.",
            FutureWarning,
            stacklevel=2,
        )
        return BitwiseFunctions.bitwise_not(column)

    @staticmethod
    def shiftleft(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Bitwise left shift.

        Args:
            column: Integer column.
            num_bits: Number of bits to shift left.

        Returns:
            ColumnOperation representing the shiftleft function.
        """
        from sparkless.functions.base import Column

        if isinstance(column, str):
            column = Column(column)
        if isinstance(num_bits, (str, int)):
            num_bits = (
                Column(str(num_bits)) if isinstance(num_bits, int) else Column(num_bits)
            )

        operation = ColumnOperation(
            column,
            "shiftleft",
            num_bits,
            name=f"shiftleft({column.name}, {num_bits.name if hasattr(num_bits, 'name') else num_bits})",
        )
        return operation

    @staticmethod
    def shiftright(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Bitwise right shift (signed).

        Args:
            column: Integer column.
            num_bits: Number of bits to shift right.

        Returns:
            ColumnOperation representing the shiftright function.
        """
        from sparkless.functions.base import Column

        if isinstance(column, str):
            column = Column(column)
        if isinstance(num_bits, (str, int)):
            num_bits = (
                Column(str(num_bits)) if isinstance(num_bits, int) else Column(num_bits)
            )

        operation = ColumnOperation(
            column,
            "shiftright",
            num_bits,
            name=f"shiftright({column.name}, {num_bits.name if hasattr(num_bits, 'name') else num_bits})",
        )
        return operation

    @staticmethod
    def shiftrightunsigned(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Bitwise unsigned right shift.

        Args:
            column: Integer column.
            num_bits: Number of bits to shift right.

        Returns:
            ColumnOperation representing the shiftrightunsigned function.
        """
        from sparkless.functions.base import Column

        if isinstance(column, str):
            column = Column(column)
        if isinstance(num_bits, (str, int)):
            num_bits = (
                Column(str(num_bits)) if isinstance(num_bits, int) else Column(num_bits)
            )

        operation = ColumnOperation(
            column,
            "shiftrightunsigned",
            num_bits,
            name=f"shiftrightunsigned({column.name}, {num_bits.name if hasattr(num_bits, 'name') else num_bits})",
        )
        return operation

    # Deprecated camelCase aliases (PySpark 3.0-3.1)
    @staticmethod
    def shiftLeft(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Deprecated alias for shiftleft (PySpark 3.0-3.1).

        Use shiftleft instead.

        Args:
            column: Integer column.
            num_bits: Number of bits to shift left.

        Returns:
            ColumnOperation representing the shiftLeft function.
        """
        import warnings

        warnings.warn(
            "shiftLeft is deprecated. Use shiftleft instead.",
            FutureWarning,
            stacklevel=2,
        )
        return BitwiseFunctions.shiftleft(column, num_bits)

    @staticmethod
    def shiftRight(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Deprecated alias for shiftright (PySpark 3.0-3.1).

        Use shiftright instead.

        Args:
            column: Integer column.
            num_bits: Number of bits to shift right.

        Returns:
            ColumnOperation representing the shiftRight function.
        """
        import warnings

        warnings.warn(
            "shiftRight is deprecated. Use shiftright instead.",
            FutureWarning,
            stacklevel=2,
        )
        return BitwiseFunctions.shiftright(column, num_bits)

    @staticmethod
    def shiftRightUnsigned(
        column: Union[Column, str], num_bits: Union[Column, str, int]
    ) -> ColumnOperation:
        """Deprecated alias for shiftrightunsigned (PySpark 3.0-3.1).

        Use shiftrightunsigned instead.

        Args:
            column: Integer column.
            num_bits: Number of bits to shift right.

        Returns:
            ColumnOperation representing the shiftRightUnsigned function.
        """
        import warnings

        warnings.warn(
            "shiftRightUnsigned is deprecated. Use shiftrightunsigned instead.",
            FutureWarning,
            stacklevel=2,
        )
        return BitwiseFunctions.shiftrightunsigned(column, num_bits)

    # Bitmap Functions (PySpark 3.5+)
    @staticmethod
    def bitmap_bit_position(column: Union[Column, str]) -> ColumnOperation:
        """Get the bit position in a bitmap (PySpark 3.5+).

        Args:
            column: Bitmap column.

        Returns:
            ColumnOperation representing the bitmap_bit_position function.

        Example:
            >>> df.select(F.bitmap_bit_position(F.col("bitmap")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "bitmap_bit_position", name=f"bitmap_bit_position({column.name})"
        )

    @staticmethod
    def bitmap_bucket_number(column: Union[Column, str]) -> ColumnOperation:
        """Get the bucket number in a bitmap (PySpark 3.5+).

        Args:
            column: Bitmap column.

        Returns:
            ColumnOperation representing the bitmap_bucket_number function.

        Example:
            >>> df.select(F.bitmap_bucket_number(F.col("bitmap")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "bitmap_bucket_number", name=f"bitmap_bucket_number({column.name})"
        )

    @staticmethod
    def bitmap_construct_agg(column: Union[Column, str]) -> "AggregateFunction":
        """Aggregate function - construct bitmap from values (PySpark 3.5+).

        Args:
            column: Integer column to construct bitmap from.

        Returns:
            AggregateFunction representing the bitmap_construct_agg function.

        Example:
            >>> df.groupBy("dept").agg(F.bitmap_construct_agg("id"))
        """
        from sparkless.functions.base import AggregateFunction
        from sparkless.spark_types import BinaryType

        return AggregateFunction(column, "bitmap_construct_agg", BinaryType())

    @staticmethod
    def bitmap_count(column: Union[Column, str]) -> ColumnOperation:
        """Count the number of set bits in a bitmap (PySpark 3.5+).

        Args:
            column: Bitmap column.

        Returns:
            ColumnOperation representing the bitmap_count function.

        Example:
            >>> df.select(F.bitmap_count(F.col("bitmap")))
        """
        if isinstance(column, str):
            column = Column(column)

        return ColumnOperation(
            column, "bitmap_count", name=f"bitmap_count({column.name})"
        )

    @staticmethod
    def bitmap_or_agg(column: Union[Column, str]) -> "AggregateFunction":
        """Aggregate function - bitwise OR of bitmaps (PySpark 3.5+).

        Args:
            column: Bitmap column.

        Returns:
            AggregateFunction representing the bitmap_or_agg function.

        Example:
            >>> df.groupBy("dept").agg(F.bitmap_or_agg("bitmap"))
        """
        from sparkless.functions.base import AggregateFunction
        from sparkless.spark_types import BinaryType

        return AggregateFunction(column, "bitmap_or_agg", BinaryType())
