"""
Type mapper for converting Sparkless types to Polars dtypes.

This module provides functions to convert Sparkless data types
to Polars data types for DataFrame operations.
"""

import polars as pl
from sparkless.spark_types import (
    DataType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    DateType,
    TimestampType,
    TimestampNTZType,
    DecimalType,
    BinaryType,
    ArrayType,
    MapType,
    StructType,
    ShortType,
    ByteType,
    NullType,
)


def mock_type_to_polars_dtype(mock_type: DataType) -> pl.DataType:
    """Convert Sparkless type to Polars dtype.

    Args:
        mock_type: Sparkless data type

    Returns:
        Polars data type

    Raises:
        ValueError: If the type is not supported
    """
    if isinstance(mock_type, StringType):
        return pl.Utf8
    elif isinstance(mock_type, IntegerType):
        return pl.Int32
    elif isinstance(mock_type, LongType):
        return pl.Int64
    elif isinstance(mock_type, DoubleType):
        return pl.Float64
    elif isinstance(mock_type, FloatType):
        return pl.Float32
    elif isinstance(mock_type, BooleanType):
        return pl.Boolean
    elif isinstance(mock_type, DateType):
        return pl.Date
    elif isinstance(mock_type, TimestampType):
        return pl.Datetime(time_unit="us")  # Polars uses microseconds
    elif isinstance(mock_type, TimestampNTZType):
        return pl.Datetime(time_unit="us", time_zone=None)
    elif isinstance(mock_type, DecimalType):
        # Polars doesn't have exact decimal type, use Float64
        return pl.Float64
    elif isinstance(mock_type, BinaryType):
        return pl.Binary
    elif isinstance(mock_type, ArrayType):
        element_type = mock_type_to_polars_dtype(mock_type.element_type)
        return pl.List(element_type)
    elif isinstance(mock_type, MapType):
        key_type = mock_type_to_polars_dtype(mock_type.key_type)
        value_type = mock_type_to_polars_dtype(mock_type.value_type)
        return pl.Struct(
            [
                pl.Field("key", key_type),
                pl.Field("value", value_type),
            ]
        )
    elif isinstance(mock_type, StructType):
        # Convert struct fields to Polars struct
        fields = []
        for field in mock_type.fields:
            field_type = mock_type_to_polars_dtype(field.dataType)
            fields.append(pl.Field(field.name, field_type))
        return pl.Struct(fields)
    elif isinstance(mock_type, ShortType):
        return pl.Int16
    elif isinstance(mock_type, ByteType):
        return pl.Int8
    elif isinstance(mock_type, NullType):
        return pl.Null
    else:
        raise ValueError(f"Unsupported Sparkless type: {type(mock_type)}")


def polars_dtype_to_mock_type(polars_dtype: pl.DataType) -> DataType:
    """Convert Polars dtype to Sparkless type.

    Args:
        polars_dtype: Polars data type

    Returns:
        Sparkless data type

    Raises:
        ValueError: If the type is not supported
    """
    if polars_dtype == pl.Utf8:
        return StringType()
    elif polars_dtype == pl.Int32:
        return IntegerType()
    elif polars_dtype == pl.Int64:
        return LongType()
    elif polars_dtype == pl.Float64:
        return DoubleType()
    elif polars_dtype == pl.Float32:
        return FloatType()
    elif polars_dtype == pl.Boolean:
        return BooleanType()
    elif polars_dtype == pl.Date:
        return DateType()
    elif isinstance(polars_dtype, pl.Datetime):
        if polars_dtype.time_zone is None:
            return TimestampNTZType()
        return TimestampType()
    elif polars_dtype == pl.Binary:
        return BinaryType()
    elif isinstance(polars_dtype, pl.List):
        element_type = polars_dtype_to_mock_type(polars_dtype.inner)
        return ArrayType(element_type=element_type)
    elif isinstance(polars_dtype, pl.Struct):
        # Convert struct to StructType
        from sparkless.spark_types import StructField

        fields = []
        for field in polars_dtype.fields:
            field_type = polars_dtype_to_mock_type(field.dtype)
            fields.append(StructField(field.name, field_type))
        return StructType(fields)
    elif polars_dtype == pl.Object:
        # Object dtype is used for Python objects (dicts, etc.)
        # For withField results, these are structs, so return StructType
        # We can't infer the exact structure, so return an empty StructType
        # The actual data will be preserved as Python dicts
        return StructType([])
    elif polars_dtype == pl.Int16:
        return ShortType()
    elif polars_dtype == pl.Int8:
        return ByteType()
    elif polars_dtype == pl.UInt32:
        # Polars UInt32 from list.len() - convert to IntegerType for PySpark compatibility
        return IntegerType()
    elif polars_dtype == pl.UInt64:
        # Polars UInt64 - convert to LongType for PySpark compatibility
        return LongType()
    elif polars_dtype == pl.UInt16:
        # Polars UInt16 - convert to ShortType for PySpark compatibility
        return ShortType()
    elif polars_dtype == pl.UInt8:
        # Polars UInt8 - convert to ByteType for PySpark compatibility
        return ByteType()
    elif polars_dtype == pl.Null:
        return NullType()
    else:
        raise ValueError(f"Unsupported Polars dtype: {polars_dtype}")
