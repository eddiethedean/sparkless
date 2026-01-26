Data Types
==========

Sparkless provides all PySpark-compatible data types.

Base Types
----------

.. automodule:: sparkless.spark_types
   :members:
   :undoc-members:
   :show-inheritance:

Primitive Types
---------------

The following primitive types are available:

* :class:`StringType` - String data type
* :class:`IntegerType` - 32-bit integer
* :class:`LongType` - 64-bit integer
* :class:`ShortType` - 16-bit integer
* :class:`ByteType` - 8-bit integer
* :class:`DoubleType` - 64-bit floating point
* :class:`FloatType` - 32-bit floating point
* :class:`BooleanType` - Boolean type
* :class:`DateType` - Date type
* :class:`TimestampType` - Timestamp type
* :class:`BinaryType` - Binary data type
* :class:`NullType` - Null type

Complex Types
-------------

* :class:`ArrayType` - Array of elements
* :class:`MapType` - Map/dictionary type
* :class:`StructType` - Struct/row type
* :class:`StructField` - Field in a struct
* :class:`DecimalType` - Decimal/precision numeric type

Usage Examples
--------------

.. code-block:: python

   from sparkless.sql.types import (
       StructType, StructField, StringType, IntegerType,
       ArrayType, MapType, DoubleType
   )

   # Simple schema
   schema = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), False)
   ])

   # Complex schema with arrays and maps
   complex_schema = StructType([
       StructField("id", IntegerType(), False),
       StructField("tags", ArrayType(StringType()), True),
       StructField("metadata", MapType(StringType(), StringType()), True),
       StructField("score", DoubleType(), True)
   ])
