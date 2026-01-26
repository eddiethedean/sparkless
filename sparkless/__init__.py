"""
Sparkless - A lightweight mock implementation of PySpark for testing and development.

This package provides a complete mock implementation of PySpark's core functionality
without requiring a Java Virtual Machine (JVM) or actual Spark installation.

Core Features (PySpark API):
    - Complete PySpark API compatibility
    - No JVM required - pure Python implementation
    - DataFrame operations (select, filter, groupBy, join, etc.)
    - SQL query execution
    - Window functions with proper partitioning and ordering
    - 15+ data types including complex types (Array, Map, Struct)
    - Type-safe operations with automatic schema inference
    - Edge case handling (null values, unicode, large numbers)

Testing Utilities (Optional):
    Additional utilities to make testing easier:
    - Error simulation for testing error handling
    - Performance simulation for testing edge cases
    - Test data generation with realistic patterns

    Import explicitly when needed:
        from sparkless.error_simulation import MockErrorSimulator
        from sparkless.performance_simulation import MockPerformanceSimulator
        from sparkless.data_generation import create_test_data

    See docs/testing_utilities_guide.md for details.

Quick Start:
    >>> from sparkless.sql import SparkSession, functions as F
    >>> spark = SparkSession("MyApp")
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.upper(F.col("name"))).show()
    DataFrame[2 rows, 1 columns]

    upper(name)
    ALICE
    BOB

Author: Odos Matthews
"""

import sys
from types import ModuleType

from .session import SparkSession  # noqa: E402
from .session.context import SparkContext, JVMContext  # noqa: E402
from .dataframe import DataFrame, DataFrameWriter, GroupedData  # noqa: E402
from .functions import Functions, Column, ColumnOperation, F  # noqa: E402
from . import compat  # noqa: E402
from .window import Window, WindowSpec  # noqa: E402
from .delta import DeltaTable, DeltaMergeBuilder  # noqa: E402
from .spark_types import (  # noqa: E402
    DataType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    ArrayType,
    MapType,
    BinaryType,
    NullType,
    FloatType,
    ShortType,
    ByteType,
    StructType,
    StructField,
)
from sparkless.storage import MemoryStorageManager  # noqa: E402
from .errors import (  # noqa: E402
    MockException,
    AnalysisException,
    PySparkValueError,
    PySparkTypeError,
    PySparkRuntimeError,
    IllegalArgumentException,
)

# Note: Exceptions are also available via sparkless.sql.utils for PySpark compatibility:
#   from sparkless.sql.utils import AnalysisException  # PySpark-compatible path
#   from sparkless import AnalysisException  # Backward-compatible path (still works)

# ==============================================================================
# TESTING UTILITIES - AVAILABLE VIA EXPLICIT IMPORT
# ==============================================================================
# These utilities are NOT imported here to keep the main namespace clean.
# Import them explicitly when needed:
#
#   from sparkless.error_simulation import MockErrorSimulator
#   from sparkless.performance_simulation import MockPerformanceSimulator
#   from sparkless.data_generation import create_test_data
#
# Available modules:
#   - sparkless.error_simulation - Error injection for testing
#   - sparkless.performance_simulation - Performance testing utilities
#   - sparkless.data_generation - Test data generation
# ==============================================================================

__version__ = "3.27.1"
__author__ = "Odos Matthews"
__email__ = "odosmatthews@gmail.com"

# ==============================================================================
# MAIN EXPORTS - CORE PYSPARK API
# ==============================================================================
# These are the primary exports that mirror PySpark's API.
# Use these for mocking PySpark in your tests.

__all__ = [
    # -------------------------------------------------------------------------
    # Package Metadata
    # -------------------------------------------------------------------------
    "__version__",  # Package version
    # -------------------------------------------------------------------------
    # Session & Context (Core PySpark API)
    # -------------------------------------------------------------------------
    "SparkSession",  # Main entry point - like pyspark.sql.SparkSession
    "SparkContext",  # Spark context - like pyspark.SparkContext
    "JVMContext",  # JVM context compatibility
    # -------------------------------------------------------------------------
    # DataFrame & Operations (Core PySpark API)
    # -------------------------------------------------------------------------
    "DataFrame",  # DataFrame - like pyspark.sql.DataFrame
    "DataFrameWriter",  # Writer - like pyspark.sql.DataFrameWriter
    "GroupedData",  # Grouped data - like pyspark.sql.GroupedData
    # -------------------------------------------------------------------------
    # Functions & Columns (Core PySpark API)
    # -------------------------------------------------------------------------
    "Functions",  # Functions module
    "Column",  # Column - like pyspark.sql.Column
    "ColumnOperation",  # Column operations
    "F",  # Functions shorthand - like pyspark.sql.functions
    # -------------------------------------------------------------------------
    # Window Functions (Core PySpark API)
    # -------------------------------------------------------------------------
    "Window",  # Window - like pyspark.sql.Window
    "WindowSpec",  # Window spec - like pyspark.sql.WindowSpec
    # -------------------------------------------------------------------------
    # Delta Lake (Simple Support - Mock Operations)
    # -------------------------------------------------------------------------
    "DeltaTable",  # Basic Delta table wrapper
    "DeltaMergeBuilder",  # Delta MERGE builder (mock)
    # -------------------------------------------------------------------------
    # Data Types (Core PySpark API)
    # -------------------------------------------------------------------------
    "DataType",  # Base data type
    "StringType",  # String type
    "IntegerType",  # Integer type
    "LongType",  # Long type
    "DoubleType",  # Double type
    "FloatType",  # Float type
    "BooleanType",  # Boolean type
    "DateType",  # Date type
    "TimestampType",  # Timestamp type
    "DecimalType",  # Decimal type
    "ArrayType",  # Array type
    "MapType",  # Map type
    "StructType",  # Struct type
    "StructField",  # Struct field
    "BinaryType",  # Binary type
    "NullType",  # Null type
    "ShortType",  # Short type
    "ByteType",  # Byte type
    # -------------------------------------------------------------------------
    # Storage (Core Infrastructure)
    # -------------------------------------------------------------------------
    "MemoryStorageManager",  # Storage backend
    # -------------------------------------------------------------------------
    # Exceptions (PySpark-compatible)
    # -------------------------------------------------------------------------
    "MockException",  # Base exception
    "AnalysisException",  # Analysis exception - like pyspark.sql.utils.AnalysisException
    "PySparkValueError",  # Value error
    "PySparkTypeError",  # Type error
    "PySparkRuntimeError",  # Runtime error
    "IllegalArgumentException",  # Illegal argument exception
    # -------------------------------------------------------------------------
    # Compatibility helpers
    # -------------------------------------------------------------------------
    "compat",
]

# ==============================================================================
# DELTA MODULE ALIASING - Support "from delta.tables import DeltaTable"
# ==============================================================================
# This allows sparkless to be used as a drop-in replacement for delta-spark
# in tests that import DeltaTable from delta.tables

# Create delta module and delta.tables submodule
delta_module = ModuleType("delta")
delta_tables_module = ModuleType("delta.tables")

# Export DeltaTable as the main class
delta_tables_module.DeltaTable = DeltaTable  # type: ignore[attr-defined]

# Set up module hierarchy
delta_module.tables = delta_tables_module  # type: ignore[attr-defined]

# Register modules in sys.modules
sys.modules["delta"] = delta_module
sys.modules["delta.tables"] = delta_tables_module

# ==============================================================================
# SQL MODULE - Support "from sparkless.sql import ..."
# ==============================================================================
# This allows sparkless to be used as a drop-in replacement for pyspark
# in tests that import from pyspark.sql

from . import sql  # noqa: E402

# Register sql module in sys.modules
sys.modules["sparkless.sql"] = sql
