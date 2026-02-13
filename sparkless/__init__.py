"""
Sparkless - A lightweight mock implementation of PySpark for testing and development.

This package re-exports Robin (robin-sparkless) as the execution engine. Use
`from sparkless.sql import SparkSession, DataFrame, functions as F` for the
PySpark-compatible API.

Quick Start:
    >>> from sparkless.sql import SparkSession, functions as F
    >>> spark = SparkSession.builder().appName("MyApp").getOrCreate()
    >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(F.upper(F.col("name"))).show()

Author: Odos Matthews
"""

import sys
from types import ModuleType

# Re-export from sparkless.sql (Robin-backed)
from .sql import (  # noqa: E402
    SparkSession,
    DataFrame,
    DataFrameWriter,
    GroupedData,
    Column,
    ColumnOperation,
    F,
    Functions,
    Window,
    WindowSpec,
)

# SparkContext / JVMContext (minimal compatibility; no Robin equivalent)
from .session.context import SparkContext, JVMContext  # noqa: E402

from . import compat  # noqa: E402

# Delta: minimal stubs for import compatibility (Robin may provide Delta support separately)
class DeltaTable:
    """Stub for Delta Lake compatibility. Use Robin's read_delta / Delta support when available."""

    @staticmethod
    def forName(spark: object, table_name: str) -> "DeltaTable":
        raise NotImplementedError("DeltaTable.forName is not implemented; use Robin's Delta support")

    def toDF(self: object) -> object:
        raise NotImplementedError("DeltaTable.toDF is not implemented")


class DeltaMergeBuilder:
    """Stub for Delta MERGE compatibility."""

    def execute(self: object) -> None:
        pass  # No-op


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

__version__ = "4.0.0"
__author__ = "Odos Matthews"
__email__ = "odosmatthews@gmail.com"

__all__ = [
    "__version__",
    "SparkSession",
    "SparkContext",
    "JVMContext",
    "DataFrame",
    "DataFrameWriter",
    "GroupedData",
    "Functions",
    "Column",
    "ColumnOperation",
    "F",
    "Window",
    "WindowSpec",
    "DeltaTable",
    "DeltaMergeBuilder",
    "DataType",
    "StringType",
    "IntegerType",
    "LongType",
    "DoubleType",
    "FloatType",
    "BooleanType",
    "DateType",
    "TimestampType",
    "DecimalType",
    "ArrayType",
    "MapType",
    "StructType",
    "StructField",
    "BinaryType",
    "NullType",
    "ShortType",
    "ByteType",
    "MemoryStorageManager",
    "MockException",
    "AnalysisException",
    "PySparkValueError",
    "PySparkTypeError",
    "PySparkRuntimeError",
    "IllegalArgumentException",
    "compat",
]

# Delta module aliasing for "from delta.tables import DeltaTable"
delta_module = ModuleType("delta")
delta_tables_module = ModuleType("delta.tables")
delta_tables_module.DeltaTable = DeltaTable  # type: ignore[attr-defined]
delta_module.tables = delta_tables_module  # type: ignore[attr-defined]
sys.modules["delta"] = delta_module
sys.modules["delta.tables"] = delta_tables_module

from . import sql  # noqa: E402
sys.modules["sparkless.sql"] = sql
