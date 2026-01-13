"""
Sparkless SQL Functions module - PySpark-compatible functions interface.

This module provides access to all PySpark functions, mirroring pyspark.sql.functions.

In PySpark, you can import functions in two ways:
    1. from pyspark.sql import functions as F
    2. from pyspark.sql.functions import col, upper, etc.

This module supports both patterns and behaves as a proper module (not a class),
matching PySpark's behavior exactly.

Example:
    >>> from sparkless.sql import functions as F
    >>> from sparkless.sql import SparkSession
    >>> spark = SparkSession("test")
    >>> df = spark.createDataFrame([{"name": "Alice", "age": 25}])
    >>> df.select(F.upper(F.col("name")), F.col("age") * 2).show()

    >>> from sparkless.sql.functions import col, upper
    >>> df.select(upper(col("name"))).show()

    >>> from types import ModuleType
    >>> from sparkless.sql import functions
    >>> isinstance(functions, ModuleType)  # True - matches PySpark
"""

import warnings
from typing import Any, Dict

# Import F and Functions for backward compatibility
from ..functions import F, Functions  # noqa: E402

# Cache for dynamically accessed attributes
_cached_attrs: Dict[str, object] = {}

# Build __all__ list with all public functions from F
__all__ = ["F", "Functions"]

# Pre-populate module-level attributes for all public functions
# This allows: from sparkless.sql.functions import col, upper, etc.
for attr_name in dir(F):
    if not attr_name.startswith("_"):
        attr_value = getattr(F, attr_name)
        # Only expose callable attributes (functions) and non-private attributes
        if callable(attr_value) or not attr_name.startswith("_"):
            _cached_attrs[attr_name] = attr_value
            if attr_name not in __all__:
                __all__.append(attr_name)


def __getattr__(name: str) -> object:
    """Dynamically provide access to functions from F instance.

    This makes the module behave like PySpark's functions module,
    where all functions are available at module level.
    """
    if name in _cached_attrs:
        return _cached_attrs[name]

    # Try to get from F instance
    if hasattr(F, name):
        attr_value = getattr(F, name)
        _cached_attrs[name] = attr_value
        return attr_value

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# Deprecation warning for Functions() instantiation
# Store original __init__ if it exists
if hasattr(Functions, "__init__"):
    _original_functions_init = Functions.__init__

    def _deprecated_functions_init(self: Any, *args: Any, **kwargs: Any) -> Any:
        """Warn when Functions() is instantiated directly."""
        warnings.warn(
            "Functions() is deprecated. Use 'from sparkless.sql import functions as F' instead. "
            "This matches PySpark where functions is a module, not a class. "
            "Migration: Replace 'Functions()' with 'from sparkless.sql import functions as F'. "
            "Example: Instead of 'f = Functions(); f.col(\"name\")', use 'from sparkless.sql import functions as F; F.col(\"name\")'.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _original_functions_init(self, *args, **kwargs)

    # Monkey-patch Functions.__init__ to add deprecation warning
    Functions.__init__ = _deprecated_functions_init  # type: ignore[method-assign]
