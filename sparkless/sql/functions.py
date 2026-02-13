"""
Sparkless SQL Functions module - PySpark-compatible functions interface.

When using sparkless.sql (Robin backend), this module re-exports Robin's functions
so that F.col(), F.approx_count_distinct(), etc. return Robin Column types and
work with Robin-backed DataFrames (e.g. df.agg(F.approx_count_distinct(...))).

Supports both import styles:
    from sparkless.sql import functions as F
    from sparkless.sql.functions import col, upper, etc.

F.DataFrame is the compat DataFrame class for reduce(F.DataFrame.union, dfs).
"""

from typing import Any, Dict

import robin_sparkless as _robin

# Use Robin's functions so F returns Robin Column types (required for df.agg(), groupBy().agg(), etc.)
F = _robin
Functions = _robin

# PySpark accepts column as str or Column; Robin often expects Column only. Wrappers convert str -> col(name).
def _col_or_col(name: str, col_arg: Any) -> Any:
    """Return col_arg if it is already a Robin Column, else _robin.col(col_arg) for string."""
    if isinstance(col_arg, str):
        return _robin.col(col_arg)
    return col_arg


def _approx_count_distinct(col: Any, rsd: Any = None, **kwargs: Any) -> Any:
    """PySpark-style: accept column name as str or Column; result column name matches PySpark."""
    col_name = col if isinstance(col, str) else None
    col = _col_or_col("approx_count_distinct", col)
    fn = getattr(_robin, "approx_count_distinct", None)
    if fn is None:
        raise AttributeError("robin_sparkless has no approx_count_distinct")
    if rsd is not None:
        expr = fn(col, rsd=rsd, **kwargs)
    else:
        expr = fn(col, **kwargs)
    # PySpark default result column name is approx_count_distinct(column_name)
    if col_name is not None and hasattr(expr, "alias"):
        expr = expr.alias(f"approx_count_distinct({col_name})")
    return expr


# Cache for dynamically accessed attributes
_cached_attrs: Dict[str, object] = {}

# Build __all__ and pre-populate from Robin
__all__ = ["F", "Functions"]
for attr_name in dir(_robin):
    if not attr_name.startswith("_"):
        attr_value = getattr(_robin, attr_name)
        if callable(attr_value) or not attr_name.startswith("_"):
            _cached_attrs[attr_name] = attr_value
            if attr_name not in __all__:
                __all__.append(attr_name)

# Override with PySpark-style wrappers (str column name -> Column)
_cached_attrs["approx_count_distinct"] = _approx_count_distinct


def __getattr__(name: str) -> object:
    """Dynamically provide access to functions from F instance.

    This makes the module behave like PySpark's functions module,
    where all functions are available at module level.
    """
    if name in _cached_attrs:
        return _cached_attrs[name]

    # PySpark compatibility: F.DataFrame for reduce(F.DataFrame.union, dfs)
    if name == "DataFrame":
        from sparkless.sql import DataFrame  # noqa: E402

        _cached_attrs[name] = DataFrame
        return DataFrame

    if hasattr(_robin, name):
        attr_value = getattr(_robin, name)
        _cached_attrs[name] = attr_value
        return attr_value

    # Fallback: Sparkless functions Robin may not expose (e.g. udf for decorator API)
    try:
        import sparkless.functions as _sparkless_functions

        if hasattr(_sparkless_functions, name):
            attr_value = getattr(_sparkless_functions, name)
            _cached_attrs[name] = attr_value
            return attr_value
    except ImportError:
        pass

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
