"""
Phase 3: Robin-backed functions module.

Re-exports PyO3 functions from sparkless_robin with *args wrappers for
concat, coalesce, format_string, greatest, least, etc.
"""

from __future__ import annotations

from typing import Any


def _get_robin() -> Any:
    try:
        import sparkless_robin as _r  # type: ignore[import-untyped]
        return _r
    except ImportError as e:
        raise ImportError(
            "sparkless_robin native extension is not available. "
            "Build with: maturin develop"
        ) from e


def _robin_functions_module() -> Any:
    """Lazy module-like object that exposes Robin functions with *args wrappers."""
    _r = _get_robin()

    # Wrap variadic functions to accept *args
    def _concat(*cols: Any) -> Any:
        return _r.concat(list(cols))

    def _concat_ws(separator: str, *cols: Any) -> Any:
        return _r.concat_ws(separator, list(cols))

    def _format_string(fmt: str, *cols: Any) -> Any:
        return _r.format_string(fmt, list(cols))

    def _coalesce(*cols: Any) -> Any:
        return _r.coalesce(list(cols))

    def _greatest(*cols: Any) -> Any:
        return _r.greatest(list(cols))

    def _least(*cols: Any) -> Any:
        return _r.least(list(cols))

    # Build a namespace with all Robin functions
    class RobinFunctions:
        # Column
        col = staticmethod(_r.col)
        lit = staticmethod(_r.lit)

        # String (variadic)
        concat = staticmethod(_concat)
        concat_ws = staticmethod(_concat_ws)
        format_string = staticmethod(_format_string)

        # String (single col)
        upper = staticmethod(_r.upper)
        lower = staticmethod(_r.lower)
        trim = staticmethod(_r.trim)
        substring = staticmethod(_r.substring)
        length = staticmethod(_r.length)
        regexp_extract = staticmethod(_r.regexp_extract)
        regexp_replace = staticmethod(_r.regexp_replace)
        split = staticmethod(_r.split)
        lpad = staticmethod(_r.lpad)
        rpad = staticmethod(_r.rpad)
        contains = staticmethod(_r.contains)
        like = staticmethod(_r.like)

        # Math
        abs = staticmethod(_r.abs)
        ceil = staticmethod(_r.ceil)
        floor = staticmethod(_r.floor)
        round = staticmethod(_r.round)
        sqrt = staticmethod(_r.sqrt)
        pow = staticmethod(_r.pow)
        power = staticmethod(_r.power)
        exp = staticmethod(_r.exp)
        log = staticmethod(_r.log)

        # Datetime
        year = staticmethod(_r.year)
        month = staticmethod(_r.month)
        day = staticmethod(_r.day)
        hour = staticmethod(_r.hour)
        minute = staticmethod(_r.minute)
        second = staticmethod(_r.second)
        to_date = staticmethod(_r.to_date)
        date_format = staticmethod(_r.date_format)
        current_timestamp = staticmethod(_r.current_timestamp)
        date_add = staticmethod(_r.date_add)
        date_sub = staticmethod(_r.date_sub)

        # Conditional (variadic)
        coalesce = staticmethod(_coalesce)
        greatest = staticmethod(_greatest)
        least = staticmethod(_least)
        when = staticmethod(_r.when)
        when_otherwise = staticmethod(_r.when_otherwise)

        # Aggregation
        sum_ = staticmethod(_r.sum)
        count = staticmethod(_r.count)
        avg = staticmethod(_r.avg)
        mean = staticmethod(_r.mean)
        min_ = staticmethod(_r.min)
        max_ = staticmethod(_r.max)
        count_distinct = staticmethod(_r.count_distinct)

    # PySpark uses sum, min, max (not sum_, min_, max_)
    RobinFunctions.sum = RobinFunctions.sum_  # type: ignore[attr-defined]
    RobinFunctions.min = RobinFunctions.min_  # type: ignore[attr-defined]
    RobinFunctions.max = RobinFunctions.max_  # type: ignore[attr-defined]

    return RobinFunctions()


# Singleton
_robin_fns: Any = None


def get_robin_functions() -> Any:
    global _robin_fns
    if _robin_fns is None:
        _robin_fns = _robin_functions_module()
    return _robin_fns
