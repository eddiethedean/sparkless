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

    # Optional: first, rank (expose if crate has them)
    _first = getattr(_r, "first", None)
    _rank = getattr(_r, "rank", None)
    _cast = getattr(_r, "cast", None)
    _get_item = getattr(_r, "get_item", None)
    _is_null = getattr(_r, "is_null", None)
    _with_field = getattr(_r, "with_field", None)
    _rlike = getattr(_r, "rlike", None)
    _fill = getattr(_r, "fill", None)
    _over = getattr(_r, "over", None)
    _isin = getattr(_r, "isin", None)

    from ._robin_column import RobinColumn, _unwrap

    def _wrap_col(col: Any) -> Any:
        return RobinColumn(col) if col is not None else None

    def _wrap1(f: Any) -> Any:
        """Wrap f so first arg is unwrapped and result is wrapped."""
        def _w(col: Any, *args: Any, **kwargs: Any) -> Any:
            return _wrap_col(f(_unwrap(col), *args, **kwargs))
        return _w

    def _col(name: str) -> Any:
        return _wrap_col(_r.col(name))

    def _lit(value: Any) -> Any:
        return _wrap_col(_r.lit(value))

    def _concat_w(*cols: Any) -> Any:
        return _wrap_col(_r.concat([_unwrap(c) for c in cols]))

    def _concat_ws_w(separator: str, *cols: Any) -> Any:
        return _wrap_col(_r.concat_ws(separator, [_unwrap(c) for c in cols]))

    def _format_string_w(fmt: str, *cols: Any) -> Any:
        return _wrap_col(_r.format_string(fmt, [_unwrap(c) for c in cols]))

    def _coalesce_w(*cols: Any) -> Any:
        return _wrap_col(_r.coalesce([_unwrap(c) for c in cols]))

    def _greatest_w(*cols: Any) -> Any:
        return _wrap_col(_r.greatest([_unwrap(c) for c in cols]))

    def _least_w(*cols: Any) -> Any:
        return _wrap_col(_r.least([_unwrap(c) for c in cols]))

    # Build a namespace with all Robin functions
    class RobinFunctions:
        # Column (wrap so F.col/F.lit return RobinColumn)
        col = staticmethod(_col)
        lit = staticmethod(_lit)

        # String (variadic)
        concat = staticmethod(_concat_w)
        concat_ws = staticmethod(_concat_ws_w)
        format_string = staticmethod(_format_string_w)

        def _array_w(*cols: Any) -> Any:
            return _wrap_col(_r.array([_unwrap(c) for c in cols]))

        def _create_map_w(*key_values: Any) -> Any:
            return _wrap_col(_r.create_map([_unwrap(c) for c in key_values]))

        array = staticmethod(_array_w)
        create_map = staticmethod(_create_map_w)

        # String (single col) - unwrap so crate receives PyColumn
        upper = staticmethod(_wrap1(_r.upper))
        lower = staticmethod(_wrap1(_r.lower))
        trim = staticmethod(_wrap1(_r.trim))
        def _substring_w(col: Any, start: int, length: Any = None) -> Any:
            return _wrap_col(_r.substring(_unwrap(col), start, length))
        substring = staticmethod(_substring_w)
        length = staticmethod(_wrap1(_r.length))
        regexp_extract = staticmethod(_wrap1(_r.regexp_extract))
        regexp_replace = staticmethod(_wrap1(_r.regexp_replace))
        split = staticmethod(_wrap1(_r.split))
        lpad = staticmethod(_wrap1(_r.lpad))
        rpad = staticmethod(_wrap1(_r.rpad))
        contains = staticmethod(_wrap1(_r.contains))
        like = staticmethod(_wrap1(_r.like))

        # Math
        abs = staticmethod(_wrap1(_r.abs))
        ceil = staticmethod(_wrap1(_r.ceil))
        floor = staticmethod(_wrap1(_r.floor))
        round = staticmethod(_wrap1(_r.round))
        sqrt = staticmethod(_wrap1(_r.sqrt))
        pow = staticmethod(_wrap1(_r.pow))
        power = staticmethod(_wrap1(_r.power))
        exp = staticmethod(_wrap1(_r.exp))
        log = staticmethod(_wrap1(_r.log))

        # Datetime
        year = staticmethod(_wrap1(_r.year))
        month = staticmethod(_wrap1(_r.month))
        day = staticmethod(_wrap1(_r.day))
        hour = staticmethod(_wrap1(_r.hour))
        minute = staticmethod(_wrap1(_r.minute))
        second = staticmethod(_wrap1(_r.second))
        to_date = staticmethod(_wrap1(_r.to_date))
        date_format = staticmethod(_wrap1(_r.date_format))
        current_timestamp = staticmethod(lambda: _wrap_col(_r.current_timestamp()))
        date_add = staticmethod(_wrap1(_r.date_add))
        date_sub = staticmethod(_wrap1(_r.date_sub))

        def _to_timestamp_stub(*args: Any, **kwargs: Any) -> Any:
            raise NotImplementedError(
                "to_timestamp is not implemented for the Robin backend. "
                "Use skip list for tests that require to_timestamp."
            )

        _to_ts = getattr(_r, "to_timestamp", None)
        to_timestamp = staticmethod(
            _wrap1(_to_ts) if _to_ts is not None else _to_timestamp_stub
        )

        # Conditional (variadic)
        coalesce = staticmethod(_coalesce_w)
        greatest = staticmethod(_greatest_w)
        least = staticmethod(_least_w)
        when = staticmethod(lambda cond: _r.when(_unwrap(cond)))
        when_otherwise = staticmethod(_r.when_otherwise)

        # Aggregation
        sum_ = staticmethod(_wrap1(_r.sum))
        count = staticmethod(_wrap1(_r.count))
        avg = staticmethod(_wrap1(_r.avg))
        mean = staticmethod(_wrap1(_r.mean))
        min_ = staticmethod(_wrap1(_r.min))
        max_ = staticmethod(_wrap1(_r.max))
        count_distinct = staticmethod(_wrap1(_r.count_distinct))

    # PySpark uses sum, min, max (not sum_, min_, max_)
    RobinFunctions.sum = RobinFunctions.sum_  # type: ignore[attr-defined]
    RobinFunctions.min = RobinFunctions.min_  # type: ignore[attr-defined]
    RobinFunctions.max = RobinFunctions.max_  # type: ignore[attr-defined]

    # Optional: first, rank (if crate exposes them)
    if _first is not None:
        RobinFunctions.first = staticmethod(_first)  # type: ignore[attr-defined]
    if _rank is not None:
        RobinFunctions.rank = staticmethod(_rank)  # type: ignore[attr-defined]
    if _cast is not None:
        RobinFunctions.cast = staticmethod(_cast)  # type: ignore[attr-defined]
    # Sort order (col.desc(), col.asc(), etc.) - from Rust module
    for _name in ("desc", "asc", "desc_nulls_last", "asc_nulls_last", "desc_nulls_first", "asc_nulls_first"):
        _fn = getattr(_r, _name, None)
        if _fn is not None:
            setattr(RobinFunctions, _name, staticmethod(_fn))  # type: ignore[attr-defined]
    if _get_item is not None:
        RobinFunctions.get_item = staticmethod(_get_item)  # type: ignore[attr-defined]
    if _is_null is not None:
        RobinFunctions.is_null = staticmethod(_is_null)  # type: ignore[attr-defined]
    if _with_field is not None:
        RobinFunctions.with_field = staticmethod(_with_field)  # type: ignore[attr-defined]
    if _rlike is not None:
        RobinFunctions.rlike = staticmethod(_rlike)  # type: ignore[attr-defined]
    if _fill is not None:
        RobinFunctions.fill = staticmethod(_fill)  # type: ignore[attr-defined]
    if _over is not None:
        RobinFunctions.over = staticmethod(_over)  # type: ignore[attr-defined]
    if _isin is not None:
        RobinFunctions.isin = staticmethod(_isin)  # type: ignore[attr-defined]

    return RobinFunctions()


# Singleton
_robin_fns: Any = None


def get_robin_functions() -> Any:
    global _robin_fns
    if _robin_fns is None:
        _robin_fns = _robin_functions_module()
    return _robin_fns
