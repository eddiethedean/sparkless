"""
Robin column wrapper: adds PySpark-compatible methods to PyColumn from the crate.

PyColumn (Rust) does not implement astype, substr, desc, asc, isin, etc.
This wrapper delegates to F.* where possible and implements reverse operators.
"""

from __future__ import annotations

from typing import Any, List, Union


def _unwrap(col: Any) -> Any:
    """Return the inner PyColumn for engine calls."""
    if isinstance(col, RobinColumn):
        return col._inner
    return col


def _wrap(col: Any) -> "RobinColumn":
    """Wrap PyColumn in RobinColumn if not already wrapped."""
    if isinstance(col, RobinColumn):
        return col
    return RobinColumn(col)


class RobinColumn:
    """Wrapper around PyColumn that adds missing PySpark Column methods."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def _f(self) -> Any:
        from ._robin_functions import get_robin_functions
        return get_robin_functions()

    # ---- Forward to inner ----
    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def __str__(self) -> str:
        return str(self._inner)

    # ---- Reverse operators (int/float * PyColumn etc.) ----
    def _lit_inner(self, other: Any) -> Any:
        """Literal as inner PyColumn for crate calls."""
        return _unwrap(self._f().lit(other))

    def __radd__(self, other: Any) -> RobinColumn:
        return _wrap(self._lit_inner(other).__add__(self._inner))

    def __rsub__(self, other: Any) -> RobinColumn:
        return _wrap(self._lit_inner(other).__sub__(self._inner))

    def __rmul__(self, other: Any) -> RobinColumn:
        return _wrap(self._lit_inner(other).__mul__(self._inner))

    def __rtruediv__(self, other: Any) -> RobinColumn:
        return _wrap(self._lit_inner(other).__truediv__(self._inner))

    def __rfloordiv__(self, other: Any) -> RobinColumn:
        return _wrap(self._lit_inner(other).__floordiv__(self._inner))

    def __rmod__(self, other: Any) -> RobinColumn:
        return _wrap(self._lit_inner(other).__mod__(self._inner))

    # ---- Forward binary operators: wrap result so we never return raw PyColumn ----
    def _other_inner(self, other: Any) -> Any:
        """Convert other to inner PyColumn for crate binary ops."""
        return _unwrap(other) if isinstance(other, RobinColumn) else _unwrap(self._f().lit(other))

    def __add__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__add__(self._other_inner(other)))

    def __sub__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__sub__(self._other_inner(other)))

    def __mul__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__mul__(self._other_inner(other)))

    def __truediv__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__truediv__(self._other_inner(other)))

    def __floordiv__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__floordiv__(self._other_inner(other)))

    def __mod__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__mod__(self._other_inner(other)))

    def __eq__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__eq__(self._other_inner(other)))

    def __ne__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__ne__(self._other_inner(other)))

    def __lt__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__lt__(self._other_inner(other)))

    def __le__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__le__(self._other_inner(other)))

    def __gt__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__gt__(self._other_inner(other)))

    def __ge__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__ge__(self._other_inner(other)))

    def __invert__(self) -> RobinColumn:
        """Boolean NOT. PySpark: ~col."""
        return _wrap(self._inner.__invert__())

    # ---- PySpark Column methods (use inner when present, else F) ----
    def astype(self, data_type: Union[str, Any]) -> RobinColumn:
        """Cast column to type. PySpark: col.astype('string'). Crate has .cast()."""
        if hasattr(self._inner, "cast"):
            return _wrap(self._inner.cast(str(data_type)))
        return _wrap(self._f().cast(self._inner, data_type))

    def cast(self, data_type: Union[str, Any]) -> RobinColumn:
        """Cast column to type."""
        if hasattr(self._inner, "cast"):
            return _wrap(self._inner.cast(str(data_type)))
        return _wrap(self._f().cast(self._inner, data_type))

    def substr(self, start: int, length: int) -> RobinColumn:
        """Substring (1-based start). PySpark: col.substr(start, length)."""
        return _wrap(self._f().substring(self._inner, start, length))

    def desc(self):
        """Sort descending (returns SortOrder for orderBy)."""
        return self._f().desc(self._inner)

    def asc(self):
        """Sort ascending (returns SortOrder for orderBy)."""
        return self._f().asc(self._inner)

    def desc_nulls_last(self):
        """Sort descending, nulls last (returns SortOrder for orderBy)."""
        return self._f().desc_nulls_last(self._inner)

    def asc_nulls_last(self):
        """Sort ascending, nulls last (returns SortOrder for orderBy)."""
        return self._f().asc_nulls_last(self._inner)

    def desc_nulls_first(self):
        """Sort descending, nulls first (returns SortOrder for orderBy)."""
        return self._f().desc_nulls_first(self._inner)

    def asc_nulls_first(self):
        """Sort ascending, nulls first (returns SortOrder for orderBy)."""
        return self._f().asc_nulls_first(self._inner)

    def isin(self, *values: Any) -> RobinColumn:
        """Column is in values. PySpark: col.isin(1, 2, 3). Crate has isin_(list)."""
        vals = list(values)
        if hasattr(self._inner, "isin_"):
            return _wrap(self._inner.isin_(vals))
        if hasattr(self._inner, "isin"):
            return _wrap(self._inner.isin(vals))
        return _wrap(self._f().isin(self._inner, vals))

    def getItem(self, key: Any) -> RobinColumn:
        """Get item (array index or map key)."""
        return _wrap(self._f().get_item(self._inner, key))

    def startswith(self, prefix: str) -> RobinColumn:
        """String starts with prefix. Use like(col, 'prefix%')."""
        return _wrap(self._f().like(self._inner, prefix + "%"))

    def isNull(self) -> RobinColumn:
        """True if null. Crate has isnull()."""
        if hasattr(self._inner, "isnull"):
            return _wrap(self._inner.isnull())
        if hasattr(self._inner, "is_null"):
            return _wrap(self._inner.is_null())
        return _wrap(self._f().is_null(self._inner))

    @property
    def name(self) -> str:
        """Column name. Delegate to inner if present."""
        if hasattr(self._inner, "name"):
            return getattr(self._inner, "name")
        if hasattr(self._inner, "get_name"):
            return self._inner.get_name()
        return str(self._inner)

    def withField(self, fieldName: str, col: Any) -> RobinColumn:
        """Add or replace struct field. PySpark 3.1+."""
        return _wrap(self._f().with_field(self._inner, fieldName, col))

    def like(self, pattern: str) -> RobinColumn:
        """SQL LIKE."""
        return _wrap(self._f().like(self._inner, pattern))

    def rlike(self, pattern: str) -> RobinColumn:
        """SQL RLIKE (regex)."""
        return _wrap(self._f().rlike(self._inner, pattern))

    def fill(self, value: Any) -> RobinColumn:
        """Fill null with value (column-level). PySpark: col.fill(value)."""
        return _wrap(self._f().fill(self._inner, value))

    def over(self, window: Any) -> RobinColumn:
        """Window function. PySpark: col.over(window)."""
        return _wrap(self._f().over(self._inner, window))

    def __iter__(self) -> Any:
        """Make single column iterable for left_on/right_on (list of one)."""
        return iter([self])
