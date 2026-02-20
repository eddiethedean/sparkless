"""
Robin column wrapper: adds PySpark-compatible methods to PyColumn from the crate.

PyColumn (Rust) does not implement astype, substr, desc, asc, isin, etc.
This wrapper delegates to F.* where possible and implements reverse operators.
"""

from __future__ import annotations

import math
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


def _is_pycolumn_like(obj: Any) -> bool:
    """True if obj is a crate PyColumn (so we should wrap it as RobinColumn)."""
    if obj is None or isinstance(obj, RobinColumn):
        return False
    # Crate exposes PyColumn; avoid wrapping SortOrder or other non-Column types
    t = type(obj)
    return getattr(t, "__name__", "") == "PyColumn" or (
        hasattr(obj, "alias") and hasattr(obj, "__add__") and callable(getattr(obj, "cast", None))
    )


def _data_type_to_cast_string(data_type: Any) -> str:
    """Convert PySpark/Sparkless DataType to crate type name (e.g. IntegerType() -> 'int')."""
    if isinstance(data_type, str):
        return data_type
    type_name = getattr(data_type, "typeName", None)
    if type_name is not None and callable(type_name):
        return type_name()
    simple = getattr(data_type, "simpleString", None)
    if simple is not None and callable(simple):
        return simple()
    return str(data_type)


class RobinCaseWhenBuilder:
    """Builder for F.when(cond).when(cond2, value2).otherwise(else_value). Crate has when_otherwise(cond, then_val, else_val)."""

    def __init__(self, first_cond: Any, first_value: Any = None) -> None:
        self._pairs: List[tuple] = [(first_cond, first_value)]

    def when(self, cond: Any, value: Any = None) -> "RobinCaseWhenBuilder":
        self._pairs.append((cond, value))
        return self

    def otherwise(self, value: Any) -> RobinColumn:
        from ._robin_functions import _get_robin
        _r = _get_robin()
        else_inner = _unwrap(value) if isinstance(value, RobinColumn) else _r.lit(value)
        result = else_inner
        for cond, then_val in reversed(self._pairs):
            cond_inner = _unwrap(cond) if isinstance(cond, RobinColumn) else cond
            if then_val is None:
                # .when(cond) with no value: then-branch is null, else-branch is current result
                result = _r.when_otherwise(cond_inner, _r.lit(None), result)
            else:
                then_inner = _unwrap(then_val) if isinstance(then_val, RobinColumn) else _r.lit(then_val)
                result = _r.when_otherwise(cond_inner, then_inner, result)
        return _wrap(result)


class RobinColumn:
    """Wrapper around PyColumn that adds missing PySpark Column methods."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def _f(self) -> Any:
        from ._robin_functions import get_robin_functions
        return get_robin_functions()

    # ---- Forward to inner ----
    def __getattr__(self, name: str) -> Any:
        val = getattr(self._inner, name)
        if callable(val) and name in ("when", "otherwise"):
            def _wrap_result(*args: Any, **kwargs: Any) -> Any:
                result = val(*args, **kwargs)
                return _wrap(result) if _is_pycolumn_like(result) else result
            return _wrap_result
        return _wrap(val) if _is_pycolumn_like(val) else val

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

    def __pow__(self, other: Any) -> RobinColumn:
        """Power: col ** other. PySpark: col ** 2. Crate pow(col, exp) expects int exponent."""
        if isinstance(other, (int, float)):
            return self._f().pow(self, int(other))
        # Column ** Column: exp(log(self) * other)
        return self._f().exp(self._f().log(self) * other)

    def __rpow__(self, other: Any) -> RobinColumn:
        """Reverse power: other ** col. PySpark: 2 ** col. Implement as exp(log(other) * col)."""
        if isinstance(other, (int, float)) and other <= 0:
            raise ValueError("math domain error: base must be positive for __rpow__")
        return self._f().exp(self._f().lit(math.log(other)) * self)

    def __neg__(self) -> RobinColumn:
        """Unary minus. PySpark: -col."""
        return _wrap(self._lit_inner(0).__sub__(self._inner))

    def __eq__(self, other: Any) -> RobinColumn:
        return _wrap(self._inner.__eq__(self._other_inner(other)))

    def eqNullSafe(self, other: Any) -> RobinColumn:
        """Null-safe equality: True if both null, or both non-null and equal."""
        other_inner = self._other_inner(other)
        if hasattr(self._inner, "eq_null_safe"):
            return _wrap(self._inner.eq_null_safe(other_inner))
        # Fallback: (self.isNull() & other.isNull()) | (self.isNotNull() & (self == other))
        other_col = _wrap(other_inner)
        both_null = self.isNull() & other_col.isNull()
        both_eq = self.isNotNull() & (self == other_col)
        return both_null | both_eq

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

    def __and__(self, other: Any) -> RobinColumn:
        """Boolean AND. PySpark: col & other. Use _other_inner so literals become columns."""
        return _wrap(self._inner.and_(self._other_inner(other)))

    def __or__(self, other: Any) -> RobinColumn:
        """Boolean OR. PySpark: col | other."""
        return _wrap(self._inner.or_(self._other_inner(other)))

    # ---- PySpark Column methods (use inner when present, else F) ----
    def alias(self, name: str) -> RobinColumn:
        """Column alias. Wrap result so chained .cast(DataType) uses RobinColumn.cast."""
        return _wrap(self._inner.alias(name))

    def astype(self, data_type: Union[str, Any]) -> RobinColumn:
        """Cast column to type. PySpark: col.astype('string'). Crate has .cast()."""
        type_str = _data_type_to_cast_string(data_type)
        if hasattr(self._inner, "cast"):
            return _wrap(self._inner.cast(type_str))
        return _wrap(self._f().cast(self._inner, type_str))

    def cast(self, data_type: Union[str, Any]) -> RobinColumn:
        """Cast column to type. Accepts DataType (uses typeName()) or string."""
        type_str = _data_type_to_cast_string(data_type)
        if hasattr(self._inner, "cast"):
            return _wrap(self._inner.cast(type_str))
        return _wrap(self._f().cast(self._inner, type_str))

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

    def __getitem__(self, key: Any) -> RobinColumn:
        """PySpark: col[key] for array index or map key."""
        return self.getItem(key)

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

    def replace(self, to_replace: Any, replacement: Any = None, **kwargs: Any) -> RobinColumn:
        """Replace to_replace with replacement. PySpark: col.replace(to_replace, replacement) or replace(to_replace, subset=...). subset= is ignored for Column-level replace."""
        subset = kwargs.pop("subset", None)  # accept but ignore at Column level
        if hasattr(self._inner, "replace"):
            if replacement is None and not isinstance(to_replace, dict):
                raise TypeError(
                    "RobinColumn.replace() missing 1 required positional argument: 'replacement'. "
                    "Use col.replace(to_replace, replacement)."
                )
            repl_inner = self._other_inner(replacement) if replacement is not None else None
            if isinstance(to_replace, dict) and replacement is None:
                # PySpark: replace({k: v, ...}) - crate may not support; use first pair or raise
                if not to_replace:
                    return _wrap(self._inner)
                first_k, first_v = next(iter(to_replace.items()))
                repl_inner = self._other_inner(first_v)
                to_replace = first_k
            to_inner = self._other_inner(to_replace)
            return _wrap(
                self._inner.replace(to_inner, repl_inner)
            )
        raise NotImplementedError(
            "Column.replace is not implemented for the Robin backend. "
            "See docs/robin_parity_matrix.md and tests/robin_skip_list.json."
        )

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
