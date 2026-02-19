"""
Phase 3: Thin Python compatibility layer over PyO3 types (sparkless_robin).

Provides PySpark-compatible wrappers around PySparkSession, PyDataFrame, and
PyO3 functions. Row-wraps collect() output for PySpark compatibility.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Union

from ..spark_types import Row
from ..robin.schema_ser import serialize_schema, schema_from_robin_list
from ..core.schema_inference import SchemaInferenceEngine

try:
    from ._robin_column import _unwrap
except ImportError:
    _unwrap = lambda x: x  # noqa: E731


def _is_simple_column_name(raw_name: Any) -> bool:
    """True if raw_name looks like a single column name (not an expression repr like 'False' or '<...>')."""
    if not isinstance(raw_name, str) or not raw_name:
        return False
    if raw_name in ("False", "True"):
        return False
    if raw_name.strip().startswith("<") and "object at" in raw_name:
        return False
    return True


def _value_to_dict_for_crate(val: Any) -> Any:
    """Convert tuple (struct-like) to dict so crate receives dict not tuple. Leaves list as-is (array)."""
    if isinstance(val, dict):
        return {k: _value_to_dict_for_crate(v) for k, v in val.items()}
    if isinstance(val, tuple):
        return {str(i): _value_to_dict_for_crate(v) for i, v in enumerate(val)}
    return val


def _native_value(val: Any) -> Any:
    """Convert numpy/pandas scalars to Python native types for crate compatibility."""
    if val is None:
        return None
    if isinstance(val, (bool, str)):
        return val
    if isinstance(val, (int, float)):
        return val
    try:
        import numpy as np
        if isinstance(val, (np.integer, np.int64, np.int32)):
            return int(val)
        if isinstance(val, (np.floating, np.float64, np.float32)):
            if np.isnan(val):
                return None
            return float(val)
        if isinstance(val, np.bool_):
            return bool(val)
        if isinstance(val, np.ndarray):
            return [_native_value(v) for v in val.tolist()]
    except ImportError:
        pass
    if isinstance(val, dict):
        return {k: _native_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_native_value(v) for v in val]
    return val


def _parse_schema_string(schema_str: str) -> List[dict]:
    """Parse PySpark-style schema string 'name type, name2 type2' to [{\"name\", \"type\"}, ...]."""
    out = []
    for part in schema_str.strip().split(","):
        part = part.strip()
        if not part:
            continue
        tokens = part.split()
        if len(tokens) >= 2:
            name = tokens[0]
            type_str = tokens[1].lower()
            out.append({"name": name, "type": type_str})
        elif len(tokens) == 1:
            out.append({"name": tokens[0], "type": "string"})
    return out


def _rows_to_dicts(
    data: List[Any], schema: Optional[Any]
) -> List[dict]:
    """Convert list of tuple/Row to list of dicts using schema names. Leaves dict rows unchanged."""
    if not data:
        return []
    if schema is None:
        out = []
        for row in data:
            if isinstance(row, dict):
                out.append(row)
            elif hasattr(row, "asDict"):
                out.append(dict(row.asDict()))
            else:
                out.append(row)
        return out
    names = []
    if schema is not None and hasattr(schema, "fields") and getattr(schema, "fields", None):
        names = [f.name for f in schema.fields]
    elif schema is not None and hasattr(schema, "fieldNames"):
        names = list(schema.fieldNames())
    elif isinstance(schema, (list, tuple)):
        names = [str(s) for s in schema]
    if not names:
        return list(data) if all(isinstance(r, dict) for r in data) else []
    out = []
    for row in data:
        if isinstance(row, dict):
            out.append(row)
        elif hasattr(row, "asDict"):
            out.append(dict(row.asDict()))
        elif isinstance(row, (list, tuple)):
            out.append(dict(zip(names, row)))
        else:
            out.append(row)
    return out


def _get_robin() -> Any:
    """Import sparkless_robin; raise if unavailable."""
    try:
        import sparkless_robin as _r  # type: ignore[import-untyped]
        return _r
    except ImportError as e:
        raise ImportError(
            "sparkless_robin native extension is not available. "
            "Build with: maturin develop"
        ) from e


# -----------------------------------------------------------------------------
# SparkSession wrapper: schema inference + PySpark names
# -----------------------------------------------------------------------------


class _RobinRuntimeConfig:
    """Minimal conf object for spark.conf.get/set compatibility."""

    def __init__(self) -> None:
        self._conf: dict = {}

    def get(self, key: str, default: str = "") -> str:
        return self._conf.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._conf[key] = str(value)


class RobinSparkSession:
    """PySpark-compatible SparkSession backed by PySparkSession from sparkless_robin."""

    backend_type: str = "robin"  # For tests that assert spark.backend_type == "robin"
    _active_sessions: List[Any] = []  # Class-level; last created session is "active"

    @classmethod
    def _has_active_session(cls) -> bool:
        """Return True if there is an active session (for compatibility)."""
        return len(cls._active_sessions) > 0

    @classmethod
    def getActiveSession(cls) -> Optional["RobinSparkSession"]:
        """Return the active SparkSession if any (PySpark compatibility)."""
        return cls._active_sessions[-1] if cls._active_sessions else None

    def stop(self) -> None:
        """Stop the session. No-op if the crate does not support it."""
        if hasattr(self._inner, "stop"):
            self._inner.stop()

    @property
    def app_name(self) -> str:
        """Application name. PySpark: spark.sparkContext.appName or spark.conf.get('spark.app.name')."""
        app_name_attr = getattr(self._inner, "app_name", None)
        if callable(app_name_attr):
            return app_name_attr()
        if app_name_attr is not None and isinstance(app_name_attr, str):
            return app_name_attr
        return "SparklessApp"

    def __init__(self, app_name_or_inner: Any = "SparklessApp") -> None:
        """Support SparkSession('AppName') or SparkSession(inner) from builder().getOrCreate()."""
        self._conf = _RobinRuntimeConfig()
        if isinstance(app_name_or_inner, str):
            _r = _get_robin()
            builder = _r.PySparkSessionBuilder()
            builder.app_name(app_name_or_inner)
            self._inner = builder.get_or_create()
        else:
            self._inner = app_name_or_inner
        RobinSparkSession._active_sessions.append(self)

    def createDataFrame(
        self,
        data: Union[Sequence[dict], Any],
        schema: Optional[Any] = None,
    ) -> "RobinDataFrame":
        """createDataFrame with optional schema inference."""
        _r = _get_robin()
        # Normalize data to list of dicts
        if hasattr(data, "toDict"):
            # Pandas DataFrame
            data_list = data.toDict("records")  # type: ignore[union-attr]
        elif isinstance(data, (list, tuple)):
            data_list = list(data)
        else:
            data_list = list(data)

        if schema is None:
            if not data_list:
                raise ValueError("createDataFrame requires schema when data is empty")
            # Ensure dict rows for inference (tuples need schema to convert)
            data_list = _rows_to_dicts(data_list, None)
            if data_list and not isinstance(data_list[0], dict):
                raise ValueError("createDataFrame requires schema when data is list of tuples")
            inferred_schema, normalized_data = SchemaInferenceEngine.infer_from_data(
                data_list
            )
            schema_list = serialize_schema(inferred_schema)
            data_list = normalized_data
        else:
            if isinstance(schema, str):
                schema_list = _parse_schema_string(schema)
                # _rows_to_dicts needs something with field names for tuple rows
                _schema_for_rows = type("_Schema", (), {"fieldNames": lambda: [e["name"] for e in schema_list], "fields": []})()
                data_list = _rows_to_dicts(data_list, _schema_for_rows)
            else:
                schema_list = serialize_schema(schema)
                data_list = _rows_to_dicts(data_list, schema)

        # Crate expects dicts; convert any nested tuple (struct) to dict
        data_list = [_value_to_dict_for_crate(r) for r in data_list]
        # Normalize values to Python native types (crate may reject numpy/pandas scalars)
        data_list = [
            {k: _native_value(v) for k, v in r.items()} if isinstance(r, dict) else r
            for r in data_list
        ]

        df = self._inner.createDataFrame(data_list, schema_list)
        return RobinDataFrame(df)

    def sql(self, query: str) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.sql(query))

    def table(self, name: str) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.table(name))

    @property
    def read(self) -> "RobinDataFrameReader":
        return RobinDataFrameReader(self._inner)

    @property
    def conf(self) -> _RobinRuntimeConfig:
        """PySpark-compatible spark.conf (get/set)."""
        return self._conf

    @property
    def _storage(self) -> Any:
        """PySpark-compatible _storage; Robin has no storage API, return None to avoid AttributeError."""
        return None

    @property
    def read_parquet(self) -> Any:
        """Convenience: spark.read_parquet(path)"""
        def _read(path: str) -> "RobinDataFrame":
            return RobinDataFrame(self._inner.read_parquet(path))
        return _read

    @property
    def read_csv(self) -> Any:
        def _read(path: str) -> "RobinDataFrame":
            return RobinDataFrame(self._inner.read_csv(path))
        return _read

    @property
    def read_json(self) -> Any:
        def _read(path: str) -> "RobinDataFrame":
            return self.read().json(path)
        return _read

    @property
    def read_delta(self) -> Any:
        def _read(path: str) -> "RobinDataFrame":
            return self.read().delta(path)
        return _read


class RobinSparkSessionBuilder:
    """PySpark-compatible builder. getOrCreate() returns RobinSparkSession."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._config: dict = {}

    def appName(self, name: str) -> "RobinSparkSessionBuilder":
        self._inner.app_name(name)
        return self

    def config(self, key: str, value: Any = None) -> "RobinSparkSessionBuilder":
        """Set config key=value. PySpark: builder.config('k', 'v')."""
        if value is not None:
            self._config[key] = value
        return self

    def getOrCreate(self) -> RobinSparkSession:
        return RobinSparkSession(self._inner.get_or_create())


# PySpark compatibility: SparkSession.builder.appName(...).getOrCreate()
RobinSparkSession.builder = RobinSparkSessionBuilder(  # type: ignore[attr-defined]
    _get_robin().PySparkSessionBuilder()
)


class RobinDataFrameReader:
    """Minimal DataFrameReader for spark.read.parquet(path) etc."""

    def __init__(self, session: Any) -> None:
        self._session = session
        self._options: dict = {}

    def option(self, key: str, value: Any) -> "RobinDataFrameReader":
        """Store option for use in read (e.g. header, inferSchema)."""
        self._options[key] = value
        return self

    def parquet(self, path: str) -> "RobinDataFrame":
        return RobinDataFrame(self._session.read_parquet(path))

    def csv(self, path: str) -> "RobinDataFrame":
        return RobinDataFrame(self._session.read_csv(path))

    def json(self, path: str) -> "RobinDataFrame":
        from ..robin import native as _robin_native

        rows, schema = _robin_native.read_json_via_robin(path)
        inner = self._session.createDataFrame(rows, schema)
        return RobinDataFrame(inner)

    def delta(self, path: str) -> "RobinDataFrame":
        from ..robin import native as _robin_native

        rows = _robin_native.read_delta_via_robin(path)
        if not rows:
            raise ValueError("read_delta requires non-empty data to infer schema")
        inferred_schema, normalized = SchemaInferenceEngine.infer_from_data(rows)
        schema_list = serialize_schema(inferred_schema)
        inner = self._session.createDataFrame(normalized, schema_list)
        return RobinDataFrame(inner)


# -----------------------------------------------------------------------------
# DataFrame wrapper: Row-wrapping collect
# -----------------------------------------------------------------------------


class RobinDataFrame:
    """PySpark-compatible DataFrame backed by PyDataFrame. Wraps collect() in Row."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        """PySpark compat: df.column_name -> F.col('column_name')."""
        if name.startswith("_"):
            raise AttributeError(name)
        # Reserved attributes that are not column references
        if name in ("columns", "schema"):
            raise AttributeError(name)
        from ._robin_functions import get_robin_functions

        return get_robin_functions().col(name)

    @property
    def columns(self) -> List[str]:
        """Column names as a list."""
        schema_list = self._inner.schema()
        return [f["name"] for f in schema_list]

    @property
    def schema(self) -> Any:
        """Schema as StructType. Never None for Robin DataFrames."""
        schema_list = self._inner.schema()
        if not schema_list:
            from ..spark_types import StructType
            return StructType([])
        return schema_from_robin_list(schema_list)

    def __getitem__(self, item: Union[str, Any]) -> Any:
        """PySpark compat: df['col'] -> F.col('col')."""
        if isinstance(item, str):
            from ._robin_functions import get_robin_functions
            return get_robin_functions().col(item)
        raise TypeError(f"RobinDataFrame does not support __getitem__ for {type(item)}")

    def filter(self, condition: Any) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.filter(_unwrap(condition)))

    def select(self, *cols: Any) -> "RobinDataFrame":
        # Flatten list/tuple so df.select(["a", "b"]) and df.select((c1, c2)) work like df.select("a", "b")
        flat: List[Any] = []
        for c in cols:
            if isinstance(c, (list, tuple)):
                flat.extend(_unwrap(x) for x in c)
            else:
                flat.append(_unwrap(c))
        return RobinDataFrame(self._inner.select(flat))

    def withColumn(self, name: str, col: Any) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.with_column(name, _unwrap(col)))

    def withColumnRenamed(self, existing: str, new: str) -> "RobinDataFrame":
        """Rename a column. PySpark: df.withColumnRenamed('old', 'new')."""
        if hasattr(self._inner, "with_column_renamed"):
            return RobinDataFrame(self._inner.with_column_renamed(existing, new))
        from ._robin_functions import get_robin_functions
        F = get_robin_functions()
        select_list = [
            F.col(name).alias(new) if name == existing else F.col(name)
            for name in self.columns
        ]
        return self.select(*select_list)

    def drop(self, *cols: Union[str, Any]) -> "RobinDataFrame":
        col_names = []
        for c in cols:
            if isinstance(c, str):
                col_names.append(c)
            else:
                c_unwrapped = _unwrap(c)
                col_names.append(getattr(c_unwrapped, "name", str(c_unwrapped)) if hasattr(c_unwrapped, "name") else str(c_unwrapped))
        return RobinDataFrame(self._inner.drop(col_names))

    def limit(self, n: int) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.limit(n))

    def orderBy(self, *cols: Any, ascending: Optional[Union[bool, List[bool]]] = None) -> "RobinDataFrame":
        if not cols:
            return self
        try:
            import sparkless_robin as _r
            PySortOrder = getattr(_r, "PySortOrder", None)
        except ImportError:
            PySortOrder = None
        if PySortOrder and any(isinstance(c, PySortOrder) for c in cols):
            # Build list of SortOrder: use as-is or F.asc(col) for Column/str
            sort_orders = []
            for c in cols:
                if isinstance(c, PySortOrder):
                    sort_orders.append(c)
                elif isinstance(c, str):
                    sort_orders.append(_r.asc(_r.col(c)))
                else:
                    sort_orders.append(_r.asc(_unwrap(c)))
            return RobinDataFrame(self._inner.order_by_exprs(sort_orders))
        # Build column names for order_by(col_names, asc); if any is an expression use order_by_exprs
        asc = ascending if isinstance(ascending, bool) else (ascending[0] if ascending else True)
        asc_list = ascending if isinstance(ascending, (list, tuple)) else ([ascending] * len(cols) if ascending is not None else [True] * len(cols))
        col_names = []
        use_exprs = False
        for c in cols:
            if isinstance(c, str):
                col_names.append(c)
            else:
                c_unwrapped = _unwrap(c)
                raw_name = getattr(c_unwrapped, "name", str(c_unwrapped)) if hasattr(c_unwrapped, "name") else str(c_unwrapped)
                if not _is_simple_column_name(raw_name):
                    use_exprs = True
                col_names.append(raw_name)
        if use_exprs:
            sort_orders = []
            for i, c in enumerate(cols):
                asc_i = asc_list[i] if i < len(asc_list) else True
                if isinstance(c, str):
                    sort_orders.append(_r.asc(_r.col(c)) if asc_i else _r.desc(_r.col(c)))
                else:
                    sort_orders.append(_r.asc(_unwrap(c)) if asc_i else _r.desc(_unwrap(c)))
            return RobinDataFrame(self._inner.order_by_exprs(sort_orders))
        return RobinDataFrame(self._inner.order_by(col_names, asc))

    def groupBy(self, *cols: Any) -> "RobinGroupedData":
        """Group by columns. Flattens list/tuple so groupBy([a, b]) works like groupBy(a, b)."""
        col_names: List[str] = []
        for c in cols:
            if isinstance(c, (list, tuple)):
                for item in c:
                    if isinstance(item, str):
                        col_names.append(item)
                    else:
                        item_unwrapped = _unwrap(item)
                        col_names.append(
                            getattr(item_unwrapped, "name", str(item_unwrapped))
                            if hasattr(item_unwrapped, "name")
                            else str(item_unwrapped)
                        )
            elif isinstance(c, str):
                col_names.append(c)
            else:
                c_unwrapped = _unwrap(c)
                col_names.append(
                    getattr(c_unwrapped, "name", str(c_unwrapped))
                    if hasattr(c_unwrapped, "name")
                    else str(c_unwrapped)
                )
        return RobinGroupedData(self._inner.group_by(col_names))

    def join(
        self,
        other: "RobinDataFrame",
        on: Union[str, List[str], Any],
        how: str = "inner",
    ) -> "RobinDataFrame":
        # Normalize so single Column is not iterated (PyColumn is not iterable)
        if isinstance(on, str):
            on_list = [on]
        elif isinstance(on, (list, tuple)):
            on_list = list(on)
        else:
            on_list = [on]
        # If on_list contains Column-like (e.g. RobinColumn), extract names
        names = []
        for o in on_list:
            if isinstance(o, str):
                names.append(o)
            else:
                u = _unwrap(o)
                raw_name = getattr(u, "name", str(u)) if hasattr(u, "name") else str(u)
                if not _is_simple_column_name(raw_name):
                    raise NotImplementedError(
                        "join on expression (e.g. df1.a == df2.b) is not supported by the Robin backend. "
                        "Use join on column name(s) or add the test to tests/robin_skip_list.json. "
                        "See docs/robin_parity_matrix.md."
                    )
                names.append(raw_name)
        return RobinDataFrame(self._inner.join(other._inner, names, how))

    def crossJoin(self, other: "RobinDataFrame") -> "RobinDataFrame":
        """Cartesian product with other. PySpark: df.crossJoin(other)."""
        if hasattr(self._inner, "cross_join"):
            return RobinDataFrame(self._inner.cross_join(other._inner))
        raise NotImplementedError(
            "crossJoin() is not implemented for the Robin backend. "
            "See docs/robin_parity_matrix.md and tests/robin_skip_list.json."
        )

    def first(self) -> Optional[Row]:
        """Return the first row or None if empty. PySpark: df.first()."""
        rows = self.limit(1).collect()
        return rows[0] if rows else None

    def union(self, other: "RobinDataFrame") -> "RobinDataFrame":
        return RobinDataFrame(self._inner.union(other._inner))

    def unionByName(
        self,
        other: "RobinDataFrame",
        allowMissingColumns: bool = False,
    ) -> "RobinDataFrame":
        return RobinDataFrame(
            self._inner.union_by_name(other._inner, allowMissingColumns)
        )

    def distinct(self) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.distinct())

    def cube(self, *cols: Any) -> "RobinGroupedData":
        """Cube (grouping set). Not implemented for Robin; use skip list for tests."""
        raise NotImplementedError(
            "cube() is not implemented for the Robin backend. "
            "See docs/robin_parity_matrix.md and tests/robin_skip_list.json."
        )

    def rollup(self, *cols: Any) -> "RobinGroupedData":
        """Rollup (grouping set). Not implemented for Robin; use skip list for tests."""
        raise NotImplementedError(
            "rollup() is not implemented for the Robin backend. "
            "See docs/robin_parity_matrix.md and tests/robin_skip_list.json."
        )

    def _subset_to_column_names(self, subset: Optional[Union[Sequence[str], Any]] = None) -> Optional[List[str]]:
        """Normalize subset to list of column names. Single Column -> [name]; list of Column/str -> names."""
        if subset is None:
            return None
        if isinstance(subset, (list, tuple)):
            names = []
            for s in subset:
                if isinstance(s, str):
                    names.append(s)
                else:
                    u = _unwrap(s)
                    names.append(
                        getattr(u, "name", str(u)) if hasattr(u, "name") else str(u)
                    )
            return names
        # Single Column (or similar)
        u = _unwrap(subset)
        return [getattr(u, "name", str(u)) if hasattr(u, "name") else str(u)]

    def dropDuplicates(self, subset: Optional[Union[List[str], Any]] = None) -> "RobinDataFrame":
        """Drop duplicate rows. subset: column names or Column(s); normalized to names for crate."""
        col_names = self._subset_to_column_names(subset)
        if hasattr(self._inner, "distinct_subset"):
            return RobinDataFrame(self._inner.distinct_subset(col_names))
        return RobinDataFrame(self._inner.distinct())

    def drop_duplicates(self, subset: Optional[Union[List[str], Any]] = None) -> "RobinDataFrame":
        """Alias for dropDuplicates()."""
        return self.dropDuplicates(subset)

    def fillna(
        self,
        value: Any,
        subset: Optional[Union[str, List[str], tuple]] = None,
    ) -> "RobinDataFrame":
        """Fill null values. When value is a dict, fill by column name (PySpark: ignore subset)."""
        rows = self._inner.collect()
        schema_list = self._inner.schema()
        if not rows:
            return self
        cols = self.columns
        filled = []
        if isinstance(value, dict):
            # PySpark: value=dict uses column name -> fill value; subset is ignored
            for row in rows:
                r = dict(row)
                for k, fill_val in value.items():
                    if k in r and r[k] is None:
                        r[k] = fill_val
                filled.append(r)
        else:
            if subset is not None:
                # Single Column or str -> list of one; list/tuple -> list
                if isinstance(subset, str):
                    raw = [subset]
                elif isinstance(subset, (list, tuple)):
                    raw = list(subset)
                else:
                    raw = [subset]  # single Column-like
                # Resolve Column to column name so subset is always list of str
                subset_cols = [
                    c if isinstance(c, str) else getattr(c, "name", str(c))
                    for c in raw
                ]
            else:
                subset_cols = cols
            for row in rows:
                r = dict(row)
                for k in subset_cols:
                    if k in r and r[k] is None:
                        r[k] = value
                filled.append(r)
        session = get_or_create_robin_session()
        schema_struct = schema_from_robin_list(schema_list)
        return session.createDataFrame(filled, schema_struct)

    def createOrReplaceTempView(self, name: str) -> None:
        """Register this DataFrame as a temp view (createOrReplaceTempView)."""
        from ..robin import native as _robin_native

        data = self._inner.collect()
        schema = self._inner.schema()
        _robin_native.register_temp_view_via_robin(name, data, schema)

    def count(self) -> int:
        """Return number of rows."""
        return len(self.collect())

    def collect(self) -> List[Row]:
        """Collect rows, wrapping each dict in Row for PySpark compatibility."""
        raw = self._inner.collect()
        return [Row(d) for d in raw]

    def show(self, n: int = 20, truncate: bool = True) -> None:
        """Simple show: print first n rows."""
        rows = self.collect()[:n]
        if not rows:
            print("+---+\n|  |\n+---+")
            return
        keys = list(rows[0].asDict().keys()) if hasattr(rows[0], "asDict") else list(rows[0].keys())
        col_widths = [max(len(str(k)), 4) for k in keys]
        for r in rows:
            d = r.asDict() if hasattr(r, "asDict") else dict(r)
            for i, k in enumerate(keys):
                v = str(d.get(k, ""))[:20] if truncate else str(d.get(k, ""))
                col_widths[i] = max(col_widths[i], len(v))
        sep = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
        print(sep)
        print("|" + "|".join(f" {str(k):{col_widths[i]}} " for i, k in enumerate(keys)) + "|")
        print(sep)
        for r in rows:
            d = r.asDict() if hasattr(r, "asDict") else dict(r)
            vals = [str(d.get(k, ""))[:20] if truncate else str(d.get(k, "")) for k in keys]
            print("|" + "|".join(f" {v:{col_widths[i]}} " for i, v in enumerate(vals)) + "|")
        print(sep)

    @property
    def _py(self) -> Any:
        """Access underlying PyDataFrame for advanced use."""
        return self._inner


# -----------------------------------------------------------------------------
# DataFrameWriter: save/saveAsTable via Robin native write functions
# -----------------------------------------------------------------------------

from ..robin import native as _robin_native


class RobinDataFrameWriter:
    """DataFrameWriter for RobinDataFrame. save() and saveAsTable() via Robin."""

    def __init__(self, df: RobinDataFrame) -> None:
        self._df = df
        self._format: str = "parquet"
        self._mode: str = "overwrite"

    def mode(self, mode: str) -> "RobinDataFrameWriter":
        self._mode = str(mode).lower()
        return self

    def format(self, source: str) -> "RobinDataFrameWriter":
        self._format = str(source).lower()
        return self

    def option(self, key: str, value: Any) -> "RobinDataFrameWriter":
        # Options stored but not yet used by Robin write functions
        return self

    def save(self, path: str) -> None:
        data = self._df._inner.collect()
        schema = self._df._inner.schema()
        overwrite = self._mode in ("overwrite",)
        fmt = self._format.lower()
        if fmt == "parquet":
            _robin_native.write_parquet_via_robin(data, schema, path, overwrite)
        elif fmt == "csv":
            _robin_native.write_csv_via_robin(data, schema, path, overwrite)
        elif fmt in ("json", "jsonl"):
            _robin_native.write_json_via_robin(data, schema, path, overwrite)
        elif fmt == "delta":
            _robin_native.write_delta_via_robin(data, schema, path, overwrite)
        else:
            raise NotImplementedError(
                f"df.write.format('{self._format}').save() not supported. "
                "Use parquet, csv, json, or delta."
            )

    def saveAsTable(self, name: str) -> None:
        data = self._df._inner.collect()
        schema = self._df._inner.schema()
        mode = self._mode if self._mode in ("overwrite", "append", "ignore", "error") else "error"
        _robin_native.save_as_table_via_robin(name, data, schema, mode)


# -----------------------------------------------------------------------------
# GroupedData: limited agg support
# -----------------------------------------------------------------------------


class RobinGroupedData:
    """Wrapper for PyGroupedData. agg() and pivot() delegate to crate when available."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def agg(self, *exprs: Any) -> RobinDataFrame:
        agg_fn = getattr(self._inner, "agg", None)
        if agg_fn is None:
            raise NotImplementedError(
                "GroupedData.agg() not yet fully implemented for Robin backend."
            )
        unwrapped = [_unwrap(e) for e in exprs]
        return RobinDataFrame(agg_fn(unwrapped))

    def pivot(self, pivot_col: str, values: Optional[Sequence[str]] = None) -> "RobinPivotedGroupedData":
        """Pivot (PySpark: groupBy(...).pivot(col, values)). Returns object with .sum(), .avg(), etc."""
        pivot_fn = getattr(self._inner, "pivot", None)
        if pivot_fn is None:
            raise NotImplementedError(
                "'RobinGroupedData' object has no attribute 'pivot'. "
                "Implement pivot on Robin backend."
            )
        result = pivot_fn(pivot_col, list(values) if values is not None else None)
        return RobinPivotedGroupedData(result)

    def count(self) -> RobinDataFrame:
        """Return aggregated count per group. Delegates to Rust PyGroupedData.count()."""
        return RobinDataFrame(self._inner.count())


class RobinPivotedGroupedData:
    """Result of groupBy(...).pivot(col). Has .sum(column), .avg(column), etc."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def sum(self, column: str) -> RobinDataFrame:
        return RobinDataFrame(self._inner.sum(column))

    def avg(self, column: str) -> RobinDataFrame:
        return RobinDataFrame(self._inner.avg(column))

    def min(self, column: str) -> RobinDataFrame:
        return RobinDataFrame(self._inner.min(column))

    def max(self, column: str) -> RobinDataFrame:
        return RobinDataFrame(self._inner.max(column))

    def count(self) -> RobinDataFrame:
        return RobinDataFrame(self._inner.count())


# -----------------------------------------------------------------------------
# Session factory: builder().appName().getOrCreate() -> RobinSparkSession
# -----------------------------------------------------------------------------


def _create_robin_session() -> RobinSparkSession:
    """Create session via builder. Used when SparkSession.builder().getOrCreate() is called."""
    _r = _get_robin()
    builder = _r.PySparkSession.builder()
    # Default app name if not set
    inner = builder.get_or_create()
    return RobinSparkSession(inner)


def get_or_create_robin_session(app_name: str = "SparklessApp") -> RobinSparkSession:
    """Get or create a Robin-backed SparkSession."""
    _r = _get_robin()
    builder = _r.PySparkSessionBuilder()
    builder.app_name(app_name)
    inner = builder.get_or_create()
    return RobinSparkSession(inner)
