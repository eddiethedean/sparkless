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


def _value_to_dict_for_crate(val: Any) -> Any:
    """Convert tuple (struct-like) to dict so crate receives dict not tuple. Leaves list as-is (array)."""
    if isinstance(val, dict):
        return {k: _value_to_dict_for_crate(v) for k, v in val.items()}
    if isinstance(val, tuple):
        return {str(i): _value_to_dict_for_crate(v) for i, v in enumerate(val)}
    return val


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


class RobinSparkSession:
    """PySpark-compatible SparkSession backed by PySparkSession from sparkless_robin."""

    backend_type: str = "robin"  # For tests that assert spark.backend_type == "robin"

    @classmethod
    def _has_active_session(cls) -> bool:
        """Return True if there is an active session (for compatibility)."""
        return True

    def stop(self) -> None:
        """Stop the session. No-op if the crate does not support it."""
        if hasattr(self._inner, "stop"):
            self._inner.stop()

    def __init__(self, app_name_or_inner: Any = "SparklessApp") -> None:
        """Support SparkSession('AppName') or SparkSession(inner) from builder().getOrCreate()."""
        if isinstance(app_name_or_inner, str):
            _r = _get_robin()
            builder = _r.PySparkSessionBuilder()
            builder.app_name(app_name_or_inner)
            self._inner = builder.get_or_create()
        else:
            self._inner = app_name_or_inner

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
            schema_list = serialize_schema(schema)
            data_list = _rows_to_dicts(data_list, schema)

        # Crate expects dicts; convert any nested tuple (struct) to dict
        data_list = [_value_to_dict_for_crate(r) for r in data_list]

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

    def appName(self, name: str) -> "RobinSparkSessionBuilder":
        self._inner.app_name(name)
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
        return RobinDataFrame(self._inner.select([_unwrap(c) for c in cols]))

    def withColumn(self, name: str, col: Any) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.with_column(name, _unwrap(col)))

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
        col_names = []
        for c in cols:
            if isinstance(c, str):
                col_names.append(c)
            else:
                c_unwrapped = _unwrap(c)
                col_names.append(getattr(c_unwrapped, "name", str(c_unwrapped)) if hasattr(c_unwrapped, "name") else str(c_unwrapped))
        asc = ascending if isinstance(ascending, bool) else (ascending[0] if ascending else True)
        return RobinDataFrame(self._inner.order_by(col_names, asc))

    def groupBy(self, *cols: Any) -> "RobinGroupedData":
        """Group by columns."""
        col_names = []
        for c in cols:
            if isinstance(c, str):
                col_names.append(c)
            else:
                c_unwrapped = _unwrap(c)
                col_names.append(getattr(c_unwrapped, "name", str(c_unwrapped)) if hasattr(c_unwrapped, "name") else str(c_unwrapped))
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
                names.append(getattr(u, "name", str(u)) if hasattr(u, "name") else str(u))
        return RobinDataFrame(self._inner.join(other._inner, names, how))

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
                raw = [subset] if isinstance(subset, str) else list(subset)
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
