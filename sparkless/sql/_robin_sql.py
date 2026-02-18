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
            # Infer schema
            inferred_schema, normalized_data = SchemaInferenceEngine.infer_from_data(
                data_list
            )
            schema_list = serialize_schema(inferred_schema)
            data_list = normalized_data
        else:
            schema_list = serialize_schema(schema)

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
        """Schema as StructType."""
        schema_list = self._inner.schema()
        return schema_from_robin_list(schema_list)

    def __getitem__(self, item: Union[str, Any]) -> Any:
        """PySpark compat: df['col'] -> F.col('col')."""
        if isinstance(item, str):
            from ._robin_functions import get_robin_functions
            return get_robin_functions().col(item)
        raise TypeError(f"RobinDataFrame does not support __getitem__ for {type(item)}")

    def filter(self, condition: Any) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.filter(condition))

    def select(self, *cols: Any) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.select(list(cols)))

    def withColumn(self, name: str, col: Any) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.with_column(name, col))

    def drop(self, *cols: Union[str, Any]) -> "RobinDataFrame":
        col_names = [c if isinstance(c, str) else str(c) for c in cols]
        return RobinDataFrame(self._inner.drop(col_names))

    def limit(self, n: int) -> "RobinDataFrame":
        return RobinDataFrame(self._inner.limit(n))

    def orderBy(self, *cols: Any, ascending: Optional[Union[bool, List[bool]]] = None) -> "RobinDataFrame":
        if not cols:
            return self
        # Simplified: assume col names as strings
        col_names = [c if isinstance(c, str) else str(c) for c in cols]
        asc = ascending if isinstance(ascending, bool) else (ascending[0] if ascending else True)
        return RobinDataFrame(self._inner.order_by(col_names, asc))

    def groupBy(self, *cols: Any) -> "RobinGroupedData":
        """Group by columns."""
        col_names = [c if isinstance(c, str) else str(c) for c in cols]
        return RobinGroupedData(self._inner.group_by(col_names))

    def join(
        self,
        other: "RobinDataFrame",
        on: Union[str, List[str], Any],
        how: str = "inner",
    ) -> "RobinDataFrame":
        on_list = [on] if isinstance(on, str) else list(on)
        return RobinDataFrame(self._inner.join(other._inner, on_list, how))

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

    @property
    def write(self) -> "RobinDataFrameWriter":
        """Return DataFrameWriter for write operations."""
        return RobinDataFrameWriter(self)

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
    """Wrapper for PyGroupedData. agg() support is limited."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def agg(self, *exprs: Any) -> RobinDataFrame:
        raise NotImplementedError(
            "GroupedData.agg() not yet fully implemented for Robin backend."
        )


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
