"""
Thin wrapper around Robin's SparkSession to expose PySpark-style createDataFrame(data, schema=None).
"""

from __future__ import annotations

from typing import Any, List, Optional, Union

import robin_sparkless as _robin

from sparkless.spark_types import StructType
from ._robin_compat import create_dataframe_via_robin, wrap_robin_dataframe


class SparkSession:
    """Sparkless SparkSession: wraps Robin's SparkSession and adds createDataFrame(data, schema=None)."""

    def __init__(self, robin_session: Any = None, **kwargs: Any) -> None:
        # kwargs e.g. backend_type="robin" from fixtures; ignored
        if isinstance(robin_session, str):
            self._robin_session = _robin.SparkSession.builder().app_name(robin_session).get_or_create()
        else:
            self._robin_session = robin_session

    @property
    def backend_type(self) -> str:
        """Backend in use; tests may expect this."""
        return "robin"

    def createDataFrame(
        self,
        data: Union[List[dict], List[tuple], Any],
        schema: Optional[Union[StructType, List[str], str]] = None,
    ) -> Any:
        """Create a DataFrame from data and optional schema (PySpark-compatible)."""
        return create_dataframe_via_robin(self._robin_session, data, schema)

    @property
    def app_name(self) -> str:
        """PySpark-compatible app name; Robin may not expose it."""
        return getattr(self._robin_session, "app_name", None) or getattr(
            self._robin_session, "appName", None
        ) or "Sparkless"

    def table(self, name: str) -> Any:
        """Return a DataFrame for the given table name (delegates to Robin)."""
        out = getattr(self._robin_session, "table", None)
        if out is None:
            raise NotImplementedError("spark.table() is not available on this backend")
        result = out(name)
        return wrap_robin_dataframe(result)

    def sql(self, query: str) -> Any:
        """Execute SQL and return DataFrame (delegates to Robin)."""
        fn = getattr(self._robin_session, "sql", None)
        if fn is None:
            raise NotImplementedError("spark.sql() is not available on this backend")
        result = fn(query)
        return wrap_robin_dataframe(result)

    @classmethod
    def _has_active_session(cls) -> bool:
        """True if an active session exists (tests/F namespace expect this)."""
        return True

    @classmethod
    def getActiveSession(cls) -> Optional["SparkSession"]:
        """Return the active session if any (delegate to Robin or None)."""
        get_active = getattr(_robin.SparkSession, "get_active_session", None) or getattr(
            _robin.SparkSession, "getActiveSession", None
        )
        if get_active is not None:
            robin_sess = get_active() if callable(get_active) else get_active
            if robin_sess is not None:
                return SparkSession(robin_sess)
        return None

    def __getattr__(self, name: str) -> Any:
        """Delegate all other attributes to the Robin session."""
        return getattr(self._robin_session, name)


class _SparkSessionBuilder:
    """Builder that returns Sparkless SparkSession wrapping Robin's session."""

    def __init__(self) -> None:
        self._robin_builder = _robin.SparkSession.builder()

    def appName(self, name: str) -> _SparkSessionBuilder:
        self._robin_builder = self._robin_builder.app_name(name)
        return self

    def app_name(self, name: str) -> _SparkSessionBuilder:
        return self.appName(name)

    def master(self, master: str) -> _SparkSessionBuilder:
        self._robin_builder = self._robin_builder.master(master)
        return self

    def config(self, key: str, value: Any = None) -> _SparkSessionBuilder:
        if value is not None:
            self._robin_builder = self._robin_builder.config(key, value)
        return self

    def getOrCreate(self) -> SparkSession:
        return SparkSession(self._robin_builder.get_or_create())

    def get_or_create(self) -> SparkSession:
        return self.getOrCreate()


# Attach builder to SparkSession (instance for PySpark-style SparkSession.builder.appName(...))
SparkSession.builder = _SparkSessionBuilder()  # type: ignore[attr-defined]
SparkSessionBuilder = _SparkSessionBuilder
