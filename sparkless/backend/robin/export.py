"""Robin backend export: row-based export without Polars.

DataFrame export uses materialized rows and stdlib/pandas (if available)
for CSV, JSON, and Parquet. to_polars is not supported in v4.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from sparkless.dataframe import DataFrame


def _materialized_rows_and_schema(df: "DataFrame") -> tuple[list[dict[str, Any]], Any]:
    """Return (list of row dicts, schema) after materializing the DataFrame."""
    from sparkless.spark_types import get_row_value

    m = df._materialize_if_lazy()
    schema = m.schema
    rows = []
    for r in m.collect():
        rows.append(
            {name: get_row_value(r, name) for name in schema.fieldNames()}
        )
    return rows, schema


class RobinExporter:
    """Export backend for Robin; no Polars dependency."""

    def to_polars(self, df: "DataFrame") -> Any:
        raise NotImplementedError(
            "v4 Robin backend does not export to Polars; use to_pandas() or collect()."
        )

    def to_pandas(self, df: "DataFrame") -> Any:
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "to_pandas() requires pandas. Install with: pip install pandas"
            ) from None
        rows, schema = _materialized_rows_and_schema(df)
        if not rows:
            return pd.DataFrame(columns=schema.fieldNames())
        return pd.DataFrame(rows, columns=schema.fieldNames())

    def to_parquet(
        self, df: "DataFrame", path: str, compression: str = "snappy"
    ) -> None:
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "to_parquet() requires pandas. Install with: pip install pandas"
            ) from None
        pdf = self.to_pandas(df)
        pdf.to_parquet(path, index=False, compression=compression)

    def to_csv(
        self,
        df: "DataFrame",
        path: str,
        header: bool = True,
        separator: str = ",",
    ) -> None:
        rows, schema = _materialized_rows_and_schema(df)
        names = schema.fieldNames()
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=names, delimiter=separator)
            if header:
                w.writeheader()
            w.writerows(rows)

    def to_json(self, df: "DataFrame", path: str, pretty: bool = False) -> None:
        rows, schema = _materialized_rows_and_schema(df)
        with open(path, "w", encoding="utf-8") as f:
            if pretty:
                json.dump(rows, f, indent=2, default=str)
            else:
                json.dump(rows, f, default=str)
