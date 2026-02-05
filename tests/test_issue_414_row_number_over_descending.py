"""
Tests for issue #414: row_number().over(Window.partitionBy(...).orderBy(F.desc(...)))
must not raise TypeError: over() got an unexpected keyword argument 'descending'.

Polars over() expects descending: bool and supports it only from 1.22+.
Sparkless supports polars>=0.20.0, so we must handle older Polars and avoid
passing List[bool] where a single bool is expected.
"""

import pytest

from sparkless.session import SparkSession
from sparkless.window import Window
from sparkless import functions as F


class TestIssue414RowNumberOverDescending:
    """Regression tests for row_number().over() with orderBy(F.desc(...))."""

    def test_row_number_over_partition_order_desc(self):
        """row_number().over(partitionBy, orderBy(F.desc)) must not raise."""
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(
            [("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 2)],
            ["id", "value"],
        )
        window = Window.partitionBy("id").orderBy(F.desc("value"))
        result = df.select(
            F.row_number().over(window).alias("rn"), "id", "value"
        ).collect()
        assert len(result) == 5
        # id='a': value 3,2,1 -> rn 1,2,3 (descending order)
        a_rows = {r["value"]: r["rn"] for r in result if r["id"] == "a"}
        assert a_rows[3] == 1
        assert a_rows[2] == 2
        assert a_rows[1] == 3
        # id='b': value 2,1 -> rn 1,2
        b_rows = {r["value"]: r["rn"] for r in result if r["id"] == "b"}
        assert b_rows[2] == 1
        assert b_rows[1] == 2

    def test_row_number_over_order_desc_no_partition(self):
        """row_number().over(orderBy(F.desc)) without partition must not raise."""
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(
            [(1,), (2,), (3,)],
            ["value"],
        )
        window = Window.orderBy(F.desc("value"))
        result = df.select(
            F.row_number().over(window).alias("rn"), "value"
        ).collect()
        assert len(result) == 3
        assert result[0]["rn"] == 1 and result[0]["value"] == 3
        assert result[1]["rn"] == 2 and result[1]["value"] == 2
        assert result[2]["rn"] == 3 and result[2]["value"] == 1

    def test_row_number_over_partition_order_asc(self):
        """row_number().over(partitionBy, orderBy(F.asc)) must still work."""
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 1)],
            ["id", "value"],
        )
        window = Window.partitionBy("id").orderBy(F.asc("value"))
        result = df.select(
            F.row_number().over(window).alias("rn"), "id", "value"
        ).collect()
        assert len(result) == 3
        assert result[0]["rn"] == 1 and result[0]["value"] == 1
        assert result[1]["rn"] == 2 and result[1]["value"] == 2
        assert result[2]["rn"] == 1 and result[2]["value"] == 1
