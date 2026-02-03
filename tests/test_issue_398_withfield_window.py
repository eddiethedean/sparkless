"""
Tests for issue #398: withField with Window function inside struct.

Sparkless previously stored WindowFunction object instead of evaluating it,
and select(edge.apples_div_count) failed with ColumnNotFoundError.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestIssue398WithFieldWindow:
    """Test withField with Window functions (issue #398)."""

    def test_withfield_window_exact_issue(self, spark):
        """Exact scenario from issue #398."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame(
            [
                {"id": "Alice", "apples": 100},
                {"id": "Alice", "apples": 200},
            ]
        )
        df = df.withColumn("edge", F.struct("apples"))
        df = df.withColumn(
            "edge",
            F.col("edge").withField(
                "count", F.count("*").over(Window.partitionBy("id"))
            ),
        )
        df = df.withColumn(
            "edge",
            F.col("edge").withField(
                "apples_div_count",
                F.col("edge.apples") / F.col("edge.count"),
            ),
        )
        df = df.select("id", F.col("edge.apples_div_count"))
        rows = df.collect()
        assert len(rows) == 2
        keys = list(rows[0].asDict().keys())
        val_key = (
            "apples_div_count"
            if "apples_div_count" in keys
            else "edge.apples_div_count"
        )
        vals = {r[val_key] for r in rows}
        assert vals == {50.0, 100.0}

    def test_withfield_window_select_struct(self, spark):
        """Select full struct after withField + window."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        df = spark.createDataFrame(
            [{"id": "x", "v": 10}, {"id": "x", "v": 20}, {"id": "y", "v": 5}]
        )
        df = df.withColumn("s", F.struct("v"))
        df = df.withColumn(
            "s",
            F.col("s").withField("cnt", F.count("*").over(Window.partitionBy("id"))),
        )
        df = df.select("id", F.col("s"))
        rows = df.collect()
        assert len(rows) == 3
        for r in rows:
            s = r["s"]
            assert "v" in s and "cnt" in s
            assert isinstance(s["cnt"], int)
            assert s["cnt"] in (2, 1)  # id=x has 2 rows, id=y has 1
