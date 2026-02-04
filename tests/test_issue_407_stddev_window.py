"""
Tests for issue #407: stddev() over Window returns None in Sparkless.

PySpark returns the correct sample standard deviation per partition; Sparkless
previously returned None because the Polars window handler did not implement
STDDEV/STDDEV_SAMP. Fixed by adding these in window_handler.py.
"""


class TestIssue407StddevWindow:
    """Test stddev() over Window (issue #407)."""

    def test_stddev_over_window_partition(self, spark):
        """Exact scenario from issue #407: F.stddev('Value').over(w) with partitionBy ID."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        df = spark.createDataFrame(
            [
                {"ID": 1, "Value": 0},
                {"ID": 1, "Value": 10},
                {"ID": 1, "Value": 5},
                {"ID": 1, "Value": 2},
            ]
        )
        w = Window.partitionBy(F.col("ID"))
        df = df.withColumn("stddev", F.stddev("Value").over(w))
        rows = df.collect()
        assert len(rows) == 4
        stddevs = [r["stddev"] for r in rows]
        # All rows in same partition -> same sample std (PySpark: ~4.349...)
        assert all(s is not None for s in stddevs), "stddev must not be None"
        assert len(set(stddevs)) == 1
        assert 4.0 <= stddevs[0] <= 5.0

    def test_stddev_over_window_multiple_partitions(self, spark):
        """stddev over window with multiple partitions; each partition has same stddev value."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        df = spark.createDataFrame(
            [
                {"g": "a", "x": 1},
                {"g": "a", "x": 3},
                {"g": "b", "x": 10},
                {"g": "b", "x": 20},
            ]
        )
        w = Window.partitionBy("g")
        df = df.withColumn("stddev", F.stddev("x").over(w))
        rows = df.collect()
        assert len(rows) == 4
        by_g = {}
        for r in rows:
            g, s = r["g"], r["stddev"]
            assert s is not None
            by_g.setdefault(g, []).append(s)
        assert len(by_g["a"]) == 2 and len(by_g["b"]) == 2
        assert by_g["a"][0] == by_g["a"][1]  # same partition -> same stddev
        assert by_g["b"][0] == by_g["b"][1]
        # a: sample std([1,3]) = sqrt(2) ~ 1.41; b: sample std([10,20]) = 7.07...
        assert 1.0 <= by_g["a"][0] <= 2.0
        assert 6.0 <= by_g["b"][0] <= 8.0
