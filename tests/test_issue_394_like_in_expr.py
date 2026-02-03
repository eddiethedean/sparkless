"""
Tests for issue #394: LIKE and NOT LIKE in F.expr() / filter string expressions.

PySpark supports df.filter("Name like '%TEST%'") and df.filter("Name not like '%TEST%'").
Sparkless SQLExprParser previously raised ParseException for these expressions.
Now parses LIKE and NOT LIKE in F.expr() for PySpark parity.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestIssue394LikeInExpr:
    """Test LIKE and NOT LIKE in filter string expressions (issue #394)."""

    def test_filter_like_exact_issue(self, spark):
        """Exact scenario from issue #394."""
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
            ]
        )
        df1 = df.filter("Name like '%TEST%'")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Al-TEST-ice"

    def test_filter_not_like_exact_issue(self, spark):
        """Exact scenario from issue #394 - NOT LIKE."""
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
            ]
        )
        df2 = df.filter("Name not like '%TEST%'")
        rows = df2.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Bob"

    def test_filter_like_with_show(self, spark):
        """filter + show() as in issue example."""
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
            ]
        )
        df1 = df.filter("Name like '%TEST%'")
        df1.show()
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Al-TEST-ice"

    def test_filter_like_prefix_pattern(self, spark):
        """LIKE with prefix pattern (starts with)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice"},
                {"Name": "Bob"},
                {"Name": "Charlie"},
            ]
        )
        df1 = df.filter("Name like 'A%'")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_filter_like_underscore_wildcard(self, spark):
        """LIKE with _ wildcard (single char)."""
        df = spark.createDataFrame(
            [
                {"Name": "A"},
                {"Name": "AB"},
                {"Name": "ABC"},
            ]
        )
        df1 = df.filter("Name like 'A_'")
        rows = df1.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "AB"

    def test_filter_not_like_multiple_rows(self, spark):
        """NOT LIKE returns multiple rows."""
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
                {"Name": "Charlie"},
            ]
        )
        df2 = df.filter("Name not like '%TEST%'")
        rows = df2.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"Bob", "Charlie"}

    def test_expr_like_standalone(self, spark):
        """F.expr with LIKE used in select/withColumn."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Al-TEST-ice"},
                {"Name": "Bob"},
            ]
        )
        df = df.withColumn("has_test", F.expr("Name like '%TEST%'"))
        rows = df.collect()
        alice = next(r for r in rows if r["Name"] == "Al-TEST-ice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert alice["has_test"] is True
        assert bob["has_test"] is False
