"""
Tests for issue #437: F.mean() on string columns with numeric content.

PySpark interprets string columns as numeric for mean/avg; sparkless previously
raised TypeError: can't convert type 'str' to numerator/denominator when using
F.mean() on string columns. Sparkless now coerces string columns to numeric
(PySpark parity).
"""

from tests.fixtures.spark_imports import get_spark_imports


def _norm(val):
    """Normalize for backend-agnostic assertion."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if isinstance(val, float) else int(val)
    return val


class TestIssue437MeanStringColumn:
    """Test F.mean() on string columns with numeric content (issue #437)."""

    def test_mean_string_column_exact_issue_scenario(self, spark):
        """Exact scenario from issue #437: F.mean() on string X and int Y."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "X": "1", "Y": 1},
                {"Name": "Alice", "X": "2", "Y": 2},
                {"Name": "Alice", "X": "3", "Y": 3},
                {"Name": "Bob", "X": "4", "Y": 4},
            ]
        )
        df = df.groupBy("Name").agg(
            F.mean(F.col("X")).alias("avg(X)"),
            F.mean(F.col("Y")).alias("avg(Y)"),
        )
        rows = df.collect()
        assert len(rows) == 2
        alice = next(r for r in rows if r["Name"] == "Alice")
        bob = next(r for r in rows if r["Name"] == "Bob")
        assert _norm(alice["avg(X)"]) == 2.0
        assert _norm(alice["avg(Y)"]) == 2.0
        assert _norm(bob["avg(X)"]) == 4.0
        assert _norm(bob["avg(Y)"]) == 4.0

    def test_mean_string_column_groupby(self, spark):
        """F.mean() on string column with groupBy."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "10"},
                {"Type": "A", "Value": "20"},
                {"Type": "A", "Value": "30"},
                {"Type": "B", "Value": "5"},
                {"Type": "B", "Value": "15"},
            ]
        )
        df = df.groupBy("Type").agg(F.mean("Value"))
        rows = df.collect()
        a_row = next(r for r in rows if r["Type"] == "A")
        b_row = next(r for r in rows if r["Type"] == "B")
        assert _norm(a_row["avg(Value)"]) == 20.0
        assert _norm(b_row["avg(Value)"]) == 10.0

    def test_mean_string_column_decimal_values(self, spark):
        """F.mean() on string column with decimal-like values."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"Type": "A", "Value": "1.5"},
                {"Type": "A", "Value": "2.5"},
                {"Type": "A", "Value": "3.5"},
            ]
        )
        df = df.groupBy("Type").agg(F.mean("Value"))
        rows = df.collect()
        assert len(rows) == 1
        assert abs(_norm(rows[0]["avg(Value)"]) - 2.5) < 1e-9
