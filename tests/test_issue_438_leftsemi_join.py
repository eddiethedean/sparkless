"""
Tests for issue #438: leftsemi join incorrectly returns columns from right DataFrame.

PySpark leftsemi/left_semi join returns only columns from the left DataFrame.
Sparkless was incorrectly including right-side columns in the result.
"""


class TestIssue438LeftsemiJoin:
    """Test leftsemi join returns only left DataFrame columns (issue #438)."""

    def test_leftsemi_join_excludes_right_columns(self, spark):
        """Exact scenario from issue #438: leftsemi should not include V2."""
        df1 = spark.createDataFrame(
            [
                {"Name": "Alice", "V1": 1},
                {"Name": "Bob", "V1": 2},
            ]
        )
        df2 = spark.createDataFrame(
            [
                {"Name": "Alice", "V2": 3},
            ]
        )
        result = df1.join(df2, on="Name", how="leftsemi")
        rows = result.collect()

        assert len(rows) == 1
        row = rows[0]
        assert row["Name"] == "Alice"
        assert row["V1"] == 1
        # V2 must NOT be in result (leftsemi returns only left columns)
        assert "V2" not in row

    def test_left_semi_join_excludes_right_columns(self, spark):
        """left_semi (underscore) alias also excludes right columns."""
        df1 = spark.createDataFrame(
            [{"A": 1, "B": 10}, {"A": 2, "B": 20}, {"A": 3, "B": 30}]
        )
        df2 = spark.createDataFrame([{"A": 1, "C": 100}, {"A": 3, "C": 300}])
        result = df1.join(df2, on="A", how="left_semi")
        rows = result.collect()

        assert len(rows) == 2  # A=1 and A=3 match
        for row in rows:
            assert "A" in row
            assert "B" in row
            assert "C" not in row
