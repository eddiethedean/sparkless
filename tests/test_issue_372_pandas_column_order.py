"""
Tests for issue #372: Column order when creating DataFrame from Pandas vs list of dicts.

PySpark: createDataFrame(pandas_df) preserves column order as-given; createDataFrame(list_of_dicts)
sorts columns alphabetically. Sparkless now matches both behaviors.
"""

import pytest


class TestIssue372PandasColumnOrder:
    """Test column ordering: Pandas preserves order, list-of-dicts alphabetical."""

    def test_create_dataframe_from_pandas_preserves_column_order(self, spark):
        """Exact scenario from issue #372: Pandas input preserves order (ZZZ, AAA, PPP)."""
        pd = pytest.importorskip("pandas")

        pdf = pd.DataFrame(
            {
                "ZZZ-FirstColumn": ["12", "34", "56", "78"],
                "AAA-SecondColumn": [10, 20, 30, 40],
                "PPP-ThirdColumn": ["ab", "cd", "ef", "gh"],
            }
        )
        df = spark.createDataFrame(pdf)
        cols = df.columns
        assert cols == ["ZZZ-FirstColumn", "AAA-SecondColumn", "PPP-ThirdColumn"], (
            f"Expected Pandas column order, got {cols}"
        )
        rows = df.collect()
        assert len(rows) == 4
        assert rows[0]["ZZZ-FirstColumn"] == "12"
        assert rows[0]["AAA-SecondColumn"] == 10
        assert rows[0]["PPP-ThirdColumn"] == "ab"

    def test_create_dataframe_from_list_of_dicts_alphabetical(self, spark):
        """List of dicts: columns sorted alphabetically (PySpark behavior)."""
        data = [
            {"ZZZ-FirstColumn": "12", "AAA-SecondColumn": 10, "PPP-ThirdColumn": "ab"},
            {"ZZZ-FirstColumn": "34", "AAA-SecondColumn": 20, "PPP-ThirdColumn": "cd"},
        ]
        df = spark.createDataFrame(data)
        cols = df.columns
        assert cols == ["AAA-SecondColumn", "PPP-ThirdColumn", "ZZZ-FirstColumn"], (
            f"Expected alphabetical order, got {cols}"
        )
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["AAA-SecondColumn"] == 10
        assert rows[0]["ZZZ-FirstColumn"] == "12"

    def test_pandas_column_order_show(self, spark):
        """Pandas DataFrame: show() displays columns in Pandas order."""
        pd = pytest.importorskip("pandas")

        pdf = pd.DataFrame({"B-second": [1, 2], "A-first": [10, 20]})
        df = spark.createDataFrame(pdf)
        df.show()
        assert df.columns == ["B-second", "A-first"]
