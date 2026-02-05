"""
Tests for issue #422: fillna(0.0) fails to replace None in calculated numeric columns and integer columns.

- fillna(0.0, subset=["V3"]) did not fill None in column V3 (V1/V2) when V3 was a calculated float column.
- fillna(0.0) did not fill None in integer column V1 when using float fill value; only float columns were filled.

PySpark allows fillna(0.0) to fill both integer and float columns (coerces as needed).
"""

import pytest


class TestIssue422FillnaFloat:
    """Test fillna(0.0) with calculated columns and integer columns."""

    def test_fillna_float_subset_calculated_column(self, spark):
        """fillna(0.0, subset=['V3']) fills None in calculated float column V3 (V1/V2)."""
        from sparkless.sql import functions as F

        df = spark.createDataFrame([
            {"Name": "Alice", "V1": 1, "V2": 0},
            {"Name": "Bob", "V1": 0, "V2": 2},
            {"Name": "Charlie", "V1": None, "V2": 0},
            {"Name": "Delta", "V1": 0, "V2": None},
        ])
        df = df.withColumn("V3", F.col("V1") / F.col("V2"))
        df = df.fillna(0.0, subset=["V3"])

        rows = df.collect()
        assert len(rows) == 4
        # Alice: 1/0 -> None -> filled with 0.0
        assert rows[0]["Name"] == "Alice"
        assert rows[0]["V3"] == 0.0
        # Bob: 0/2 -> 0.0
        assert rows[1]["Name"] == "Bob"
        assert rows[1]["V3"] == 0.0
        # Charlie: None/0 -> None -> filled with 0.0
        assert rows[2]["Name"] == "Charlie"
        assert rows[2]["V3"] == 0.0
        # Delta: 0/None -> None -> filled with 0.0
        assert rows[3]["Name"] == "Delta"
        assert rows[3]["V3"] == 0.0

    def test_fillna_float_fills_integer_column(self, spark):
        """fillna(0.0) fills None in both integer (V1) and float (V2) columns (PySpark behavior)."""
        df = spark.createDataFrame([
            {"Name": "Alice", "V1": 1, "V2": 0.1},
            {"Name": "Bob", "V1": 0, "V2": 2.0},
            {"Name": "Charlie", "V1": None, "V2": 3.5},
            {"Name": "Delta", "V1": 0, "V2": None},
        ])
        df = df.fillna(0.0)

        rows = df.collect()
        assert len(rows) == 4
        assert rows[0]["V1"] == 1 and rows[0]["V2"] == 0.1
        assert rows[1]["V1"] == 0 and rows[1]["V2"] == 2.0
        # Charlie: V1 was None -> filled with 0.0
        assert rows[2]["V1"] == 0.0 and rows[2]["V2"] == 3.5
        # Delta: V2 was None -> filled with 0.0
        assert rows[3]["V1"] == 0 and rows[3]["V2"] == 0.0

    def test_fillna_float_subset_single_int_column(self, spark):
        """fillna(0.0, subset=['V1']) fills None in integer column V1."""
        df = spark.createDataFrame([
            {"V1": 1, "V2": 10},
            {"V1": None, "V2": 20},
        ])
        result = df.fillna(0.0, subset=["V1"])
        rows = result.collect()
        assert rows[0]["V1"] == 1 and rows[0]["V2"] == 10
        assert rows[1]["V1"] == 0.0 and rows[1]["V2"] == 20
