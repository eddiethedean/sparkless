"""
Tests for issue #406: ValueError when combining aggregate + cast to DecimalType + drop.

Sparkless raised Unsupported Polars dtype: Decimal(precision=38, scale=6) because
polars_dtype_to_mock_type did not handle pl.Decimal. Fixed by mapping Polars Decimal
to Sparkless DecimalType and using pl.Decimal in mock_type_to_polars_dtype for DecimalType.
"""


def _decimal_value(val):
    """Normalize collected value to float for assertion (PySpark/Sparkless may return Decimal)."""
    if val is None:
        return None
    return float(val)


class TestIssue406AggregateCastDecimalDrop:
    """Test aggregate + cast(DecimalType) + drop (issue #406)."""

    def test_groupby_agg_cast_decimal_drop(self, spark):
        """Exact scenario from issue #406: groupBy, agg with sum().cast(DecimalType(38,6)), drop."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        DecimalType = imports.DecimalType
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1.234},
                {"Name": "Alice", "Value": 1.234},
                {"Name": "Bob", "Value": 2.345},
            ]
        )
        df = df.groupBy("Name").agg(
            F.sum("Value").cast(DecimalType(38, 6))
        )
        df = df.drop("Name")
        df.show()
        rows = df.collect()
        assert len(rows) == 2
        col_name = list(rows[0].asDict().keys())[0]
        values = [_decimal_value(r[col_name]) for r in rows]
        assert sorted(values) == [2.345, 2.468]
