"""
Tests for issue #439: array_distinct returns non-unique values as wrong data type.

Sparkless was returning:
- ['1', '1', '2', '2', '3'] (strings, not deduplicated)

Instead of PySpark behavior:
- [1, 2, 3] (integers, deduplicated, original element type preserved)

Run with PySpark first: SPARKLESS_TEST_BACKEND=pyspark pytest tests/test_issue_439_array_distinct.py -v
Then mock: pytest tests/test_issue_439_array_distinct.py -v
"""

from tests.fixtures.spark_imports import get_spark_imports


def _row_val(row: object, key: str) -> object:
    """Get value from Row or dict (PySpark returns Row, mock may return dict)."""
    if hasattr(row, "__getitem__"):
        return row[key]
    return getattr(row, key, None)


class TestIssue439ArrayDistinct:
    """Test array_distinct preserves element type and deduplicates correctly."""

    def test_array_distinct_integer_arrays_preserves_type(self, spark, spark_backend):
        """array_distinct on int arrays returns int array, not string array."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": [1, 1, 2, 2, 3]},
                {"Name": "Bob", "Value": [4, 4, 5, 5, 6]},
            ]
        )
        df = df.withColumn("ValueSet", F.array_distinct("Value"))

        rows = df.collect()
        assert len(rows) == 2

        # Alice: [1,1,2,2,3] -> [1,2,3] (order may vary in PySpark)
        alice_valset = _row_val(rows[0], "ValueSet")
        assert alice_valset is not None
        assert set(alice_valset) == {1, 2, 3}
        assert len(alice_valset) == 3
        # Element type must be int, not str
        for v in alice_valset:
            assert isinstance(v, int), f"Expected int, got {type(v).__name__}: {v!r}"

        # Bob: [4,4,5,5,6] -> [4,5,6]
        bob_valset = _row_val(rows[1], "ValueSet")
        assert bob_valset is not None
        assert set(bob_valset) == {4, 5, 6}
        assert len(bob_valset) == 3
        for v in bob_valset:
            assert isinstance(v, int), f"Expected int, got {type(v).__name__}: {v!r}"

    def test_array_distinct_string_arrays(self, spark, spark_backend):
        """array_distinct on string arrays returns deduplicated string array."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"tags": ["a", "a", "b", "b", "c"]},
                {"tags": ["x", "x", "x"]},
            ]
        )
        df = df.withColumn("unique_tags", F.array_distinct("tags"))

        rows = df.collect()
        assert len(rows) == 2

        row0 = _row_val(rows[0], "unique_tags")
        assert row0 is not None
        assert set(row0) == {"a", "b", "c"}
        assert len(row0) == 3
        for v in row0:
            assert isinstance(v, str), f"Expected str, got {type(v).__name__}: {v!r}"

        row1 = _row_val(rows[1], "unique_tags")
        assert row1 is not None
        assert list(row1) == ["x"]
        assert isinstance(row1[0], str)

    def test_array_distinct_with_column_expr(self, spark, spark_backend):
        """array_distinct(F.col('Value')) works same as string column name."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": [10, 10, 20]},
            ]
        )
        result = df.select(F.array_distinct(F.col("Value")).alias("distinct_values"))

        rows = result.collect()
        assert len(rows) == 1
        vals = _row_val(rows[0], "distinct_values")
        assert vals is not None
        assert set(vals) == {10, 20}
        assert len(vals) == 2
        for v in vals:
            assert isinstance(v, int)

    def test_array_distinct_empty_array(self, spark, spark_backend):
        """array_distinct on empty array returns empty array."""
        F = get_spark_imports(spark_backend).F
        StructType = get_spark_imports(spark_backend).StructType
        StructField = get_spark_imports(spark_backend).StructField
        ArrayType = get_spark_imports(spark_backend).ArrayType
        IntegerType = get_spark_imports(spark_backend).IntegerType

        schema = StructType([StructField("arr", ArrayType(IntegerType()))])
        df = spark.createDataFrame([{"arr": []}], schema=schema)
        result = df.withColumn("unique_arr", F.array_distinct("arr"))

        rows = result.collect()
        assert len(rows) == 1
        vals = _row_val(rows[0], "unique_arr")
        assert vals is not None
        assert vals == []

    def test_array_distinct_single_element(self, spark, spark_backend):
        """array_distinct on single-element array returns same."""
        F = get_spark_imports(spark_backend).F

        df = spark.createDataFrame(
            [
                {"arr": [42]},
            ]
        )
        result = df.withColumn("unique_arr", F.array_distinct("arr"))

        rows = result.collect()
        assert len(rows) == 1
        vals = _row_val(rows[0], "unique_arr")
        assert vals == [42]
        assert isinstance(vals[0], int)
