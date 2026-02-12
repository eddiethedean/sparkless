"""Tests for Robin materializer _expression_to_robin (alias, literal-on-left, etc.)."""

from __future__ import annotations

from typing import Any

import pytest

from sparkless.backend.factory import BackendFactory
from sparkless.functions import F
from sparkless.window import Window
from tests.fixtures.spark_backend import BackendType, get_backend_type

_HAS_ROBIN = BackendFactory._robin_available()  # type: ignore[attr-defined]


@pytest.mark.unit
@pytest.mark.skipif(
    not _HAS_ROBIN, reason="Robin backend requires robin-sparkless to be installed"
)
class TestRobinMaterializerExpressionTranslation:
    """Test _expression_to_robin supports alias and literal-on-left for withColumn (robin-sparkless 0.4.0+)."""

    def teardown_method(self) -> None:
        BackendFactory._robin_available_cache = None

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_alias_expression_robin(self, spark: Any) -> None:
        """WithColumn with col.alias('x') is supported when Robin backend is used."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        # Expression: double "a" and alias as "doubled"
        df = spark.createDataFrame([{"a": 1}, {"a": 2}, {"a": 3}])
        result = df.withColumn("doubled", (F.col("a") * 2).alias("doubled")).collect()
        assert len(result) == 3
        assert result[0]["doubled"] == 2
        assert result[1]["doubled"] == 4
        assert result[2]["doubled"] == 6

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_literal_plus_column_robin(self, spark: Any) -> None:
        """WithColumn with lit(2) + col('x') (literal on left) is supported when Robin backend is used."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([{"x": 10}, {"x": 20}])
        result = df.withColumn("plus_two", F.lit(2) + F.col("x")).collect()
        assert len(result) == 2
        assert result[0]["plus_two"] == 12
        assert result[1]["plus_two"] == 22

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_literal_times_column_robin(self, spark: Any) -> None:
        """WithColumn with lit(3) * col('x') (literal on left) is supported when Robin backend is used."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([{"x": 5}, {"x": 7}])
        result = df.withColumn("triple", F.lit(3) * F.col("x")).collect()
        assert len(result) == 2
        assert result[0]["triple"] == 15
        assert result[1]["triple"] == 21

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_filter_like_robin(self, spark: Any) -> None:
        """Filter with F.col('name').like('al%%') is supported when Robin backend is used (#469)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([("alice",), ("bob",), ("alex",)], ["name"])
        result = df.filter(F.col("name").like("al%")).collect()
        assert len(result) == 2
        names = sorted(r["name"] for r in result)
        assert names == ["alex", "alice"]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_filter_isnull_robin(self, spark: Any) -> None:
        """Filter with F.col('value').isNull() is supported when Robin backend is used (#469)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "value"])
        result = df.filter(F.col("value").isNull()).collect()
        assert len(result) == 1
        assert result[0]["id"] == 2 and result[0]["value"] is None

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_filter_isnotnull_robin(self, spark: Any) -> None:
        """Filter with F.col('value').isNotNull() is supported when Robin backend is used (#469)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "value"])
        result = df.filter(F.col("value").isNotNull()).collect()
        assert len(result) == 2
        ids = sorted(r["id"] for r in result)
        assert ids == [1, 3]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_join_same_name_on_robin(self, spark: Any) -> None:
        """Join with on='id' (same-name keys) uses op path and returns joined rows (#473)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        left = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        right = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "value"])
        result = left.join(right, on="id", how="inner").collect()
        assert len(result) == 2
        by_id = {r["id"]: (r["name"], r["value"]) for r in result}
        assert by_id == {1: ("a", "x"), 2: ("b", "y")}

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_concat_with_literal_separator_robin(self, spark: Any) -> None:
        """concat(col, lit(' '), col) is translated for Robin backend (#474)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame(
            [("alice", "x"), ("bob", "y")],
            ["first_name", "last_name"],
        )
        result = (
            df.withColumn(
                "full_name",
                F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")),
            )
            .orderBy("first_name")
            .select("full_name")
            .collect()
        )
        full_names = [r["full_name"] for r in result]
        assert full_names == ["alice x", "bob y"]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_lit_none_robin(self, spark: Any) -> None:
        """withColumn with F.lit(None) produces a nullable column when Robin backend is used (#470)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        result = df.withColumn("nullable_col", F.lit(None)).collect()
        assert [r["nullable_col"] for r in result] == [None, None]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_to_date_robin(self, spark: Any) -> None:
        """withColumn with to_date(col) is translated for Robin backend (#470)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame(
            [(1, "2024-01-15"), (2, "2024-02-20")], ["id", "date_str"]
        )
        result = (
            df.withColumn("dt", F.to_date(F.col("date_str"))).orderBy("id").collect()
        )
        assert [str(r["dt"]) for r in result] == ["2024-01-15", "2024-02-20"]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_to_timestamp_robin(self, spark: Any) -> None:
        """withColumn with to_timestamp(col) is translated for Robin backend (#470)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame(
            [(1, "2024-01-15 12:34:56")],
            ["id", "ts_str"],
        )
        result = df.withColumn("ts", F.to_timestamp(F.col("ts_str"))).collect()
        # String representation should contain the date part at least
        assert "2024-01-15" in str(result[0]["ts"])

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_regexp_replace_robin(self, spark: Any) -> None:
        """withColumn with regexp_replace is translated for Robin backend (#470)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([("a.b.123",), ("x.y.456",)], ["text"])
        result = (
            df.withColumn("cleaned", F.regexp_replace(F.col("text"), r"\\.\\d+", ""))
            .orderBy("text")
            .collect()
        )
        cleaned = [r["cleaned"] for r in result]
        assert cleaned == ["a.b", "x.y"]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_split_with_limit_robin(self, spark: Any) -> None:
        """withColumn with split(col, ',', -1) is translated for Robin backend (#470)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([("a,b,c",), ("x,,z",)], ["value"])
        result = (
            df.withColumn("arr", F.split(F.col("value"), ",", -1))
            .orderBy("value")
            .collect()
        )
        assert result[0]["arr"] == ["a", "b", "c"]
        assert result[1]["arr"] == ["x", "", "z"]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_round_with_scale_robin(self, spark: Any) -> None:
        """withColumn with round(col, scale) is translated for Robin backend (#470)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([(3.14159,), (2.71828,)], ["val"])
        result = df.withColumn("r", F.round(F.col("val"), 2)).orderBy("val").collect()
        rounded = [r["r"] for r in result]
        assert rounded == [2.72, 3.14]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_with_column_log10_robin(self, spark: Any) -> None:
        """withColumn with log10(col) is translated for Robin backend (#470)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([(10.0,), (100.0,)], ["val"])
        result = (
            df.withColumn("log10_val", F.log10(F.col("val"))).orderBy("val").collect()
        )
        values = [r["log10_val"] for r in result]
        assert values[0] == pytest.approx(1.0, rel=1e-6)
        assert values[1] == pytest.approx(2.0, rel=1e-6)

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_array_and_explode_robin(self, spark: Any) -> None:
        """array(...) and explode(array_col) are translated for Robin backend (#470)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
        arr_df = df.withColumn("arr", F.array(F.col("a"), F.col("b"), F.col("c")))
        exploded = arr_df.select(F.explode(F.col("arr")).alias("v")).collect()
        values = sorted(r["v"] for r in exploded)
        assert values == [1, 2, 3, 4, 5, 6]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_window_plus_literal_robin(self, spark: Any) -> None:
        """withColumn with row_number().over(w) + 10 is one Robin expression (#471)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([("a", 10), ("a", 20), ("b", 5)], ["dept", "salary"])
        w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
        result = (
            df.withColumn("row_plus_10", F.row_number().over(w) + 10)
            .orderBy("dept", "salary")
            .collect()
        )
        # a: row_number 1,2 -> 11,12; b: row_number 1 -> 11
        row_plus = [r["row_plus_10"] for r in result]
        assert 11 in row_plus and 12 in row_plus

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_window_alias_robin(self, spark: Any) -> None:
        """withColumn with row_number().over(w) or .alias('rn') produces one column (#471)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([("a", 10), ("a", 20), ("b", 5)], ["dept", "salary"])
        w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
        result = (
            df.withColumn("rn", F.row_number().over(w))
            .orderBy("dept", "salary")
            .collect()
        )
        assert all("rn" in r for r in result)
        assert [r["rn"] for r in result] == [1, 2, 1]

    @pytest.mark.backend("robin")  # type: ignore[untyped-decorator]
    def test_case_when_in_select_robin(self, spark: Any) -> None:
        """CaseWhen in select (plan path) produces one column (#472)."""
        if get_backend_type() != BackendType.ROBIN:
            pytest.skip("Robin backend only")
        df = spark.createDataFrame([(1, 10), (2, 50), (3, 90)], ["id", "score"])
        result = (
            df.select(
                F.when(F.col("score") >= 50, "pass").otherwise("fail").alias("result")
            )
            .orderBy("id")
            .collect()
        )
        assert [r["result"] for r in result] == ["fail", "pass", "pass"]


@pytest.mark.unit
@pytest.mark.skipif(
    not _HAS_ROBIN, reason="Robin backend requires robin-sparkless to be installed"
)
class TestRobinMaterializerEmptySchema:
    """Empty schema edge case (#475)."""

    def teardown_method(self) -> None:
        BackendFactory._robin_available_cache = None

    def test_empty_schema_no_operations_returns_rows(self) -> None:
        """materialize([], empty_schema, []) returns [] without raising."""
        from sparkless.spark_types import StructType

        mat = BackendFactory.create_materializer("robin")
        empty_schema = StructType([])
        result = mat.materialize([], empty_schema, [])
        assert result == []

    def test_empty_schema_with_operations_raises(self) -> None:
        """materialize([], empty_schema, [(op, ...)]) raises clear ValueError."""
        from sparkless.spark_types import StructType

        mat = BackendFactory.create_materializer("robin")
        empty_schema = StructType([])
        with pytest.raises(ValueError, match="does not support empty schema when"):
            mat.materialize([], empty_schema, [("limit", 5)])
