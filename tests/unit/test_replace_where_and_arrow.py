"""Tests for replaceWhere option and applyInArrow."""

import pytest

from sparkless.sql import SparkSession


@pytest.fixture
def spark():
    session = SparkSession("test_replace_where_arrow")
    yield session


class TestReplaceWhere:
    """Tests for replaceWhere option in DataFrameWriter."""

    def test_replace_where_basic(self, spark):
        """replaceWhere should replace only matching rows."""
        # Write initial data
        data = [
            {"id": 1, "category": "A", "value": 10},
            {"id": 2, "category": "B", "value": 20},
            {"id": 3, "category": "A", "value": 30},
        ]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable("rw_test")

        # Replace only category == 'A' rows with new data
        new_data = [
            {"id": 4, "category": "A", "value": 40},
        ]
        new_df = spark.createDataFrame(new_data)
        new_df.write.mode("overwrite").option(
            "replaceWhere", "category == 'A'"
        ).saveAsTable("rw_test")

        result = spark.table("rw_test").collect()
        result_dicts = sorted([r.asDict() for r in result], key=lambda x: x["id"])

        # category B row (id=2) should be kept, category A rows replaced
        assert len(result_dicts) == 2
        assert result_dicts[0]["id"] == 2
        assert result_dicts[0]["category"] == "B"
        assert result_dicts[1]["id"] == 4
        assert result_dicts[1]["category"] == "A"

    def test_replace_where_no_existing_table(self, spark):
        """replaceWhere on non-existent table should write normally."""
        data = [{"id": 1, "value": 100}]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").option("replaceWhere", "id > 0").saveAsTable(
            "rw_new_table"
        )

        result = spark.table("rw_new_table").collect()
        assert len(result) == 1
        assert result[0].asDict()["id"] == 1

    def test_replace_where_comparison(self, spark):
        """replaceWhere with numeric comparison."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable("rw_num_test")

        new_data = [{"id": 10, "value": 100}]
        new_df = spark.createDataFrame(new_data)
        new_df.write.mode("overwrite").option(
            "replaceWhere", "value >= 20"
        ).saveAsTable("rw_num_test")

        result = spark.table("rw_num_test").collect()
        result_dicts = sorted([r.asDict() for r in result], key=lambda x: x["id"])

        # id=1 (value=10) kept, ids 2,3 replaced with id=10
        assert len(result_dicts) == 2
        assert result_dicts[0]["id"] == 1
        assert result_dicts[1]["id"] == 10

    def test_replace_where_no_matches(self, spark):
        """replaceWhere with no matching rows keeps all existing + adds new."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
        ]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable("rw_nomatch")

        new_data = [{"id": 3, "value": 30}]
        new_df = spark.createDataFrame(new_data)
        new_df.write.mode("overwrite").option(
            "replaceWhere", "value > 100"
        ).saveAsTable("rw_nomatch")

        result = spark.table("rw_nomatch").collect()
        assert len(result) == 3  # All existing kept + new one added


class TestApplyInArrowDataFrame:
    """Tests for DataFrame.applyInArrow."""

    @pytest.fixture(autouse=True)
    def _require_pyarrow(self):
        pytest.importorskip("pyarrow")

    def test_apply_in_arrow_identity(self, spark):
        """applyInArrow with identity function should return same data."""
        data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}]
        df = spark.createDataFrame(data)

        def identity(table):
            return table

        result = df.applyInArrow(identity, schema=df.schema)
        result_data = sorted(
            [r.asDict() for r in result.collect()], key=lambda x: x["id"]
        )

        assert len(result_data) == 2
        assert result_data[0]["id"] == 1
        assert result_data[1]["id"] == 2

    def test_apply_in_arrow_transform(self, spark):
        """applyInArrow should apply transformation."""
        import pyarrow.compute as pc

        data = [{"value": 10}, {"value": 20}]
        df = spark.createDataFrame(data)

        def double_values(table):
            col = table.column("value")
            doubled = pc.multiply(col, 2)
            return table.append_column("doubled", doubled)

        result = df.applyInArrow(double_values, schema="value long, doubled long")
        result_data = sorted(
            [r.asDict() for r in result.collect()], key=lambda x: x["value"]
        )

        assert len(result_data) == 2
        assert result_data[0]["doubled"] == 20
        assert result_data[1]["doubled"] == 40


class TestApplyInArrowGroupedData:
    """Tests for GroupedData.applyInArrow."""

    @pytest.fixture(autouse=True)
    def _require_pyarrow(self):
        pytest.importorskip("pyarrow")

    def test_grouped_apply_in_arrow(self, spark):
        """applyInArrow on grouped data should apply per group."""
        import pyarrow as pa

        data = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 100},
        ]
        df = spark.createDataFrame(data)

        def add_group_sum(table):

            pdf = table.to_pandas()
            pdf["group_sum"] = pdf["value"].sum()
            return pa.Table.from_pandas(pdf)

        result = df.groupBy("category").applyInArrow(
            add_group_sum, schema="category string, value long, group_sum long"
        )
        result_data = sorted(
            [r.asDict() for r in result.collect()],
            key=lambda x: (x["category"], x["value"]),
        )

        assert len(result_data) == 3
        # Category A: sum = 30
        assert result_data[0]["category"] == "A"
        assert result_data[0]["group_sum"] == 30
        assert result_data[1]["category"] == "A"
        assert result_data[1]["group_sum"] == 30
        # Category B: sum = 100
        assert result_data[2]["category"] == "B"
        assert result_data[2]["group_sum"] == 100

    def test_grouped_apply_in_arrow_identity(self, spark):
        """applyInArrow identity on grouped data."""
        data = [
            {"key": "x", "val": 1},
            {"key": "y", "val": 2},
        ]
        df = spark.createDataFrame(data)

        def identity(table):
            return table

        result = df.groupBy("key").applyInArrow(identity, schema=df.schema)
        assert len(result.collect()) == 2
