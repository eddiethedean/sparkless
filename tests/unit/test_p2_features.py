"""Tests for P2 features: optimize builder, Column conditions in delta, None schema inference, reader options."""

import json
import os
import tempfile

import pytest

from sparkless.sql import SparkSession
from sparkless import functions as F
from sparkless.delta import DeltaTable
from sparkless.spark_types import (
    StringType,
    LongType,
)


@pytest.fixture
def spark():
    session = SparkSession("test_p2")
    yield session


# ---------------------------------------------------------------
# Feature 1: Delta optimize().executeCompaction() / .zOrderBy()
# ---------------------------------------------------------------


class TestDeltaOptimizeBuilder:
    def test_execute_compaction(self, spark):
        data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        spark.createDataFrame(data).write.format("delta").saveAsTable(
            "default.opt_test"
        )
        dt = DeltaTable.forName(spark, "default.opt_test")
        result = dt.optimize().executeCompaction()
        # Should return an empty DataFrame
        assert result.count() == 0

    def test_z_order_by(self, spark):
        data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        spark.createDataFrame(data).write.format("delta").saveAsTable(
            "default.zorder_test"
        )
        dt = DeltaTable.forName(spark, "default.zorder_test")
        result = dt.optimize().zOrderBy("id", "name")
        assert result.count() == 0

    def test_original_data_unchanged(self, spark):
        data = [{"id": 1}, {"id": 2}]
        spark.createDataFrame(data).write.format("delta").saveAsTable(
            "default.opt_data"
        )
        dt = DeltaTable.forName(spark, "default.opt_data")
        dt.optimize().executeCompaction()
        # Original data should be untouched
        rows = [r["id"] for r in dt.toDF().collect()]
        assert sorted(rows) == [1, 2]


# ---------------------------------------------------------------
# Feature 2: Column conditions in Delta delete/update
# ---------------------------------------------------------------


class TestDeltaColumnConditions:
    def test_delete_with_column_condition(self, spark):
        data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]
        spark.createDataFrame(data).write.format("delta").saveAsTable("default.del_col")
        dt = DeltaTable.forName(spark, "default.del_col")
        dt.delete(F.col("id") == 2)
        remaining = sorted([r["id"] for r in dt.toDF().collect()])
        assert remaining == [1, 3]

    def test_update_with_column_condition(self, spark):
        data = [{"id": 1, "status": "active"}, {"id": 2, "status": "active"}]
        spark.createDataFrame(data).write.format("delta").saveAsTable("default.upd_col")
        dt = DeltaTable.forName(spark, "default.upd_col")
        dt.update(F.col("id") == 1, {"status": F.lit("inactive")})
        rows = {r["id"]: r["status"] for r in dt.toDF().collect()}
        assert rows[1] == "inactive"
        assert rows[2] == "active"

    def test_update_with_column_value_expression(self, spark):
        data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}]
        spark.createDataFrame(data).write.format("delta").saveAsTable(
            "default.upd_col_val"
        )
        dt = DeltaTable.forName(spark, "default.upd_col_val")
        dt.update(F.col("id") == 1, {"value": F.lit(99)})
        rows = {r["id"]: r["value"] for r in dt.toDF().collect()}
        assert rows[1] == 99
        assert rows[2] == 20

    def test_delete_no_condition_deletes_all(self, spark):
        data = [{"id": 1}, {"id": 2}]
        spark.createDataFrame(data).write.format("delta").saveAsTable("default.del_all")
        dt = DeltaTable.forName(spark, "default.del_all")
        dt.delete()
        assert dt.toDF().count() == 0


# ---------------------------------------------------------------
# Feature 3: None column defaults to StringType
# ---------------------------------------------------------------


class TestNoneColumnInference:
    def test_all_none_column_defaults_to_string(self, spark):
        data = [{"id": 1, "value": None}, {"id": 2, "value": None}]
        df = spark.createDataFrame(data)
        schema = df.schema
        value_field = [f for f in schema.fields if f.name == "value"][0]
        assert isinstance(value_field.dataType, StringType)

    def test_mixed_none_and_values(self, spark):
        data = [{"id": 1, "value": None}, {"id": 2, "value": 42}]
        df = spark.createDataFrame(data)
        schema = df.schema
        value_field = [f for f in schema.fields if f.name == "value"][0]
        # Should infer from the non-None value
        assert isinstance(value_field.dataType, LongType)

    def test_all_none_multiple_columns(self, spark):
        data = [{"a": None, "b": None}]
        df = spark.createDataFrame(data)
        for f in df.schema.fields:
            assert isinstance(f.dataType, StringType)


# ---------------------------------------------------------------
# Feature 4: Advanced reader options
# ---------------------------------------------------------------


class TestReaderOptions:
    def test_json_multiline(self, spark):
        data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            # Write as a single JSON array (not NDJSON)
            json.dump(data, f, indent=2)
            path = f.name
        try:
            df = spark.read.option("multiLine", "true").json(path)
            assert df.count() == 2
            ids = sorted([r["id"] for r in df.collect()])
            assert ids == [1, 2]
        finally:
            os.unlink(path)

    def test_json_ndjson_default(self, spark):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            f.write('{"id": 1}\n{"id": 2}\n')
            path = f.name
        try:
            df = spark.read.json(path)
            assert df.count() == 2
        finally:
            os.unlink(path)

    def test_json_encoding(self, spark):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="latin-1"
        ) as f:
            f.write('{"name": "Jos\\u00e9"}\n')
            path = f.name
        try:
            df = spark.read.option("encoding", "latin-1").json(path)
            rows = df.collect()
            assert len(rows) == 1
        finally:
            os.unlink(path)

    def test_infer_schema_option_accepted_silently(self, spark):
        """inferSchema option should be accepted without error."""
        # Just verify the option can be set without raising
        reader = spark.read.option("inferSchema", "true")
        assert reader._options["inferSchema"] == "true"

    def test_encoding_option_stored(self, spark):
        """encoding option should be stored for later use."""
        reader = spark.read.option("encoding", "latin-1")
        assert reader._options["encoding"] == "latin-1"
