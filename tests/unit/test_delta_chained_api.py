"""Tests for the PySpark-standard Delta Merge chained API.

Covers:
- whenMatched().update() / .updateAll() / .delete()
- whenNotMatched().insert() / .insertAll()
- whenNotMatchedBySource().update() / .delete()
- Conditional variants with condition arguments
- Backward compatibility with the flat API
"""

import pytest

from sparkless import SparkSession
from sparkless.delta import DeltaTable


@pytest.fixture
def spark():
    session = SparkSession("delta_chained_api_suite")
    yield session
    session.stop()


def _setup_merge(spark, target_data, source_data, table_name="chained_tbl"):
    """Helper: create target table + source DataFrame and return (delta_table, source_df)."""
    spark.createDataFrame(target_data).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(table_name)
    delta_table = DeltaTable.forName(spark, table_name).alias("t")
    source_df = spark.createDataFrame(source_data).alias("s")
    return delta_table, source_df, table_name


class TestChainedWhenMatchedUpdate:
    def test_update_all(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": "old"}],
            [{"id": 1, "val": "new"}],
        )
        dt.merge(src, "t.id = s.id").whenMatched().updateAll().execute()

        rows = spark.table(tbl).collect()
        assert rows[0].val == "new"

    def test_update_set(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": "old"}],
            [{"id": 1, "val": "new"}],
        )
        dt.merge(src, "t.id = s.id").whenMatched().update(
            set={"val": "s.val"}
        ).execute()

        rows = spark.table(tbl).collect()
        assert rows[0].val == "new"

    def test_update_with_condition(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": 10}, {"id": 2, "val": 20}],
            [{"id": 1, "val": 99}, {"id": 2, "val": 99}],
        )
        (
            dt.merge(src, "t.id = s.id")
            .whenMatched("s.val > t.val")
            .updateAll()
            .execute()
        )

        by_id = {r.id: r.val for r in spark.table(tbl).collect()}
        assert by_id[1] == 99  # updated (99 > 10)
        assert by_id[2] == 99  # updated (99 > 20)


class TestChainedWhenMatchedDelete:
    def test_delete_all_matched(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}],
            [{"id": 1, "val": "x"}],
        )
        dt.merge(src, "t.id = s.id").whenMatched().delete().execute()

        rows = spark.table(tbl).collect()
        assert len(rows) == 1
        assert rows[0].id == 2

    def test_delete_with_condition(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": 10}, {"id": 2, "val": 20}],
            [{"id": 1, "val": 5}, {"id": 2, "val": 25}],
        )
        (dt.merge(src, "t.id = s.id").whenMatched("s.val > t.val").delete().execute())

        by_id = {r.id: r.val for r in spark.table(tbl).collect()}
        assert 1 in by_id  # not deleted (5 < 10)
        assert 2 not in by_id  # deleted (25 > 20)


class TestChainedWhenNotMatchedInsert:
    def test_insert_all(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": "a"}],
            [{"id": 1, "val": "x"}, {"id": 2, "val": "y"}],
        )
        (
            dt.merge(src, "t.id = s.id")
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute()
        )

        by_id = {r.id: r.val for r in spark.table(tbl).collect()}
        assert by_id[1] == "x"
        assert by_id[2] == "y"

    def test_insert_values(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": "a"}],
            [{"id": 2, "val": "b"}],
        )
        (
            dt.merge(src, "t.id = s.id")
            .whenNotMatched()
            .insert(values={"id": "s.id", "val": "s.val"})
            .execute()
        )

        by_id = {r.id: r.val for r in spark.table(tbl).collect()}
        assert by_id[1] == "a"
        assert by_id[2] == "b"


class TestChainedWhenNotMatchedBySource:
    def test_delete_unmatched_target_rows(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}],
            [{"id": 1, "val": "x"}],
        )
        (
            dt.merge(src, "t.id = s.id")
            .whenMatched()
            .updateAll()
            .whenNotMatchedBySource()
            .delete()
            .execute()
        )

        rows = spark.table(tbl).collect()
        assert len(rows) == 1
        assert rows[0].id == 1
        assert rows[0].val == "x"

    def test_update_unmatched_target_rows(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}],
            [{"id": 1, "val": "x"}],
        )
        (
            dt.merge(src, "t.id = s.id")
            .whenMatched()
            .updateAll()
            .whenNotMatchedBySource()
            .update(set={"val": "'archived'"})
            .execute()
        )

        by_id = {r.id: r.val for r in spark.table(tbl).collect()}
        assert by_id[1] == "x"
        assert by_id[2] == "archived"

    def test_conditional_delete_unmatched(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": 10}, {"id": 2, "val": 20}, {"id": 3, "val": 30}],
            [{"id": 1, "val": 99}],
        )
        (
            dt.merge(src, "t.id = s.id")
            .whenMatched()
            .updateAll()
            .whenNotMatchedBySource("t.val < 25")
            .delete()
            .execute()
        )

        by_id = {r.id: r.val for r in spark.table(tbl).collect()}
        assert by_id[1] == 99  # matched, updated
        assert 2 not in by_id  # unmatched, val=20 < 25 -> deleted
        assert by_id[3] == 30  # unmatched, val=30 >= 25 -> kept


class TestChainedApiMultipleActions:
    def test_matched_update_plus_not_matched_insert(self, spark):
        """Full merge: update matched, insert not matched."""
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": "old"}, {"id": 2, "val": "keep"}],
            [{"id": 1, "val": "new"}, {"id": 3, "val": "added"}],
        )
        (
            dt.merge(src, "t.id = s.id")
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute()
        )

        by_id = {r.id: r.val for r in spark.table(tbl).collect()}
        assert by_id[1] == "new"
        assert by_id[2] == "keep"
        assert by_id[3] == "added"

    def test_full_merge_with_all_three_clauses(self, spark):
        """Update matched, insert not matched, delete not matched by source."""
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}],
            [{"id": 1, "val": "x"}, {"id": 3, "val": "z"}],
        )
        (
            dt.merge(src, "t.id = s.id")
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .whenNotMatchedBySource()
            .delete()
            .execute()
        )

        by_id = {r.id: r.val for r in spark.table(tbl).collect()}
        assert by_id == {1: "x", 3: "z"}


class TestBackwardCompatibility:
    """Verify the legacy flat API still works."""

    def test_flat_api_update_and_insert(self, spark):
        dt, src, tbl = _setup_merge(
            spark,
            [{"id": 1, "val": "old"}],
            [{"id": 1, "val": "new"}, {"id": 2, "val": "added"}],
        )
        (
            dt.merge(src, "t.id = s.id")
            .whenMatchedUpdate({"val": "s.val"})
            .whenNotMatchedInsertAll()
            .execute()
        )

        by_id = {r.id: r.val for r in spark.table(tbl).collect()}
        assert by_id[1] == "new"
        assert by_id[2] == "added"
