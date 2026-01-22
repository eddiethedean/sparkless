"""Tests for complex MERGE patterns in SQL executor.

This module tests advanced MERGE functionality including:
- Multiple WHEN MATCHED clauses with conditions
- WHEN NOT MATCHED BY SOURCE clause
- Complex expressions in SET clauses
"""

import pytest
from tests.fixtures.spark_backend import get_backend_type, BackendType


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


@pytest.mark.skipif(
    _is_pyspark_mode(),
    reason="Complex MERGE patterns are sparkless-specific features",
)
class TestComplexMergeBasic:
    """Test basic complex MERGE functionality."""

    def test_merge_with_matched_condition(self, spark) -> None:
        """Test MERGE with condition on WHEN MATCHED.

        Only rows matching the condition should be updated.
        """
        try:
            # Create target table
            spark.sql(
                "CREATE TABLE test_db.target (id INT, value INT, updated_at STRING)"
            )
            spark.sql(
                "INSERT INTO test_db.target VALUES "
                "(1, 100, '2024-01-01'), (2, 200, '2024-01-01')"
            )

            # Create source table
            spark.sql(
                "CREATE TABLE test_db.source (id INT, value INT, updated_at STRING)"
            )
            spark.sql(
                "INSERT INTO test_db.source VALUES "
                "(1, 150, '2024-02-01'), (2, 200, '2023-12-01')"
            )

            # MERGE with condition - only update if source is newer
            spark.sql(
                """
                MERGE INTO test_db.target t
                USING test_db.source s
                ON t.id = s.id
                WHEN MATCHED AND s.updated_at > t.updated_at THEN
                    UPDATE SET t.value = s.value, t.updated_at = s.updated_at
            """
            )

            # Only id=1 should be updated (s.updated_at > t.updated_at)
            result = spark.sql("SELECT * FROM test_db.target ORDER BY id").collect()
            assert len(result) == 2
            assert result[0]["value"] == 150  # Updated
            assert result[0]["updated_at"] == "2024-02-01"
            assert result[1]["value"] == 200  # Not updated (source is older)
            assert result[1]["updated_at"] == "2024-01-01"
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target")
            spark.sql("DROP TABLE IF EXISTS test_db.source")

    def test_merge_multiple_when_matched(self, spark) -> None:
        """Test MERGE with multiple WHEN MATCHED clauses.

        First matching clause wins.
        """
        try:
            spark.sql("CREATE TABLE test_db.target2 (id INT, value INT, status STRING)")
            spark.sql(
                "INSERT INTO test_db.target2 VALUES "
                "(1, 100, 'active'), (2, 200, 'active')"
            )

            spark.sql("CREATE TABLE test_db.source2 (id INT, value INT, status STRING)")
            spark.sql(
                "INSERT INTO test_db.source2 VALUES "
                "(1, 150, 'deleted'), (2, 250, 'active')"
            )

            # MERGE with multiple WHEN MATCHED clauses
            # First clause: DELETE if status is 'deleted'
            # Second clause: UPDATE for all other matches
            spark.sql(
                """
                MERGE INTO test_db.target2 t
                USING test_db.source2 s
                ON t.id = s.id
                WHEN MATCHED AND s.status = 'deleted' THEN DELETE
                WHEN MATCHED THEN UPDATE SET t.value = s.value
            """
            )

            result = spark.sql("SELECT * FROM test_db.target2 ORDER BY id").collect()
            assert len(result) == 1  # id=1 deleted
            assert result[0]["id"] == 2
            assert result[0]["value"] == 250  # Updated
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target2")
            spark.sql("DROP TABLE IF EXISTS test_db.source2")

    def test_merge_first_clause_wins(self, spark) -> None:
        """Test that first matching WHEN MATCHED clause wins."""
        try:
            spark.sql("CREATE TABLE test_db.target3 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.target3 VALUES (1, 100)")

            spark.sql("CREATE TABLE test_db.source3 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.source3 VALUES (1, 999)")

            # Both conditions are true, but first clause should win
            spark.sql(
                """
                MERGE INTO test_db.target3 t
                USING test_db.source3 s
                ON t.id = s.id
                WHEN MATCHED AND s.value > 0 THEN UPDATE SET t.value = 200
                WHEN MATCHED AND s.value > 0 THEN UPDATE SET t.value = 300
            """
            )

            result = spark.sql("SELECT * FROM test_db.target3").collect()
            assert result[0]["value"] == 200  # First clause wins
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target3")
            spark.sql("DROP TABLE IF EXISTS test_db.source3")


@pytest.mark.skipif(
    _is_pyspark_mode(),
    reason="Complex MERGE patterns are sparkless-specific features",
)
class TestMergeNotMatchedBySource:
    """Test WHEN NOT MATCHED BY SOURCE clause."""

    def test_merge_delete_not_matched_by_source(self, spark) -> None:
        """Test deleting rows not in source."""
        try:
            spark.sql("CREATE TABLE test_db.target3 (id INT, value INT, org_id STRING)")
            spark.sql(
                "INSERT INTO test_db.target3 VALUES "
                "(1, 100, 'org1'), (2, 200, 'org1'), (3, 300, 'org2')"
            )

            spark.sql("CREATE TABLE test_db.source3 (id INT, value INT)")
            spark.sql(
                "INSERT INTO test_db.source3 VALUES (1, 150)"
            )  # Only id=1 in source

            # MERGE with NOT MATCHED BY SOURCE - delete rows from org1 not in source
            spark.sql(
                """
                MERGE INTO test_db.target3 t
                USING test_db.source3 s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET t.value = s.value
                WHEN NOT MATCHED BY SOURCE AND t.org_id = 'org1' THEN DELETE
            """
            )

            result = spark.sql("SELECT * FROM test_db.target3 ORDER BY id").collect()
            # id=1: updated, id=2: deleted (not in source, org1), id=3: kept (org2)
            assert len(result) == 2
            ids = [r["id"] for r in result]
            assert 1 in ids
            assert 3 in ids
            assert 2 not in ids

            # Verify id=1 was updated
            row1 = next(r for r in result if r["id"] == 1)
            assert row1["value"] == 150
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target3")
            spark.sql("DROP TABLE IF EXISTS test_db.source3")

    def test_merge_not_matched_by_source_no_condition(self, spark) -> None:
        """Test NOT MATCHED BY SOURCE without additional condition."""
        try:
            spark.sql("CREATE TABLE test_db.target4 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.target4 VALUES (1, 100), (2, 200), (3, 300)")

            spark.sql("CREATE TABLE test_db.source4 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.source4 VALUES (1, 150)")

            # Delete ALL rows not in source
            spark.sql(
                """
                MERGE INTO test_db.target4 t
                USING test_db.source4 s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET t.value = s.value
                WHEN NOT MATCHED BY SOURCE THEN DELETE
            """
            )

            result = spark.sql("SELECT * FROM test_db.target4").collect()
            # Only id=1 should remain
            assert len(result) == 1
            assert result[0]["id"] == 1
            assert result[0]["value"] == 150
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target4")
            spark.sql("DROP TABLE IF EXISTS test_db.source4")


@pytest.mark.skipif(
    _is_pyspark_mode(),
    reason="Complex MERGE patterns are sparkless-specific features",
)
class TestMergeComplexExpressions:
    """Test complex expressions in MERGE."""

    def test_merge_with_expression_in_set(self, spark) -> None:
        """Test expressions in SET clause (e.g., incrementing a version)."""
        try:
            spark.sql("CREATE TABLE test_db.target4 (id INT, value INT, version INT)")
            spark.sql("INSERT INTO test_db.target4 VALUES (1, 100, 1)")

            spark.sql("CREATE TABLE test_db.source4 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.source4 VALUES (1, 150)")

            spark.sql(
                """
                MERGE INTO test_db.target4 t
                USING test_db.source4 s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET
                    t.value = s.value,
                    t.version = t.version + 1
            """
            )

            result = spark.sql("SELECT * FROM test_db.target4").collect()
            assert result[0]["value"] == 150
            assert result[0]["version"] == 2  # Incremented
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target4")
            spark.sql("DROP TABLE IF EXISTS test_db.source4")

    def test_merge_with_arithmetic_expression(self, spark) -> None:
        """Test arithmetic expressions in SET clause."""
        try:
            spark.sql("CREATE TABLE test_db.target5 (id INT, count INT)")
            spark.sql("INSERT INTO test_db.target5 VALUES (1, 10)")

            spark.sql("CREATE TABLE test_db.source5 (id INT, increment INT)")
            spark.sql("INSERT INTO test_db.source5 VALUES (1, 5)")

            spark.sql(
                """
                MERGE INTO test_db.target5 t
                USING test_db.source5 s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET t.count = t.count + 5
            """
            )

            result = spark.sql("SELECT * FROM test_db.target5").collect()
            assert result[0]["count"] == 15
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target5")
            spark.sql("DROP TABLE IF EXISTS test_db.source5")


@pytest.mark.skipif(
    _is_pyspark_mode(),
    reason="Complex MERGE patterns are sparkless-specific features",
)
class TestMergeInsertAll:
    """Test WHEN NOT MATCHED INSERT patterns."""

    def test_merge_insert_all(self, spark) -> None:
        """Test INSERT ALL for new rows."""
        try:
            spark.sql("CREATE TABLE test_db.target5 (id INT, value INT, name STRING)")
            spark.sql("INSERT INTO test_db.target5 VALUES (1, 100, 'existing')")

            spark.sql("CREATE TABLE test_db.source5 (id INT, value INT, name STRING)")
            spark.sql(
                "INSERT INTO test_db.source5 VALUES "
                "(1, 150, 'updated'), (2, 200, 'new')"
            )

            spark.sql(
                """
                MERGE INTO test_db.target5 t
                USING test_db.source5 s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET t.value = s.value, t.name = s.name
                WHEN NOT MATCHED THEN INSERT (id, value, name) VALUES (s.id, s.value, s.name)
            """
            )

            result = spark.sql("SELECT * FROM test_db.target5 ORDER BY id").collect()
            assert len(result) == 2
            assert result[0]["id"] == 1
            assert result[0]["value"] == 150
            assert result[0]["name"] == "updated"
            assert result[1]["id"] == 2
            assert result[1]["name"] == "new"
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target5")
            spark.sql("DROP TABLE IF EXISTS test_db.source5")

    def test_merge_insert_only_unmatched(self, spark) -> None:
        """Test that INSERT only applies to unmatched source rows."""
        try:
            spark.sql("CREATE TABLE test_db.target6 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.target6 VALUES (1, 100), (2, 200)")

            spark.sql("CREATE TABLE test_db.source6 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.source6 VALUES (2, 250), (3, 300), (4, 400)")

            spark.sql(
                """
                MERGE INTO test_db.target6 t
                USING test_db.source6 s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET t.value = s.value
                WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
            """
            )

            result = spark.sql("SELECT * FROM test_db.target6 ORDER BY id").collect()
            # id=1 unchanged, id=2 updated, id=3,4 inserted
            assert len(result) == 4

            values_by_id = {r["id"]: r["value"] for r in result}
            assert values_by_id[1] == 100  # Unchanged
            assert values_by_id[2] == 250  # Updated
            assert values_by_id[3] == 300  # Inserted
            assert values_by_id[4] == 400  # Inserted
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target6")
            spark.sql("DROP TABLE IF EXISTS test_db.source6")


@pytest.mark.skipif(
    _is_pyspark_mode(),
    reason="Complex MERGE patterns are sparkless-specific features",
)
class TestMergeEdgeCases:
    """Test edge cases for MERGE operations."""

    def test_merge_no_matches(self, spark) -> None:
        """Test MERGE when no rows match."""
        try:
            spark.sql("CREATE TABLE test_db.target7 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.target7 VALUES (1, 100)")

            spark.sql("CREATE TABLE test_db.source7 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.source7 VALUES (2, 200)")

            spark.sql(
                """
                MERGE INTO test_db.target7 t
                USING test_db.source7 s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET t.value = s.value
                WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
            """
            )

            result = spark.sql("SELECT * FROM test_db.target7 ORDER BY id").collect()
            # id=1 unchanged, id=2 inserted
            assert len(result) == 2
            assert result[0]["id"] == 1
            assert result[0]["value"] == 100
            assert result[1]["id"] == 2
            assert result[1]["value"] == 200
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target7")
            spark.sql("DROP TABLE IF EXISTS test_db.source7")

    def test_merge_empty_source(self, spark) -> None:
        """Test MERGE with empty source table."""
        try:
            spark.sql("CREATE TABLE test_db.target8 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.target8 VALUES (1, 100), (2, 200)")

            spark.sql("CREATE TABLE test_db.source8 (id INT, value INT)")
            # Empty source table

            spark.sql(
                """
                MERGE INTO test_db.target8 t
                USING test_db.source8 s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET t.value = s.value
            """
            )

            result = spark.sql("SELECT * FROM test_db.target8").collect()
            # No changes
            assert len(result) == 2
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target8")
            spark.sql("DROP TABLE IF EXISTS test_db.source8")

    def test_merge_all_matched_deleted(self, spark) -> None:
        """Test MERGE that deletes all matched rows."""
        try:
            spark.sql("CREATE TABLE test_db.target9 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.target9 VALUES (1, 100), (2, 200)")

            spark.sql("CREATE TABLE test_db.source9 (id INT, value INT)")
            spark.sql("INSERT INTO test_db.source9 VALUES (1, 150), (2, 250)")

            spark.sql(
                """
                MERGE INTO test_db.target9 t
                USING test_db.source9 s
                ON t.id = s.id
                WHEN MATCHED THEN DELETE
            """
            )

            result = spark.sql("SELECT * FROM test_db.target9").collect()
            # All rows deleted
            assert len(result) == 0
        finally:
            spark.sql("DROP TABLE IF EXISTS test_db.target9")
            spark.sql("DROP TABLE IF EXISTS test_db.source9")


@pytest.mark.skipif(
    _is_pyspark_mode(),
    reason="Complex MERGE patterns are sparkless-specific features",
)
class TestMergeParserComplex:
    """Test parser for complex MERGE patterns."""

    def test_parser_extracts_multiple_when_matched(self, spark) -> None:
        """Test that parser correctly extracts multiple WHEN MATCHED clauses."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()
        query = """
            MERGE INTO target t
            USING source s
            ON t.id = s.id
            WHEN MATCHED AND s.status = 'deleted' THEN DELETE
            WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET t.value = s.value
            WHEN MATCHED THEN UPDATE SET t.flag = 1
        """

        ast = parser.parse(query)
        when_matched = ast.components.get("when_matched", [])

        assert len(when_matched) == 3
        assert when_matched[0]["action"] == "DELETE"
        assert when_matched[0]["condition"] == "s.status = 'deleted'"
        assert when_matched[1]["action"] == "UPDATE"
        assert when_matched[1]["condition"] == "s.updated_at > t.updated_at"
        assert when_matched[2]["action"] == "UPDATE"
        assert when_matched[2]["condition"] is None  # No condition

    def test_parser_extracts_not_matched_by_source(self, spark) -> None:
        """Test that parser correctly extracts WHEN NOT MATCHED BY SOURCE."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()
        query = """
            MERGE INTO target t
            USING source s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.value = s.value
            WHEN NOT MATCHED BY SOURCE AND t.org_id = 'org1' THEN DELETE
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.status = 'orphan'
        """

        ast = parser.parse(query)
        when_not_matched_by_source = ast.components.get(
            "when_not_matched_by_source", []
        )

        assert len(when_not_matched_by_source) == 2
        assert when_not_matched_by_source[0]["action"] == "DELETE"
        assert when_not_matched_by_source[0]["condition"] == "t.org_id = 'org1'"
        assert when_not_matched_by_source[1]["action"] == "UPDATE"
        assert when_not_matched_by_source[1]["condition"] is None

    def test_parser_extracts_complex_set_clause(self, spark) -> None:
        """Test that parser correctly extracts complex SET clauses."""
        from sparkless.session.sql.parser import SQLParser

        parser = SQLParser()
        query = """
            MERGE INTO target t
            USING source s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET
                t.value = s.value,
                t.version = t.version + 1,
                t.updated_at = current_timestamp()
        """

        ast = parser.parse(query)
        when_matched = ast.components.get("when_matched", [])

        assert len(when_matched) == 1
        assignments = when_matched[0].get("assignments", [])
        assert len(assignments) == 3

        # Check individual assignments
        assert assignments[0]["target"] == "t.value"
        assert assignments[0]["value"] == "s.value"
        assert assignments[1]["target"] == "t.version"
        assert assignments[1]["value"] == "t.version + 1"
        assert assignments[2]["target"] == "t.updated_at"
        assert assignments[2]["value"] == "current_timestamp()"
