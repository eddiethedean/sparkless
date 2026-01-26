"""
Test for issue #355: unionByName produces incorrect results when same DataFrame
is used in two join branches (diamond dependency).

https://github.com/eddiethedean/sparkless/issues/355
"""

from sparkless.sql import SparkSession
from sparkless import functions as F


class TestIssue355DiamondDependency:
    """Test unionByName with diamond dependency (same DataFrame in two branches)."""

    def test_unionByName_diamond_dependency(self):
        """Test that unionByName correctly handles diamond dependencies.

        When the same DataFrame is used as input to two separate join operations
        (creating a diamond/fork dependency graph), and the results are then
        combined via unionByName, sparkless should produce the correct result
        without duplicating rows.
        """
        spark = SparkSession("test")

        existing = spark.createDataFrame(
            [(1, "a", 100), (2, "b", 200), (3, "c", 300)],
            ["id", "name", "value"],
        )

        source = spark.createDataFrame(
            [(1, "a", 150), (2, "b", 250)],
            ["id", "name", "value"],
        )

        # Anti-join: existing rows NOT in source
        sk = source.select("id").distinct().withColumn("_m", F.lit(True))
        branch_a = (
            existing.join(sk, on=["id"], how="left")
            .filter(F.col("_m").isNull())
            .drop("_m")
        )

        # Inner-join: existing rows that ARE in source
        branch_b = existing.join(source.select("id").distinct(), on=["id"], how="inner")

        # Union both branches
        combined = branch_a.unionByName(branch_b, allowMissingColumns=True)
        result = combined.collect()

        # Expected: 3 rows total
        # - 1 row from branch_a (id=3, not in source)
        # - 2 rows from branch_b (id=1 and id=2, matched in source)
        assert len(result) == 3, f"Expected 3 rows, got {len(result)}"

        # Verify we have the correct rows
        ids = {row.id for row in result}
        assert ids == {1, 2, 3}, f"Expected ids {{1, 2, 3}}, got {ids}"

        # Verify values are correct (not duplicated original values)
        id_to_value = {row.id: row.value for row in result}
        assert id_to_value[1] == 100, (
            f"Expected value 100 for id=1, got {id_to_value[1]}"
        )
        assert id_to_value[2] == 200, (
            f"Expected value 200 for id=2, got {id_to_value[2]}"
        )
        assert id_to_value[3] == 300, (
            f"Expected value 300 for id=3, got {id_to_value[3]}"
        )

        # Verify no duplicates
        assert len(ids) == 3, "Found duplicate IDs in result"
